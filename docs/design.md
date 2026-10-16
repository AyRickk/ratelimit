# Conception — `ratelimit` (Rust)

Document de référence des décisions de conception, consolidé le 2026-10-16. Il remplace `docs/design-review-prompt.md`, qui garde l'historique des propositions initiales. Il intègre la revue `docs/design-review.md` (sections 1 à 7) et ses arbitrages (section 8, R1 à R8).

Les identifiants D1 à D34 sont ceux de la revue. Une décision marquée *(amendée)* diffère de la proposition initiale. Les détails de format sont figés dans les ADR :

- [ADR 0001 — Format des clés de stockage des limites `rate_limits:`](adr/0001-format-cles-redis-nouveaux-types.md)
- [ADR 0002 — Origine des fenêtres fixes (`offset`)](adr/0002-origine-fenetres-fixes.md)

L'état d'avancement de la compatibilité est suivi dans [`docs/parity.md`](parity.md).

---

## Objectif

Une réécriture en Rust d'[envoyproxy/ratelimit](https://github.com/envoyproxy/ratelimit) (référence : commit `f4e7669`), **100 % compatible** et enrichie : durées de fenêtre libres, nouveaux types de fenêtre, plusieurs limites par descripteur, modes de comptage. Remplacer envoy ratelimit derrière Envoy, Envoy Gateway, Istio ou agentgateway ne demande que de changer l'image. La première version n'est publiée qu'à **parité complète**.

---

## A. Périmètre et compatibilité

**D1. Sur-ensemble strict** *(amendée)*. Tout comportement de la référence est reproduit à l'identique pour toute entrée qu'elle traite **sans paniquer**. Ses paniques (ex. `limit` client avec `unit: UNKNOWN`, `utilities.go:38` ; variable d'environnement invalide relue au rechargement) deviennent des erreurs propres, listées comme divergences volontaires dans `docs/parity.md`. Les ajouts ne changent jamais le comportement d'une config Envoy existante.

**D2. Parité complète avant la v1.** Toutes les fonctionnalités de la référence sont incluses : Memcached, config xDS, `quota_mode`, `share_threshold`, cache local, `REDIS_PERSECOND`, Redis sentinel / cluster / TLS / AWS IAM, DogStatsD, OTLP, en-têtes `LIMIT_*`, métadonnées dynamiques, `/json`, port debug, `grpc.health.v1`, `GRPC_UDS`, rechargement à chaud des certificats, etc. La liste exhaustive est `docs/parity.md`.

**D3. Inventaire de parité** *(amendée)*. `docs/parity.md`, dont la section des variables d'environnement est **générée** depuis `src/settings/settings.go` (134 variables). Il couvre aussi les clés YAML, l'API, les noms de métriques, les logs, les traces et l'image. La v1 sort quand chaque ligne est prouvée par un test ou marquée comme divergence justifiée.

**D4. API gRPC.** Uniquement `envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit` (la v2 a été retirée en amont). À reproduire précisément :

- `hits_addend` au niveau requête : 0 vaut 1, **sauf** si un `hits_addend` par descripteur est présent, même à 0 (`utilities.go:102-118`). agentgateway met toujours 0 au niveau requête : le chemin par descripteur est le chemin critique ;
- `is_negative_hits` (D21) et `limit` envoyée par le client (D23) ;
- `current_limit.name` rempli ; `limit_remaining = 0` sur `OVER_LIMIT` ; `quota` et `raw_body` jamais remplis ; erreurs renvoyées en `codes.Unknown` avec le texte de l'erreur ;
- `overall_code` n'est jamais `UNKNOWN` (agentgateway traite tout code ≠ `OK` comme un refus).

**D5. Config invalide** *(amendée)*.

- Au rechargement : `config_load_error` incrémenté, ancienne config conservée.
- Au démarrage sans config valide : le service ne s'arrête pas, mais **chaque appel renvoie une erreur gRPC** (« no rate limit configuration loaded »), pas `OK`. Le client décide (Envoy : `failure_mode_deny` ; agentgateway : `failClosed` par défaut, soit des 500).
- Avec `FORCE_START_WITHOUT_INITIAL_CONFIG=false` et xDS injoignable, les serveurs ne démarrent pas (attente du premier événement), comme en amont.
- `HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED` évalue « au moins un domaine », pas « config valide ».
- Clés YAML inconnues rejetées (liste blanche récursive amont, `metadata` exclu), liste étendue à nos ajouts. Le proto xDS ignore silencieusement les champs inconnus.

**D6. Clés Redis legacy identiques octet pour octet.** `CACHE_KEY_PREFIX + domaine + "_" + (clé "_" valeur "_")* + bucketStart`, aligné epoch, avec trois nuances : substitution du motif `share_threshold` à la valeur, début de mois UTC avec `USE_CALENDAR_MONTH_RATE_LIMIT`, valeur vide (`key__`). TTL = durée de l'unité + jitter, pas le temps restant. Pendant une migration, les deux services doivent utiliser le même `EXPIRATION_JITTER_MAX_SECONDS`.

---

## B. Nouveaux types de fenêtre (YAML uniquement)

**D7. Plusieurs limites par descripteur** *(amendée)*. Nouvelle liste `rate_limits:`, exclusive de `rate_limit:`. Dans la liste, `unit` est remplacé par `window`, et le **`name` est obligatoire** (ADR 0001). `shadow_mode`, `quota_mode` et `detailed_metric` restent au niveau du **descripteur** et s'appliquent à toute la liste. `unlimited` est interdit dans la liste : on garde la forme legacy `rate_limit: {unlimited: true}`.

```yaml
- key: user_id
  overflow: envoy                  # ou strict (D14)
  rate_limits:
    - name: five_hour
      requests_per_unit: 500000
      window: { type: anchored, duration: 5h, start_rounding: 1h }
    - name: weekly
      requests_per_unit: 5000000
      window: { type: fixed, duration: 7d, offset: hashed }
```

**D8. `fixed`** *(amendée)*. N'importe quelle durée (`90s`, `5h`, `7d`), alignée sur l'epoch par défaut, avec une origine optionnelle : `offset: <durée>` (UTC) ou `offset: hashed` (phase propre à chaque « compte », sans état). C'est ce qui reproduit la limite hebdomadaire de Claude. Détails, règles et formule : ADR 0002. L'option de mois calendaire amont est conservée ; le fuseau horaire IANA est reporté.

**D9. `anchored`** *(amendée)*. Fenêtre de longueur fixe dont l'origine est la première requête. Mode **session** uniquement : le mode `recurring` est supprimé (remplacé par `fixed` + `offset`, ADR 0002).

- **Ouverture** : `open_on: check | first_hit`, défaut `check`.
  - `check` : l'ancre est posée par la première requête **présentée** alors qu'aucune fenêtre n'est ouverte, quels que soient `hits_addend` (0 inclus) et le verdict, **sauf en `strict`, où une requête refusée ne modifie aucun état, ancre comprise**. C'est ce qui place l'ancre au premier message avec agentgateway, qui envoie un check à 0 hit puis les tokens après la réponse.
  - `first_hit` : l'ancre est posée par la première requête dont les hits sont **effectivement comptés** selon `overflow` (en `envoy`, une requête refusée de taille > limite ouvre donc la fenêtre ; en `strict`, non).
- Une ancre posée n'est jamais déplacée ni prolongée. Elle est remplacée par la première requête présentée après `ancre + durée`. Un amendement tardif (stream commencé avant la fin, terminé après) est une requête ordinaire sur fenêtre fermée : il ouvre la fenêtre suivante et y est compté, comme ccusage.
- **`start_rounding`** : optionnel, sans arrondi par défaut, en UTC. L'arrondi s'applique à l'ancre ; la fenêtre finit à `ancre + durée`.
- **Réponse d'un check qui ouvre** : `OK`, `limit_remaining = limite − hits comptés`, `duration_until_reset = ceil(ancre + durée − now)`, donc moins que la durée complète avec un arrondi.
- **Fenêtres sans hit** : avec `open_on: check` en `envoy`, une requête refusée par une autre limite (même liste ou autre descripteur) ouvre quand même la fenêtre, avec un budget plein. Une requête qui échoue en amont (agentgateway amende alors 0 token) aussi. Ces fenêtres sont comptées dans `opened_without_hits`. Si c'est indésirable, utiliser `open_on: first_hit`. Le comportement réel de Claude dans ce cas n'est pas documenté.
- **Script** : ancre et compteur dans la même clé, lecture-décision-écriture en un seul script ; `elapsed` borné à 0 ; fermeture décidée par `now ≥ ancre + durée` dans le script, pas par l'absence de clé ; TTL = durée + marge.
- La matrice de tests de référence est dans `docs/design-review.md`, R7 (corrigée par R8).

**D10. `sliding` et `sliding_approx`** *(amendée)*.

- `sliding` (défaut) : N sous-tranches **sans interpolation**, dans **un hash Redis par (descripteur, limite)** (un script, une clé, un slot ; coût réseau indépendant de N). Ne dépasse jamais la limite ; la capacité revient au plus D/N en retard. `buckets: 12` par défaut (12 divise 60 et 3600), avec une tranche d'au moins 1 s ; la doc recommande 56 ou 84 pour 7 jours.
- `sliding_approx` : l'approximation de Cloudflare à deux fenêtres. Son chiffre de 0,003 % d'erreur (400 M de requêtes, 270 000 sources) ne se généralise pas aux tokens LLM en rafales ni aux longues durées.
- En mode `envoy`, un client qui continue d'envoyer au-dessus de la limite peut rester bloqué ; il récupère au plus 2 × D après s'être arrêté. À documenter.

**D11. `token_bucket`** *(amendée)*.

- GCRA, une seule échéance (TAT) stockée en microsecondes (exacte en double Lua jusqu'en 2255).
- Débit = `requests_per_unit / duration` ; capacité = `burst` (défaut `requests_per_unit`).
- En `envoy` : admis si le seau n'est pas vide, débité entièrement, dette autorisée **jusqu'à `max_debt`**, dont le défaut est **`burst`** (au plus un seau de dette, donc au plus `duration` de blocage). `max_debt` s'exprime en jetons ou en durée (`10m` ⇒ `r × 600` jetons) ; `0` et `unlimited` sont acceptés. Les jetons tronqués sont comptés dans la stat `total_capped_hits`.
- Avertissement au chargement quand `burst / r ≥ 24 h` : `token_bucket` ne convient pas à un budget journalier ou hebdomadaire ; utiliser `fixed` + `offset`.
- `limit_remaining` = nombre entier de jetons restants, jamais sous 0. Autorisé : `duration_until_reset` = temps jusqu'au seau plein (comme Anthropic). Refusé : temps jusqu'à assez de jetons, au moins 1 s.

**D12. `duration_until_reset`** *(amendée)*. Calculé selon le type et arrondi à la seconde **supérieure**, car agentgateway tronque `x-ratelimit-reset` (`remoteratelimit.rs:606-610`). Jamais 0 lors d'un refus. Legacy : reproduire `sec − now % sec`, y compris le 0 possible à la dernière seconde d'un mois calendaire. `fixed` avec offset : `D − (now − φ).rem_euclid(D)`.

**D13. Indiquer la durée de fenêtre** *(amendée)*. On renvoie la plus grande unité standard qui divise la durée, plus `unit_multiplier` (5h → HOUR×5). C'est une **extension** : la référence Go ne connaît pas ce champ, agentgateway non plus (proto aplati, champ ignoré). Les en-têtes par unité sont calculés sur l'expansion **par limite**, pas sur les statuts réduits ; pour une config legacy la sortie reste inchangée (`RateLimit-Limit-Hours`), et le suffixe `5Hours` n'apparaît que si le multiplicateur ≠ 1.

---

## C. Sémantique du comptage

**D14. Mode de comptage par descripteur** *(amendée)* : `overflow: envoy | strict`.

- `envoy` (défaut, pour `rate_limit:` et `rate_limits:`) : tout est compté, y compris les requêtes refusées. C'est le seul mode qui ne perd jamais les amendements de tokens d'agentgateway, envoyés après la réponse sans lecture du résultat.
- `strict` : tout-ou-rien sur la liste. **Un descripteur dont la liste est épuisée n'est pas compté**, et une requête refusée ne modifie aucun état (ancre comprise, D9).
- Requête à 0 hit : `envoy` utilise `>` comme la référence, `strict` utilise `>=`.
- À documenter : en `strict`, un amendement agentgateway refusé est perdu sans signal ; avec une expression `cost`, un amendement supérieur aux jetons restants est refusé et le seau n'est pas touché, donc `strict` + `cost` ne limite presque rien.
- `strict` × shadow : le verdict effectif est `OK`, donc la liste est comptée (la référence compte puis masque).

**D15. Plusieurs descripteurs dans une requête** *(amendée)*. Indépendants, comme dans la référence. `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` est reproduit exactement pour le legacy, selon sa table de vérité réelle (`fixed_cache_impl.go:65-93`) : une clé over-limit dans le cache local ⇒ aucune clé incrémentée (mais `INCRBY 0` + `EXPIRE` créent des clés fantômes) ; sinon seules les clés **qui vont dépasser** sont incrémentées et les autres reçoivent 0. Redis uniquement. Avec `REDIS_PERSECOND`, une liste qui contient une limite à la seconde va en entier vers un seul client (divergence documentée).

**D16. `replaces`** *(amendée)*. Placé sur chaque limite d'une liste, avec la sémantique exacte de la référence : deux passes sur toute la requête, non transitif ; les limites remplacées ou `unlimited` remplacent quand même ; un cycle retire les deux côtés ; une limite sans nom est irremplaçable ; une limite `unlimited` « remplacée » reste `OK` avec `limit_remaining = MaxUint32`. Aucune unicité des noms entre descripteurs (groupes de remplacement) ; deux noms identiques dans une liste, ou une limite qui remplace sa voisine de liste, sont des erreurs. Le filtrage se fait **avant** `strict` ; si toutes les limites d'un descripteur sont remplacées, on renvoie `OK` sans `current_limit`.

**D17. Fonctionnalités de la référence sur les nouvelles fenêtres** *(amendée)*.

- **Shadow** : le verdict utilisé pour l'ouverture et le comptage est le verdict **post-shadow**. Le shadow du descripteur et le shadow **global** (`SHADOW_MODE`) sont transmis au script.
- **`quota_mode`** (R1) : reproduit tel quel (`ratelimit.go:233-355`) : groupes par `backend_name` / `model_name_override`, `OVER_LIMIT` seulement si tous les groupes sont épuisés, `passedBackends` **dédupliqué par groupe** et filtré sur les groupes non épuisés. Avec plusieurs limites, un descripteur est épuisé si **au moins une** de ses limites l'est.
- **Statut représentatif d'un descripteur à plusieurs limites** (ordre total) : s'il existe des statuts `OVER_LIMIT`, celui au **plus grand `duration_until_reset`** ; sinon celui au **plus petit `limit_remaining`** ; à égalité, le premier de la liste. `current_limit`, `limit_remaining` et `duration_until_reset` viennent de ce seul statut ; `statuses[i]` reste 1:1 avec les descripteurs. La réduction se fait dans la couche cache ; le shadow du descripteur s'applique après et ne change que le code.
- Un descripteur dont toutes les limites sont remplacées sort du groupe quota (sauf `QUOTA_MODE` global) mais figure dans `passedBackends` s'il porte une clé de groupe.
- **`share_threshold`** : le motif remplace la valeur dans la partie `{…}` des clés (ADR 0001) et dans l'entrée du hachage d'`offset: hashed` (ADR 0002). **`detailed_metric`**, **`value_to_metric`** : statistiques seulement, tous types.

**D18. Cache local.** Legacy et `fixed` : comportement exact de la référence (entrée posée avant le test shadow, TTL = durée complète de l'unité, une limite en shadow n'est plus comptée quand le cache est actif, `over_limit_with_local_cache` compte `hitsAddend`). Nouveaux types : une entrée vit jusqu'au premier moment où la décision peut changer ; mise en cache seulement s'il reste moins d'une unité ; un remboursement efface l'entrée locale (divergence volontaire, nouveaux types uniquement) ; pas de cache pour les limites en shadow.

**D19. `quota_mode` et blocage durable.** Avertissement au chargement (log et gauge) quand `quota_mode` est combiné à `sliding`, ou à `token_bucket` avec `max_debt: unlimited`, en mode `envoy` : un groupe peut rester bloqué tant que le trafic dépasse le débit.

**D20. `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` sur les nouveaux types** *(amendée)* : **sans effet**. Pour ne pas compter les requêtes refusées, utiliser `overflow: strict`.

**D21. Remboursements (`is_negative_hits`).** Propriétés de la référence pour tous les types : réponse toujours `OK`, `limit_remaining = max(0, limite − compteur)`, stat `total_negative_hits`, TTL remis à la durée complète, aucun effet sur le cache local en legacy ; Memcached décrémente de façon asynchrone après la réponse. Par type : `fixed` décrémente la fenêtre courante (plancher 0) ; `anchored` décrémente sans jamais ouvrir ni fermer la fenêtre ; `sliding` décrémente de la sous-tranche la plus récente vers la plus ancienne ; `sliding_approx`, fenêtre courante puis précédente ; `token_bucket` recule l'échéance sans remonter avant `now`.

**D22. Stats `near_limit` et `over_limit`.** Arithmétique legacy exacte : `nearLimitThreshold = floor(float64(float32(limit) × float32(ratio)))`, répartition des `hitsAddend` entre `near_limit` et `over_limit`, `within_limit` seulement si `OK`. Usage avant et après la requête, par type : `token_bucket`, capacité − restant (peut dépasser la capacité en dette) ; `sliding`, somme brute des sous-tranches ; `anchored`, 0 avant l'ouverture.

**D23. `limit` envoyée par le client** *(amendée)*. Si le domaine est connu : **une** fenêtre fixe de `unit × unit_multiplier`, qui **remplace toutes** les limites configurées du descripteur ; pas de `shadow_mode`, `unlimited` ni `replaces` hérités, shadow global appliqué ; mode de comptage de la règle correspondante s'il y en a une, sinon `envoy` ; clé legacy si le multiplicateur vaut 1, clé incluant la durée sinon. La lecture, le renvoi et le rejet du multiplicateur 0 sont des **extensions** (la référence ignore le champ), de même que la recherche de règle pour le mode.

---

## D. Stockage, clés et horloge

**D24. Backends.** Redis (scripts Lua pour les nouveaux types), Memcached (legacy et `fixed`, y compris offset ; `anchored`, `sliding`, `token_bucket` et `strict` y sont des erreurs de config) et mémoire. Memcached n'est pas atomique (décision sur `GET`, incrément asynchrone) : les tests le tolèrent. Clé Memcached ≤ 250 octets, sans espace ni caractère de contrôle ; sinon erreur journalisée et réponse `OK`, comme en amont.

**D25. Clés des nouveaux types** *(amendée)* : ADR 0001.

**D26. Identité d'une limite** *(amendée)* : type + durée + arrondi + mode d'ouverture + `buckets` + `offset` + **nom** (ADR 0001). Changer l'un d'eux repart de zéro. Changer `requests_per_unit`, `burst` ou `max_debt` conserve les compteurs ; un plafond abaissé sous le compteur bloque jusqu'à la fin de la fenêtre, les jetons au-dessus d'un `burst` abaissé sont plafonnés à la lecture. Pendant un déploiement progressif, la limite effective peut brièvement doubler (documenté).

**D27. Horloge.** Un seul `now` par requête, lu sur une horloge injectable et passé en ARGV aux scripts ; jamais `TIME` de Redis. Les scripts bornent à 0 un temps écoulé négatif. Exception à reproduire pour le legacy : la référence relit l'horloge pour les en-têtes `LIMIT_*` (`ratelimit.go:594, 615`).

---

## E. Configuration et observabilité

**D28. Rechargement et xDS** *(amendée)*. Rechargement depuis `RUNTIME_ROOT/RUNTIME_SUBDIRECTORY/RUNTIME_APPDIRECTORY` avec la même logique (`SymlinkRefresher` si `RUNTIME_WATCH_ROOT`, sinon `DirectoryRefresher` ; `RUNTIME_IGNOREDOTFILES` ; scope de stats `runtime`). xDS SotW supporté pour ce que le proto amont porte, ce qui exclut déjà `value_to_metric` et `share_threshold` en amont. Nos ajouts sont réservés au YAML.

**D29. Métriques** *(amendée)*.

- Mêmes noms que la référence. Sinks exclusifs, erreur fatale si deux sont actifs : statsd **TCP** (`USE_STATSD=true` par défaut, donc `USE_PROMETHEUS=true` seul est fatal), DogStatsD avec mogrifiers, Prometheus natif ; `DISABLE_STATS` prime ; sinon log.
- Prometheus : réimplémentation du mapper statsd_exporter pour le sous-ensemble utilisé (globs, regex, `$n`, `match_metric_type`, `drop`, types d'observateurs et buckets, `defaults`), en respectant l'ordre réel d'évaluation (tous les globs, puis les regex dans l'ordre) et l'échappement des noms non mappés. Une clé de mapper non supportée est refusée au démarrage (divergence documentée). Fichier de mapper absent : comportement amont reproduit.
- Plusieurs limites : identifiant `~nom` accolé au **dernier segment** (`user_alice~weekly`), émis **même pour une liste à une seule limite**. Le mapper par défaut livré ajoute un label `limit=` et mappe les nouvelles stats `total_capped_hits` (D11) et `opened_without_hits` (D9).
- Stats Go (`go.*`, `/stats` expvar, pprof) : remplacées par les stats du processus Rust (divergence documentée).

---

## F. Code, tests et livraison

**D30. Stack.** tokio, tonic + prost, axum, serde, fred. TLS via `rustls`, pour un binaire sans OpenSSL.

**D31. Organisation du code** *(amendée)*. Workspace de deux crates : `ratelimit-proto` (protos aplatis, compilés avec `protox`, sans `protoc`) et `ratelimit`. Couture par opérations : `Store::apply(lot d'ops {clé, algo, limite, hits, now})`. Chaque algorithme des nouveaux types est une fonction pure `step(état, op) → (état, résultat)` en Rust (backend mémoire), réécrite en Lua pour Redis. **Le chemin legacy reste un pipeline `INCRBY` + `EXPIRE` avec décision côté service**, pas un script, pour que les commandes observées dans Redis soient identiques. La couche de décision (seuils, shadow, quota, réduction des statuts, réponse) est partagée.

**D32. Tests** *(amendée)*.

1. **Comparaison exacte en CI à chaque PR** avec un programme Go qui importe la référence à un commit épinglé, avec une `TimeSource` injectable et un jitter seedé ou nul ; réponses, en-têtes, clés, valeurs, TTL et séquence de commandes Redis (`MONITOR`) comparés.
2. **Test nocturne** contre l'image officielle épinglée par digest, avec `EXPIRATION_JITTER_MAX_SECONDS=0` et une tolérance d'environ 1 s.
3. Scripts `integration-test/*.sh` amont rejoués contre notre image : test de fumée, pas de parité.
4. **Tests de propriétés Rust ↔ Lua** sur un vrai Redis **en mode cluster**, avec un fuzz des séquences concurrentes `check(0)` / `amend(n)` / `refund(n)`.
5. Test différentiel du mapper contre un vrai `statsd_exporter`, et du flux statsd brut contre la référence.
6. TDD avec l'horloge injectable pour les nouveaux types, incluant la matrice R7/R8.

**D33. Image** *(amendée)*. Binaire exactement à `/bin/ratelimit`, distroless sans shell, amd64 + arm64 (`cargo-zigbuild`). UID quelconque (Envoy Gateway force 65534), système de fichiers en lecture seule, aucune capability. Ports 8080 / 8081 / 6070, `RUNTIME_ROOT=/srv/runtime_data/current`. Écoute dual-stack explicite (`[::]` avec `IPV6_V6ONLY=0`) et `SO_REUSEPORT` sur les trois ports. `/healthcheck` servi seulement après la connexion Redis initiale, comme en amont. Toutes les variables injectées par Envoy Gateway sont supportées, sauf `GRPC_SERVER_TLS_CA_CERT`, **ignorée comme en amont** (aucun code ne la lit). Image signée avec cosign, avec SBOM et provenance ; test CI sur `kind` avec un Envoy Gateway réel dont on ne change que l'image. Pas de chart Helm en v1.

**D34. Licence.** Apache-2.0, fichier NOTICE qui crédite envoy ratelimit et l'API d'Envoy, en-têtes des protos vendorisés conservés, licences des dépendances générées avec `cargo-about`.

---

## Ordre de construction

1. Protos, serveur gRPC, chargeur YAML, Redis legacy, **harnais de comparaison Go** (D32.1).
2. Cache local, `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`, en-têtes, métadonnées dynamiques, groupes `quota_mode`.
3. Sinks de statistiques et mapper Prometheus.
4. Client xDS, image, test Envoy Gateway.
5. Memcached.
6. Nouveaux types, uniquement via `rate_limits:` : `fixed` avec `offset`, `anchored`, `token_bucket`, `sliding`.

## Risques principaux

1. Parité octet pour octet du chemin legacy Redis (séquence de commandes, clés fantômes, `float32`, horloge relue pour les en-têtes).
2. Groupes `quota_mode` et métadonnées dynamiques (sémantique récente, prototext Go non reproductible).
3. Scripts Lua des nouveaux types sous Redis Cluster.
4. Mapper statsd_exporter et format gostats.
5. Client xDS SotW et déploiement Envoy Gateway.
