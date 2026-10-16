# Prompt : revue de conception exhaustive — `ratelimit` (Rust)

> **Document historique (2026-10-16).** Ce prompt a servi à produire `docs/design-review.md`. Il ne fait plus référence : les décisions **D8, D9, D11, D12, D13, D14, D17, D20, D25 et D26** ont été amendées ou remplacées par la section 8 de `docs/design-review.md` (arbitrage R1 à R5 et points résiduels). La référence est désormais **`docs/design.md`**, avec les ADR `docs/adr/0001-format-cles-redis-nouveaux-types.md` et `docs/adr/0002-origine-fenetres-fixes.md`.

> Ce fichier est un prompt destiné à un relecteur IA. Copie-le en entier comme instruction.

---

## Ton rôle

Tu es un ingénieur principal spécialisé en systèmes distribués, en rate limiting et en écosystème Envoy. Tu fais une **revue de conception adversariale** d'un projet qui n'a pas encore de code. Ton travail est de trouver ce qui est **faux, incohérent, incomplet ou risqué** dans les décisions ci-dessous, **preuves à l'appui**. Tu ne dois pas valider par politesse : une décision que tu ne contestes pas doit être une décision que tu as vérifiée.

Tu n'écris **pas** de code d'implémentation. Tu lis du code source, de la documentation, et tu raisonnes.

## Le projet

Une réécriture en **Rust** de [envoyproxy/ratelimit](https://github.com/envoyproxy/ratelimit), qui doit être :

1. **100 % compatible** : un sur-ensemble strict. Tout ce qu'envoy ratelimit fait, on le fait à l'identique (API gRPC, config YAML, variables d'environnement, clés Redis, métriques, endpoints, image Docker). Objectif : remplacer envoy ratelimit derrière Envoy, Envoy Gateway, Istio ou [agentgateway](https://github.com/agentgateway/agentgateway) en changeant seulement le nom de l'image.
2. **Plus des ajouts** : des durées de fenêtre personnalisées (`5h`, `90s`…), de nouveaux types de fenêtre (dont une fenêtre « anchored » qui reproduit la limite de 5h de Claude), plusieurs limites par descripteur, des modes de comptage.
3. **Première version publiée uniquement à parité complète** : pas de 0.x partielle.

## Sources à analyser (obligatoire)

Lis le code source, pas seulement les README. Versions de référence utilisées pendant la conception :

| Source | Version | À regarder en priorité |
|---|---|---|
| envoyproxy/ratelimit | `f4e7669` (2026-10-06) | `src/service/ratelimit.go`, `src/limiter/base_limiter.go`, `src/limiter/cache_key.go`, `src/redis/fixed_cache_impl.go`, `src/memcached/`, `src/config/config_impl.go`, `src/config/config_xds.go`, `src/settings/settings.go`, `src/stats/` (dont `prom/default_mapper.yaml`, `prom/prometheus_sink.go`), `src/server/server_impl.go`, `src/provider/`, `src/utils/`, `Dockerfile`, `test/integration/`, `integration-test/` |
| agentgateway | `c624bd8` (2026-10-15) | `crates/agentgateway/src/http/remoteratelimit.rs`, `crates/agentgateway/src/llm/mod.rs` (calcul des corrections de tokens), `crates/protos/` (proto rls aplati, build avec protox) |
| envoyproxy/envoy | `main` | `api/envoy/service/ratelimit/v3/rls.proto` (champ `unit_multiplier`, pull request n° 47307 fusionnée le 2026-10-05), `api/envoy/extensions/common/ratelimit/v3/ratelimit.proto` (`is_negative_hits`, `RateLimitOverride`), `source/extensions/filters/http/ratelimit/ratelimit_headers.cc`, `source/extensions/filters/common/ratelimit/ratelimit_impl.cc` |
| envoyproxy/gateway | `main` | `internal/infrastructure/kubernetes/ratelimit/resource.go` (commande, env, securityContext, sondes) |
| Doc Anthropic | — | [Claude API rate limits](https://platform.claude.com/docs/en/api/rate-limits) (token bucket), [Max plan usage](https://support.anthropic.com/en/articles/11014257-about-claude-max-plan-usage) (session de 5h) |

Si une version a changé depuis, signale toute différence qui affecte une décision.

## Méthode exigée

1. **Vérifie chaque fait** cité ci-dessous (marqué 🔎). Plusieurs viennent de sous-agents et n'ont pas tous été revérifiés. Un fait faux invalide les décisions qui en dépendent.
2. **Pour chaque décision** : cherche un contre-exemple concret, c'est-à-dire une config, une séquence de requêtes, une topologie Redis, un client (Envoy, agentgateway…) ou un scénario de panne qui la casse.
3. **Cherche les incohérences entre décisions.** Elles ont été prises en plusieurs tours, et des corrections tardives ont pu en contredire d'anciennes.
4. **Cherche ce qui manque** : fonctionnalités d'envoy ratelimit absentes de la liste de parité, cas limites non spécifiés, combinaisons de fonctionnalités jamais discutées.
5. **Cite tes preuves** au format `repo/chemin:ligne` ou URL. Sans preuve, marque ton point comme « hypothèse ».
6. Distingue clairement les **faits**, les **risques** et les **préférences**.

---

## Décisions à revoir

Chaque décision porte un identifiant (D1, D2…) : utilise-le dans ton retour.

### A. Périmètre et compatibilité

- **D1. Sur-ensemble strict.** Tout comportement d'envoy ratelimit est reproduit à l'identique, y compris ses bizarreries. Les ajouts ne changent jamais le comportement d'une config Envoy existante.
- **D2. Parité complète avant la v1** : Memcached, config xDS (`CONFIG_TYPE=GRPC_XDS_SOTW`), `quota_mode`, `share_threshold`, cache local (`LOCAL_CACHE_SIZE_IN_BYTES`), `REDIS_PERSECOND`, Redis Sentinel/Cluster/TLS/AWS IAM, DogStatsD, tracing OTLP, en-têtes `LIMIT_*`, `MERGE_DOMAIN_CONFIG`, `USE_CALENDAR_MONTH_RATE_LIMIT`, `SANITIZE_DESCRIPTOR_METRIC_DOTS`, etc.
- **D3. Inventaire de parité** dans `docs/parity.md` : chaque variable d'environnement, clé YAML, endpoint, métrique et comportement, avec son état et le test qui le prouve. La v1 sort quand tout est vert.
- **D4. API gRPC.** Uniquement `envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit`, puisque la v2 a été retirée en amont 🔎. Tous les champs remplis comme dans la référence. On honore `hits_addend` (requête et descripteur, 0 autorisé = vérifier sans compter), la `limit` envoyée par le client et `is_negative_hits` 🔎.
- **D5. Config invalide.** Même comportement qu'Envoy 🔎 :
  - au rechargement, on incrémente `config_load_error` et on garde l'ancienne config ;
  - au démarrage, le service ne s'arrête pas et tourne sans config (« no rate limit configuration loaded »), en tenant compte de `FORCE_START_WITHOUT_INITIAL_CONFIG` et `HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED`.

  Les clés YAML inconnues sont rejetées comme dans Envoy, mais la liste des clés acceptées inclut nos ajouts. `metadata` n'est pas cosmétique, puisqu'il est renvoyé dans les métadonnées dynamiques.
- **D6. Clés Redis legacy identiques octet pour octet** : `CACHE_KEY_PREFIX + domaine + "_" + (clé "_" valeur "_")* + bucketStart`, avec `bucketStart` aligné sur l'epoch 🔎. L'ancien et le nouveau service peuvent ainsi partager un Redis pendant une migration.

### B. Nouveaux types de fenêtre (YAML uniquement)

- **D7. Plusieurs limites par descripteur** via une nouvelle liste `rate_limits:`. Les formes `rate_limit:` et `rate_limits:` s'excluent, et `unit` et `window` aussi. Syntaxe :
  ```yaml
  - key: user_id
    overflow: envoy            # ou strict
    rate_limits:
      - name: five_hour
        requests_per_unit: 500000
        window: { type: anchored, duration: 5h, start_rounding: 1h, mode: session }
      - name: weekly
        requests_per_unit: 5000000
        window: { type: fixed, duration: 7d }
  ```
- **D8. `fixed`** : n'importe quelle durée, alignée sur l'epoch (une fenêtre de 5h a donc des frontières « bizarres »). On garde l'option de mois calendaire d'Envoy. Le décalage ou fuseau horaire est reporté à plus tard.
- **D9. `anchored`** :
  - la fenêtre s'ouvre à la première requête qui compte des hits (> 0). Une vérification à 0 hit n'ouvre rien et répond « quota plein, reset dans la durée complète » ;
  - une requête refusée ne déplace et ne prolonge jamais la fenêtre ;
  - `start_rounding` est optionnel, sans arrondi par défaut, et l'arrondi se fait en UTC ;
  - mode `session` (défaut : rien jusqu'à la prochaine requête) ou `recurring` (les fenêtres s'enchaînent depuis la toute première requête ; la clé a un TTL, donc un client inactif plus longtemps que le TTL reçoit une nouvelle ancre).
  
  Contexte : la recherche n'a **pas** pu confirmer que Claude arrondit le début de session à l'heure. La doc officielle dit seulement « commence au premier message, dure 5 heures ». L'arrondi à l'heure vient de l'outil communautaire ccusage (`floor_to_hour`). Cette incertitude est la raison pour laquelle l'arrondi est configurable.
- **D10. `sliding`** : N sous-tranches (`buckets: 10` par défaut), **sans interpolation**. On ne dépasse jamais la limite, et la capacité revient au plus D/N en retard. **`sliding_approx`** : l'approximation de Cloudflare à deux fenêtres. Le chiffre de 0,003 % d'erreur ne vaut que pour des requêtes de poids 1 sur des fenêtres d'une minute 🔎.
- **D11. `token_bucket`** :
  - implémenté en GCRA, avec une seule échéance (TAT) stockée en microsecondes pour rester exact en double Lua ;
  - débit = `requests_per_unit / duration`, capacité = `burst` (par défaut `requests_per_unit`) ;
  - en mode `envoy`, une requête est admise si le seau n'est pas vide et elle est débitée entièrement, avec une dette **illimitée** par défaut et `max_debt` optionnel ;
  - `limit_remaining` = nombre entier de jetons restants, jamais sous 0. Quand la requête est autorisée, `duration_until_reset` = temps jusqu'au seau plein.
- **D12. `duration_until_reset`** est calculé selon le type et **arrondi à la seconde supérieure**, car agentgateway tronque `x-ratelimit-reset` (`remoteratelimit.rs:609`) mais arrondit `Retry-After` (`:639`) 🔎.
  - `sliding` : temps jusqu'à ce que la prochaine requête soit autorisée, ou jusqu'à la prochaine bascule de sous-tranche si on est sous la limite.
  - `token_bucket` : temps jusqu'à assez de jetons si refusé, jusqu'au seau plein si autorisé.
  - Il n'est jamais à 0 lors d'un refus.
- **D13. Indiquer la durée de fenêtre** : on renvoie la plus grande unité standard qui divise la durée, plus `unit_multiplier` (5h → HOUR×5, 90s → SECOND×90) 🔎. On vendorise le `rls.proto` à jour. Les anciens clients ignorent le champ et voient une fenêtre approximative. En-têtes par fenêtre : `RateLimit-Limit-5Hours`, au pluriel et en majuscules comme `unitToHeaderSuffix`.

### C. Sémantique du comptage

- **D14. Mode de comptage par descripteur** (pas par limite) : `overflow: envoy | strict`.
  - `envoy` (défaut pour `rate_limit` et `rate_limits`) : tout est compté, y compris les requêtes refusées.
  - `strict` : tout-ou-rien sur la liste du descripteur, et une requête refusée n'est pas comptée.
  - Requête à 0 hit : `envoy` utilise `>` comme Envoy, `strict` utilise `>=`.
  - Le mode `soft` (« finir le message en cours ») a été envisagé puis **abandonné** : sous concurrence, il perdait les corrections de tokens comme `strict`.
  
  Raison principale : agentgateway envoie la correction de tokens *après* la réponse, sans regarder le résultat (`remoteratelimit.rs:137-205`, `llm/mod.rs:3494-3507`) 🔎. Seul le mode `envoy` ne perd jamais ces corrections.
- **D15. Plusieurs descripteurs dans une requête** : chaque descripteur est indépendant, comme dans Envoy. agentgateway envoie user et org comme descripteurs séparés 🔎. On reproduit exactement `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` (à l'échelle de la requête, non atomique) 🔎. Si `REDIS_PERSECOND` est actif et qu'une liste mélange des limites à la seconde et d'autres, toute la liste va dans un seul Redis.
- **D16. `replaces`** :
  - placé sur chaque limite de la liste, avec la sémantique exacte d'Envoy : deux passes sur toute la requête, non transitif, un cycle retire les deux côtés, et les limites remplacées ou `unlimited` remplacent quand même 🔎 ;
  - aucune exigence d'unicité des noms (groupes de remplacement), mais une limite qui remplace sa voisine dans la même liste est une erreur de config ;
  - le filtrage se fait **avant** `strict`, et si tout est remplacé on renvoie OK sans `current_limit` ;
  - `unlimited` est interdit dans `rate_limits`, on garde la forme legacy.
- **D17. Fonctionnalités d'Envoy sur les nouvelles fenêtres** :
  - le shadow mode et `quota_mode` sont passés en indicateurs au script pour que comptage et décision suivent la même règle ;
  - `share_threshold`, `detailed_metric` et `value_to_metric` s'appliquent à tous les types.
- **D18. Cache local** :
  - pour les fenêtres legacy et `fixed`, c'est le comportement exact d'Envoy, y compris sa particularité : avec le cache activé, une limite en shadow n'est plus comptée 🔎 ;
  - pour les nouveaux types, une entrée vit jusqu'au premier moment où la décision peut changer, et on ne met en cache que s'il reste moins d'une unité ;
  - un remboursement efface l'entrée locale (les autres instances la gardent jusqu'à expiration) ;
  - pas de cache pour les limites en shadow.
- **D19. `quota_mode` avec `sliding` ou `token_bucket` en mode `envoy` sans `max_debt`** : avertissement au chargement, pas d'erreur, car un groupe peut rester bloqué indéfiniment.
- **D20. `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` sur les nouveaux types** : signifie « ne pas compter les hits refusés ». `strict` l'emporte. Pour `token_bucket`, c'est équivalent à `max_debt: 0`.
- **D21. Remboursements (`is_negative_hits`)** :
  - `fixed` : on décrémente la fenêtre courante, sans descendre sous 0 ;
  - `anchored` : on décrémente sans jamais ouvrir ni fermer la fenêtre ;
  - `sliding` : de la sous-tranche la plus récente vers la plus ancienne, dans la fenêtre ;
  - `sliding_approx` : fenêtre courante puis précédente ;
  - `token_bucket` : l'échéance recule, sans remonter avant `now`.
- **D22. Statistiques `near_limit` et `over_limit`** : on calcule l'usage avant et après la requête, avec une définition par type. Pour le token bucket, usage = capacité − restant, ce qui peut dépasser la capacité en cas de dette. Pour `sliding`, c'est la somme brute des sous-tranches. Pour `anchored`, c'est 0 avant l'ouverture.
- **D23. Limite envoyée par le client** : si la requête contient `limit` et que le domaine est connu :
  - **une** fenêtre fixe de `unit × unit_multiplier` qui **remplace toutes** les limites configurées du descripteur ;
  - elle n'hérite pas de `shadow_mode`, `unlimited` ni `replaces`, mais le shadow mode global s'applique ;
  - mode de comptage de la règle correspondante s'il y en a une, sinon `envoy` ;
  - clé legacy si le multiplicateur vaut 1, clé qui inclut la durée de fenêtre sinon ;
  - le multiplicateur est renvoyé dans la réponse, et un multiplicateur à 0 est rejeté 🔎.

### D. Stockage, clés et horloge

- **D24. Backends** : Redis (scripts Lua), Memcached (legacy et `fixed` uniquement : `anchored`, `sliding`, `token_bucket` et `strict` y sont une erreur de config) et mémoire.
- **D25. Clés des nouveaux types** : `{préfixe+domaine_clé_valeur…}_<forme lisible>_<fenêtre>`.
  - Le hash tag `{…}` garde les clés d'un descripteur dans le même slot Redis Cluster.
  - Les `{` et `}` venant des valeurs sont échappés.
  - La forme est lisible (ex. `an18000r3600`) et non hachée.
  - Chaque clé a un TTL.
  - Conséquence acceptée : déplacer une limite de `rate_limit` vers `rate_limits` remet son compteur à zéro une fois.
  - L'option rejetée consistait à réutiliser la clé legacy pour un `fixed` dans une liste, ce qui cassait l'atomicité en Cluster.
- **D26. Identité d'une limite** : type + durée + arrondi + mode, **sans** le nom. Deux limites de même forme sur un descripteur sont rejetées sauf si elles ont des noms distincts, auquel cas le nom entre dans la clé.
  - Changer la forme repart de zéro, comme Envoy le fait déjà quand l'unité change.
  - Changer seulement le plafond garde les compteurs. Un plafond abaissé sous le compteur bloque jusqu'à la fin de la fenêtre, et les jetons au-dessus d'un `burst` abaissé sont plafonnés à la lecture.
  - Pendant un déploiement progressif, la limite effective peut brièvement doubler (documenté).
- **D27. Horloge** : un seul `now` par requête, lu sur une horloge injectable dans le service et passé en ARGV à tous les scripts. On n'utilise **jamais** `TIME` de Redis : ça cassait le routage Cluster des clés de `sliding`, échouait sur Dragonfly et rendait les tests non déterministes. Les scripts bornent à 0 un temps écoulé négatif.

### E. Configuration et observabilité

- **D28. Rechargement à chaud** depuis `RUNTIME_ROOT/RUNTIME_SUBDIRECTORY/…`, avec la même logique de surveillance (`RUNTIME_WATCH_ROOT`, `RUNTIME_IGNOREDOTFILES`). La config xDS est supportée pour les fonctionnalités Envoy, mais **nos ajouts sont réservés au YAML**, car le proto xDS ne sait pas les décrire et Envoy Gateway ne les génère pas.
- **D29. Métriques** :
  - statsd **en TCP** avec les mêmes noms et les mêmes variables d'environnement, DogStatsD avec ses mogrifiers, et Prometheus natif avec notre réimplémentation du mapper statsd_exporter (`PROMETHEUS_MAPPER_YAML`). Les trois sorties sont exclusives comme dans Envoy 🔎 ;
  - pour plusieurs limites sur un descripteur, l'identifiant est **accolé au dernier segment** (`user_alice~weekly`) au lieu d'ajouter un segment, ce qui casserait le mapper par défaut et fusionnerait les séries 🔎. Notre mapper par défaut ajoute aussi un label `limit=` ;
  - les stats internes de Go (runtime) n'ont pas d'équivalent : on expose celles du processus Rust, et c'est documenté.

### F. Code, tests et livraison

- **D30. Stack** : tokio, tonic + prost, axum, serde, fred (Redis : pool, cluster, sentinel).
- **D31. Organisation du code** :
  - un workspace de deux crates, `ratelimit-proto` (protos aplatis, compilés avec `protox`, sans `protoc`, comme agentgateway 🔎) et `ratelimit` ;
  - la séparation se fait par opérations : `Store::apply(lot d'ops {clé, algo, limite, hits, now})` ;
  - chaque algorithme est une fonction pure `step(état, op) → (état, résultat)` en Rust pour le backend mémoire, réécrite en Lua pour Redis ;
  - la couche de décision (seuils, shadow, construction de la réponse) est partagée.
- **D32. Tests** :
  1. comparaison exacte en CI à chaque PR avec un **programme Go** qui importe envoy ratelimit à un commit précis, avec une `TimeSource` injectable et le jitter des TTL à 0. Raison : l'horloge de l'image officielle ne peut pas être figée, parce que le binaire est statique et l'image distroless 🔎 ;
  2. test nocturne contre la vraie image épinglée par digest, avec une tolérance d'environ 1 s ;
  3. scripts `integration-test/*.sh` d'Envoy rejoués contre notre image ;
  4. tests de propriétés qui vérifient que chaque script Lua donne le même résultat que sa version Rust ;
  5. TDD avec l'horloge injectable pour les nouvelles fenêtres.
- **D33. Image** :
  - publiée sur GHCR en amd64 et arm64 (`cargo-zigbuild`), binaire exactement à `/bin/ratelimit`, base distroless sans shell ;
  - fonctionne avec n'importe quel UID (Envoy Gateway force 65534), sur un système de fichiers en lecture seule et sans aucune capability ;
  - liaison IPv6 (`HOST="::"`) et sondes sur `/healthcheck` ;
  - mêmes ports (8080, 8081, 6070) et même `RUNTIME_ROOT` par défaut (`/srv/runtime_data/current`) ;
  - toutes les variables qu'Envoy Gateway utilise (xDS, TLS gRPC, TLS Redis, Prometheus, OTLP) 🔎 ;
  - signée avec cosign, avec SBOM et provenance, et testée en CI avec le Deployment généré par Envoy Gateway.
  
  Pas de chart Helm en v1.
- **D34. Licence Apache-2.0**, avec un NOTICE qui crédite envoy ratelimit et l'API d'Envoy, en conservant les en-têtes des protos vendorisés. Licences des dépendances générées avec `cargo-about`.

---

## Questions ouvertes sur lesquelles on veut ton avis tranché

1. **D14** : le mode `envoy` par défaut pour `rate_limits` est-il le bon choix, ou un utilisateur de fenêtres `anchored` s'attendra-t-il à ce qu'une requête refusée ne consomme rien ?
2. **D11** : la dette illimitée par défaut du token bucket est-elle acceptable (une grosse réponse LLM peut bloquer un utilisateur des heures), ou faut-il un plafond par défaut, même au prix de corrections perdues ?
3. **D10** : 10 sous-tranches par défaut est un choix, pas une mesure. Propose une valeur justifiée, avec le compromis précision/coût Redis.
4. **D25** : l'échappement de `{` et `}` dans les valeurs est-il sans collision ? Propose un schéma précis.
5. **D29** : réimplémenter le mapper statsd_exporter en Rust est-il réaliste avec une compatibilité exacte (globs, regex, échappement des noms non mappés) ? Existe-t-il une meilleure approche ?
6. **D9** : le mode `recurring` vaut-il sa complexité, sachant que les sources ne s'accordent pas sur le fonctionnement de la limite hebdomadaire de Claude ?
7. Y a-t-il une fonctionnalité, une variable d'environnement ou un comportement d'envoy ratelimit **absent** de D2 ?
8. Y a-t-il un comportement d'agentgateway (au-delà des corrections de tokens, `limitOverride`, `failureMode`, en-têtes `x-ratelimit-*`) que nos décisions cassent ?

## Format du retour attendu

Réponds en **français**, en Markdown, avec cette structure :

1. **Résumé exécutif** (10 lignes max) : les 3 à 5 problèmes les plus graves.
2. **Faits vérifiés** : un tableau de chaque fait 🔎, avec ✅ confirmé, ❌ faux ou ⚠️ partiellement vrai, et la preuve.
3. **Revue décision par décision** : pour chaque D1–D34 :
   - **Verdict** : GARDER / MODIFIER / REJETER
   - **Gravité** si problème : bloquant / majeur / mineur
   - **Problème** : un scénario concret qui casse
   - **Preuve** : `repo/chemin:ligne` ou URL
   - **Alternative recommandée**
   
   Pour les décisions que tu gardes sans réserve, une ligne suffit, mais dis ce que tu as vérifié.
4. **Incohérences entre décisions** : les paires ou groupes de décisions qui se contredisent.
5. **Manques** : les fonctionnalités, cas limites ou combinaisons non couverts.
6. **Réponses aux questions ouvertes** (1 à 8).
7. **Risques d'implémentation** : les 5 parties les plus difficiles à bien faire, et comment les dé-risquer (prototype, test, ordre de construction).

Ne termine pas ta revue tant que tu n'as pas lu le code source des fichiers listés dans « Sources à analyser ». Si tu n'as pas pu accéder à une source, dis-le explicitement plutôt que de supposer.
