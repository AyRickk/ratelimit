# Revue de conception — `ratelimit` (Rust)

Revue adversariale du prompt `docs/design-review-prompt.md`, réalisée le 2026-10-16.

**Sources effectivement lues** (code, pas seulement les README) :

| Source | Version lue | Remarque |
|---|---|---|
| envoyproxy/ratelimit | `f4e7669` (= `origin/main` au 2026-10-16, aucun commit plus récent) | `src/service/ratelimit.go`, `src/limiter/*`, `src/redis/*`, `src/memcached/cache_impl.go`, `src/config/*`, `src/settings/settings.go`, `src/stats/*`, `src/stats/prom/*`, `src/server/*`, `src/provider/*`, `src/utils/*`, `src/service_cmd/runner/runner.go`, `src/metrics/metrics.go`, `Dockerfile`, `test/redis/fixed_cache_impl_test.go`, `test/integration/`, `integration-test/`, `api/ratelimit/config/ratelimit/v3/rls_conf.proto`, README |
| agentgateway | `c624bd8` (= `origin/main`) | `crates/agentgateway/src/http/remoteratelimit.rs`, `crates/agentgateway/src/llm/mod.rs` (3494-3573), `crates/agentgateway/src/proxy/httpproxy.rs` (625-660), `crates/protos/build.rs`, `crates/protos/proto/rls.proto`, `crates/http/src/lib.rs` (208-228), `examples/` |
| envoyproxy/envoy | `main` du 2026-10-16 | `api/envoy/service/ratelimit/v3/rls.proto`, `api/envoy/extensions/common/ratelimit/v3/ratelimit.proto`, `source/extensions/filters/http/ratelimit/ratelimit_headers.cc`, `source/extensions/filters/common/ratelimit/ratelimit_impl.cc`, `source/extensions/filters/http/ratelimit/ratelimit.cc` ; PR 47307 via l'API GitHub |
| envoyproxy/gateway | `main` du 2026-10-16 | `internal/infrastructure/kubernetes/ratelimit/resource.go` |
| go-control-plane | commit `98966259b99a` épinglé par `go.mod` de ratelimit, et `main` | `envoy/service/ratelimit/v3/rls.pb.go` (grep `UnitMultiplier`) |
| Anthropic | — | « Rate limits » (platform.claude.com) lu intégralement ; « What is the Max plan » (support.claude.com/articles/11049741) lu. **Non accessible** : l'article 11014257 « About Claude Max plan usage » (301 puis 404) et l'article Pro 8324991 (404). |
| ccusage | `main` | `rust/crates/ccusage/src/blocks.rs` (le code TypeScript cité dans le prompt n'existe plus ; la logique a été portée en Rust) |

Convention : **fait** = vérifié dans le code cité ; **risque** = scénario plausible non observé ; **préférence** = jugement d'ingénierie. Les lignes sont celles des fichiers aux versions ci-dessus.

---

## 1. Résumé exécutif

1. **D20 contredit D15 et D1.** `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` ne signifie pas « ne pas compter les hits refusés » : dans la référence, la clé qui *dépasse* est bien incrémentée, ce sont ses *voisines* dans la même requête qui reçoivent `INCRBY 0` (`src/redis/fixed_cache_impl.go:65-93`, test `test/redis/fixed_cache_impl_test.go:~910-935`). L'option n'existe pas pour Memcached. La sémantique « nouveaux types » de D20 est donc l'inverse de celle de la référence, et D20 traduit mal `max_debt: 0`.
2. **La parité « quota_mode » est sous-spécifiée.** Depuis le commit `63c7e9c` (#1243), `quota_mode` regroupe les descripteurs par les clés **codées en dur** `backend_name` / `model_name_override`, ne renvoie `OVER_LIMIT` que si *tous* les groupes sont épuisés et émet `passedBackends` dans les métadonnées (`src/service/ratelimit.go:200-204, 233-308, 404-518`). Aucune décision (D2, D17, D19) ne mentionne ce comportement ni son interaction avec `rate_limits`.
3. **Deux « bizarreries » ne doivent pas être reproduites, contrairement à D1.** (a) Un `limit` client avec `unit: UNKNOWN` fait paniquer `UnitToDivider` (`src/utils/utilities.go:38`) hors du `recover` ciblé (`ratelimit.go:654-655`) : le processus Go **crashe**. (b) Les métadonnées dynamiques sérialisent `descriptor.Limit.String()` en *prototext Go* (`ratelimit.go:539`), format volontairement instable (espaces aléatoires) et impossible à reproduire octet pour octet.
4. **D23 et D13 reposent sur un fait partiellement faux.** La référence Go **ne connaît pas** `unit_multiplier` : zéro occurrence dans le dépôt, et la version de go-control-plane épinglée (`98966259b99a`, 2026-04) ne contient pas le champ (0 occurrence dans `rls.pb.go` ; 3 sur `main`). Envoy ratelimit ignore donc le multiplicateur d'un override ; « rejeter un multiplicateur à 0 » et « renvoyer le multiplicateur » sont des divergences (acceptables) et non de la parité. agentgateway, lui, ne peut ni envoyer ni lire le champ (`crates/protos/proto/rls.proto` : pas de `unit_multiplier`, pas de `WEEK`, pas de `is_negative_hits`).
5. **D9 `recurring` ne reproduit aucune source.** La doc officielle dit que la limite hebdomadaire « resets at a fixed time each week that is assigned to your account » : c'est une fenêtre **fixe** avec décalage par compte (ce que D8 reporte « à plus tard »), pas une fenêtre ancrée en chaîne. ccusage ouvre un nouveau bloc à la première entrée *après* expiration (`blocks.rs:71-90`), c'est le mode `session`. `recurring` ajoute de la complexité pour un comportement que personne n'a.
6. **Risque D33 :** Envoy Gateway injecte `GRPC_SERVER_TLS_CA_CERT` (`resource.go:61, 352-355`) alors que ratelimit lit `GRPC_CLIENT_TLS_CACERT` (`settings.go:45`). Le mTLS client n'est donc **pas** activé en amont. « Honorer » cette variable casserait le déploiement EG.
7. **D32 est juste mais incomplet :** `EXPIRATION_JITTER_MAX_SECONDS` vaut 300 par défaut (`settings.go:108`) et est une variable d'environnement, donc le test nocturne contre l'image officielle peut la mettre à 0 ; seul le gel d'horloge impose le programme Go.

---

## 2. Faits vérifiés

| # | Fait (🔎) | Verdict | Preuve |
|---|---|---|---|
| F1 | D4 : la v2 de l'API a été retirée en amont | ✅ | `src/service_cmd/runner/runner.go:222-225` (« v2 proto is no longer supported », seul `pb.RegisterRateLimitServiceServer` v3) |
| F2 | D4 : `hits_addend` par descripteur, 0 autorisé = vérifier sans compter ; au niveau requête 0 → 1 | ✅ | `src/utils/utilities.go:102-118` |
| F3 | D4 : `is_negative_hits` honoré | ✅ | `utilities.go:115`, `fixed_cache_impl.go:196-197`, script `base_limiter.go:20-27` (plancher 0, **remet le TTL à la durée complète**, toujours `OK`, `limit_remaining` borné) ; Memcached `cache_impl.go:195-201` |
| F4 | D5 : au rechargement, `config_load_error` et on garde l'ancienne config | ✅ ⚠️ | `ratelimit.go:65-76`. Nuance : une erreur qui n'est pas un `RateLimitConfigError` **panique** (`:69-71`) |
| F5 | D5 : au démarrage sans config valide, le service « tourne sans config » | ⚠️ | `ratelimit.go:686-690` attend le premier événement ; si config invalide, `this.config` reste `nil` et **chaque appel renvoie une erreur gRPC** (`:134`, `serviceError` → `codes.Unknown`), pas `OK`. Envoy voit `LimitStatus::Error` → `failure_mode_deny` décide ; agentgateway → `failClosed` 500 par défaut. Avec `FORCE_START_WITHOUT_INITIAL_CONFIG=false` et xDS injoignable, le serveur gRPC/HTTP ne démarre jamais (`NewService` bloque avant `srv.Start`, `runner.go:200-227`) |
| F6 | D5 : clés YAML inconnues rejetées | ✅ | `config_impl.go:75-92, 358-403` (`validKeys`, récursif, `metadata` exclu) |
| F7 | D5 : `metadata` renvoyé dans les métadonnées dynamiques | ✅ ⚠️ | `ratelimit.go:456-468` : seulement si `RESPONSE_DYNAMIC_METADATA=true`, seulement pour les descripteurs **passés**, fusion récursive des structs ; `limit` sérialisé en prototext Go (`:539`) |
| F8 | D6 : clé = `prefix + domaine + "_" + (clé "_" valeur "_")* + bucketStart`, aligné epoch | ✅ ⚠️ | `cache_key.go:66-94`. Nuances : (a) la valeur est remplacée par le motif wildcard si `share_threshold` (`:76-80`) ; (b) `MONTH` + `USE_CALENDAR_MONTH_RATE_LIMIT` → début de mois UTC (`:86-89`) ; (c) TTL = durée de l'unité + jitter, pas le temps restant (`fixed_cache_impl.go:190-193`) |
| F9 | D9 : la doc officielle ne confirme pas l'arrondi à l'heure | ✅ | « What is the Max plan » : « Your session-based usage limit will reset every five hours » ; aucune mention de début ni d'arrondi. Hebdo : « resets at a fixed time each week that is assigned to your account ». L'article 11014257 cité dans le prompt n'a pas pu être lu (404) |
| F10 | D9 : ccusage arrondit à l'heure (`floor_to_hour`) | ✅ | `rust/crates/ccusage/src/blocks.rs:89, 92, 105-107`. Et un nouveau bloc démarre à la première entrée après expiration ou après un trou > durée (`:71-90`) : sémantique `session`, pas `recurring` |
| F11 | D10 : le 0,003 % de Cloudflare ne vaut que pour poids 1 et fenêtres d'une minute | ⚠️ | Le billet dit « 0.003% of requests have been wrongly allowed or rate limited » sur « 400 million requests from 270,000 distinct sources ». Il **ne précise ni la fenêtre ni les poids**. La conclusion (chiffre non généralisable) tient, les conditions citées sont inventées |
| F12 | D12 : agentgateway tronque `x-ratelimit-reset` et arrondit `Retry-After` | ✅ | `remoteratelimit.rs:606-610` (`d.seconds.max(0)`, nanos ignorées) ; `:634-640` (`seconds + (nanos>0)`, `.max(1)`). Envoy fait `max(1, reset.seconds)` sans nanos (`ratelimit_headers.cc:131-139`) |
| F13 | D13 : `unit_multiplier` dans `rls.proto`, PR 47307 fusionnée le 2026-10-05 | ✅ | `rls.proto:119` (`UInt32Value unit_multiplier = 4 [gt:0]`), `ratelimit.proto:35` sur `RateLimitOverride` ; PR `merged_at: 2026-10-05T07:13:18Z`. Envoy ne l'envoie que si ≠ 1 (`ratelimit_impl.cc:68-70`) et l'utilise pour `w=` de `X-RateLimit-Limit` (`ratelimit_headers.cc:88-96`) |
| F14 | D13 : la référence Go et agentgateway gèrent / ignorent le champ | ❌ pour la référence, ✅ pour agentgateway | ratelimit `f4e7669` : 0 occurrence ; go-control-plane épinglé : 0 occurrence dans `rls.pb.go` (3 sur `main`). agentgateway : proto aplati sans le champ (`crates/protos/proto/rls.proto:24-38, 66-69`), prost ignore les champs inconnus |
| F15 | D13 : en-têtes par unité au pluriel et en majuscule initiale | ✅ | `ratelimit.go:399-402` (`Hours`, `Seconds`…) ; regroupement **par `Unit` seulement** (`:363-378`) |
| F16 | D14 : agentgateway envoie la correction de tokens après la réponse sans regarder le résultat | ✅ | `remoteratelimit.rs:138-155` (`let _ = … check_internal`), `llm/mod.rs:3494-3515`, `httpproxy.rs:641-647`. Premier appel avec `hits_addend: Some(0)` si `cost` est défini (`remoteratelimit.rs:262-265`), sinon estimation `input_tokens`. Corrections négatives **abandonnées** (`:195-200`). Si le premier appel est `OVER_LIMIT`, pas d'amendement (`:370`) ; en `failOpen` après erreur RLS, l'amendement est quand même envoyé (`:373`) |
| F17 | D15 : agentgateway envoie user et org comme descripteurs séparés | ⚠️ hypothèse | C'est une propriété de la **config utilisateur** : chaque `DescriptorEntry` devient un `RateLimitDescriptor` (`remoteratelimit.rs:236-293`). Les exemples du dépôt n'utilisent que `method`+`path` (`examples/traffic-ratelimiting-global/config.yaml`) ; l'exemple LLM par clé utilise le rate limit **local** |
| F18 | D15 : `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` à l'échelle de la requête, non atomique | ✅ ⚠️ | `fixed_cache_impl.go:139-202` : pipeline `GET` puis pipeline `INCRBY`. Sémantique réelle : si une clé est marquée over-limit dans le **cache local**, aucune clé n'est incrémentée (mais `INCRBY 0` + `EXPIRE` créent des clés fantômes à 0) ; sinon, seules les clés qui **vont dépasser** sont incrémentées, les autres reçoivent 0 (`:65-93`, test `fixed_cache_impl_test.go` « one key is reaching to the Overlimit threshold » : `INCRBY key4 0`, `INCRBY key5 2`, key5 `OVER_LIMIT`). Redis uniquement |
| F19 | D16 : deux passes, non transitif, cycle retire les deux, remplacées/`unlimited` remplacent quand même | ✅ | `ratelimit.go:139-194` : passe 1 collecte **tous** les `Replaces` (y compris des limites `unlimited`, `:172-179`, et de celles qui seront elles-mêmes remplacées), passe 2 annule les limites dont `Name` est dans l'ensemble. Une limite sans `name` ne peut pas être remplacée (`:184`). Config : nom vide ou auto-remplacement = erreur (`config_impl.go:298-305`) |
| F20 | D18 : avec le cache local, une limite en shadow n'est plus comptée | ✅ | `fixed_cache_impl.go:125-134` marque `overlimitIndexes` quel que soit `ShadowMode`, `:184` saute l'incrément. Le cache est rempli **avant** le test shadow (`base_limiter.go:136-148` vs `:160`), TTL = durée complète de l'unité (`:144`), pas le temps restant. Idem Memcached (`cache_impl.go:95-99, 191`) |
| F21 | D23 : multiplicateur à 0 rejeté | ❌ | La règle `gt: 0` est une annotation PGV ; le serveur Go ne valide pas et ne connaît pas le champ (F14). La référence **ignore** ; rejeter est une divergence |
| F22 | D23 : la `limit` client n'hérite ni du shadow ni du `replaces` | ✅ | `config_impl.go:471-487` : retour immédiat sans parcourir la config ; `unlimited=false, shadow=false, quota=false, name="", replaces=[]` ; stats sur `domain.key_value…` sanitisé (`:695-707`) |
| F23 | D29 : statsd en TCP, trois sorties exclusives | ✅ ⚠️ | `runner.go:71` `NewTCPStatsdSink` ; `:53-75` `Fatalf` si deux sinks. Nuance : `USE_STATSD` vaut **true par défaut** (`settings.go:87`), donc `USE_PROMETHEUS=true` seul est fatal ; `DISABLE_STATS` prime ; sinon sink de log stdout |
| F24 | D29 : un segment supplémentaire casserait le mapper par défaut et fusionnerait les séries | ⚠️ | `default_mapper.yaml:34-73` : pour un descripteur à ≥ 2 entrées, `(\..*)?` avale le segment → **fusion** ✅. Pour un descripteur à **1 entrée**, le segment devient `key2` → pas de fusion mais **mauvais label** |
| F25 | D31 : agentgateway compile ses protos aplatis avec protox | ✅ | `crates/protos/build.rs:50` |
| F26 | D32 : binaire statique, image distroless, horloge non gelable | ✅ | `Dockerfile:11` `CGO_ENABLED=0`, `:13` `distroless/static-debian12:nonroot`. Pas de `LD_PRELOAD` possible. `TimeSource` injectable (`utilities.go:12-15`). Mais le jitter est pilotable par `EXPIRATION_JITTER_MAX_SECONDS` (défaut 300) |
| F27 | D33 : variables injectées par Envoy Gateway | ✅ ⚠️ | `resource.go:303-376` (xDS, TLS gRPC, `FORCE_START…=true`, `USE_STATSD=false`, `RUNTIME_*`), `:378-414` Redis, `:416-427` Prometheus (mapper depuis une ConfigMap **optionnelle**), `:429-475` tracing, UID/GID 65534 (`:552-557`), sondes `/healthcheck:8080` (`:177-215`). **Piège :** `GRPC_SERVER_TLS_CA_CERT` n'est lu par aucun code de ratelimit |
| F28 | Anthropic API = token bucket, reset = seau plein | ✅ | « The API uses the token bucket algorithm … continuously replenished up to your maximum limit, rather than being reset at fixed intervals » ; `anthropic-ratelimit-*-reset` = « time when the … limit will be fully replenished » |

---

## 3. Revue décision par décision

### A. Périmètre et compatibilité

**D1 — Sur-ensemble strict.** Verdict : **MODIFIER** (mineur). Problème : « y compris ses bizarreries » inclut des crashs. Scénario : un client envoie `limit: {requests_per_unit: 10, unit: UNKNOWN}` → `UnitToDivider` panique (`utilities.go:38`), le `recover` ne connaît que `RedisError`/`serviceError` et re-panique (`ratelimit.go:654-655`) → le processus meurt. Autre cas : variable d'environnement invalide lue à chaque rechargement (`settings.NewSettings()` dans `ratelimit.go:95`, `file_provider.go:66`, `xds_grpc_sotw_provider.go:182`) → panique. Alternative : formuler D1 comme « identique pour toute entrée que la référence traite sans paniquer ; les paniques deviennent des erreurs gRPC `INVALID_ARGUMENT` et sont listées dans `docs/parity.md` comme divergences volontaires ».

**D2 — Parité complète.** Verdict : **MODIFIER** (majeur). La liste omet des comportements observables (détail en §5 et Q7) : groupes `quota_mode`, `RESPONSE_DYNAMIC_METADATA`, endpoint `/json`, port debug (`/rlconfig`, `/stats`, pprof), service gRPC `grpc.health.v1`, `GRPC_UDS`, rechargement à chaud des certificats serveur (`provider/cert_provider.go`), `REDIS_AUTH_FILE`, Memcached par SRV, binaires `config_check_cmd`/`client_cmd`, format de logs, ~45 variables d'environnement.

**D3 — Inventaire de parité.** Verdict : **GARDER**, à condition d'être généré depuis `settings.go` (360 lignes, chaque tag `envconfig` est une ligne de l'inventaire) et de couvrir aussi les **noms de métriques** (`manager_impl.go:33-69`, `metrics.go:21-26`, `driver_impl.go:30-37`, `local_cache_stats.go`), le runtime (`runtime.*`) et les stats `go.*`.

**D4 — API gRPC.** Verdict : **GARDER** (vérifié F1-F3, F22). Compléments à spécifier : `current_limit.name` est rempli (`config_impl.go:106-110`) ; les codes d'erreur gRPC (la référence renvoie `Unknown` avec le texte de la panique) ; `limit_remaining` sur `OVER_LIMIT` = 0 ; `Quota` et `raw_body` jamais remplis ; `RateLimitRequest.hits_addend` au niveau requête vaut 1 si 0 **sauf** si un `hits_addend` par descripteur est présent, même à 0 (`utilities.go:106-114`). Risque : agentgateway met toujours `hits_addend: 0` au niveau requête et une valeur par descripteur (`remoteratelimit.rs:327-328`), donc le chemin « par descripteur » est le chemin critique.

**D5 — Config invalide.** Verdict : **MODIFIER** (mineur). Voir F5 : « tourne sans config » = « répond une erreur gRPC à chaque appel ». Avec agentgateway en `failClosed` (défaut, `remoteratelimit.rs:40-49`) toutes les requêtes reçoivent 500 jusqu'au prochain rechargement valide. Préciser aussi : `HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED` évalue `IsEmptyDomains()` et non « config valide » (`ratelimit.go:78-88`) ; et le proto xDS ignore silencieusement les champs inconnus, contrairement au YAML.

**D6 — Clés Redis legacy.** Verdict : **GARDER** (F8), en ajoutant les trois nuances : substitution `share_threshold`, mois calendaire, et valeur vide (`key__`). Risque de migration : pendant la cohabitation, les **TTL** ne sont pas alignés sur la fin de fenêtre mais sur `durée + jitter` ; les deux services doivent utiliser le même `EXPIRATION_JITTER_MAX_SECONDS` sinon l'un prolonge des clés que l'autre croit expirées (sans conséquence sur la décision, la clé change de nom à la fenêtre suivante).

### B. Nouveaux types de fenêtre

**D7 — `rate_limits:`.** Verdict : **GARDER**. Vérifié : la grammaire actuelle est plate (`YamlRateLimit`, `config_impl.go:22-28`) et la validation des clés est une liste blanche (`:75-92`), donc l'ajout est additif. Point ouvert : `unlimited` interdit dans `rate_limits` (D16) mais `shadow_mode`, `quota_mode`, `detailed_metric` restent au niveau du **descripteur** dans la référence (`YamlDescriptor:30-41`), donc une liste ne peut pas avoir une limite en shadow et une autre non. À dire explicitement.

**D8 — `fixed`.** Verdict : **MODIFIER** (majeur). La limite hebdomadaire de Claude est une fenêtre fixe à **décalage par compte** (F9). Le report du décalage rend `fixed` incapable de reproduire la seule limite Claude dont le fonctionnement est documenté. Alternative : `offset: <durée>` ou `offset: from_first_hit` (décalage dérivé de la première requête et stocké dans la clé) en v1, et abandon de `recurring` (voir D9, Q6).

**D9 — `anchored`.** Verdict : **MODIFIER** (majeur). (a) `recurring` ne correspond à aucune source (F9, F10) : retirer. (b) « Une vérification à 0 hit n'ouvre rien et répond quota plein, reset dans la durée complète » casse le flux LLM d'agentgateway : avec `cost` défini, la **première** requête d'une session fait un check à 0 hit (`remoteratelimit.rs:262-265`), la fenêtre ne s'ouvre qu'à l'amendement **après** la réponse. Pour un stream de 4 minutes, l'ancre dérive de 4 minutes, et deux requêtes concurrentes ancrent chacune à leur fin. Avec `start_rounding: 1h` l'effet est masqué 11 fois sur 12 mais pas éliminé. Alternative : option `open_on_check: true` (par défaut pour `anchored`) qui ouvre la fenêtre au premier check même à 0 hit ; l'ancre est alors le début réel de la session. (c) Mode `session` : préciser que l'arrondi s'applique à l'ancre et que la fenêtre finit à `ancre + durée` (ccusage : `since_start > session_duration`).

**D10 — `sliding` / `sliding_approx`.** Verdict : **GARDER** sur le principe, **MODIFIER** la justification (F11). Risque : « capacité revient au plus D/N en retard » suppose que la sous-tranche courante est partielle et que les N tranches précédentes sont complètes, soit N+1 tranches stockées. Pour une fenêtre de 5 h et N = 10, un client bloqué attend jusqu'à 30 min de plus qu'avec un log glissant exact. Voir Q3.

**D11 — `token_bucket` en GCRA.** Verdict : **MODIFIER** (majeur) sur la dette illimitée (Q2). Vérifications : le TAT en microsecondes tient dans les 2^53 du double Lua jusqu'en l'an 2255 ✅ ; « admise si le seau n'est pas vide, débitée entièrement » correspond au modèle Anthropic « estimation puis ajustement » (F28). Problème non traité : `requests_per_unit / duration` avec `burst` par défaut = `requests_per_unit` pour une fenêtre de 7 jours donne un débit de remplissage très lent et un `burst` d'une semaine, ce qui n'est pas ce qu'attend un utilisateur venant de `fixed` ; documenter.

**D12 — `duration_until_reset`.** Verdict : **GARDER** (F12). Nuance : la référence legacy renvoie `sec - now % sec` (`utilities.go:53-60`), entier, jamais 0 ✅ ; en mois calendaire, `MonthExpirationSeconds` peut valoir 0 à la dernière seconde du mois (`time.go:46-48`), à reproduire tel quel pour legacy.

**D13 — Durée de fenêtre.** Verdict : **MODIFIER** (mineur). Vérifié F13-F15. Corrections : (a) la référence ignore `unit_multiplier` dans les overrides (F14), notre lecture est une extension ; (b) `RateLimit-Limit-5Hours` : la référence regroupe les en-têtes par `Unit` seul (`ratelimit.go:363-378`), donc pour une config legacy la sortie doit rester `…-Hours` ; le suffixe `5Hours` ne doit apparaître que si le multiplicateur ≠ 1. (c) Envoy calcule `w=` avec `max(unit_multiplier, 1)` (`ratelimit_headers.cc:88-91`) : un multiplicateur à 0 donnerait `w=durée` ; cohérent avec un rejet côté serveur, mais la référence ignore (F21).

### C. Sémantique du comptage

**D14 — `overflow: envoy | strict`.** Verdict : **GARDER** (F16). Cohérent avec agentgateway. Ajouter : en `strict`, un amendement agentgateway refusé est **perdu** sans aucun signal côté agentgateway (`let _ =`), donc la doc doit dire que `strict` sous-compte les tokens en cas de dépassement. Voir Q1.

**D15 — Plusieurs descripteurs.** Verdict : **MODIFIER** (mineur). F17 : « agentgateway envoie user et org séparés » est une hypothèse de configuration, pas un comportement du client. F18 : la description de `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` est juste mais incomplète ; écrire la table de vérité (cache local over-limit → tout à 0 ; sinon clés qui dépassent → incrémentées, autres → 0 ; `INCRBY 0` crée des clés fantômes). `REDIS_PERSECOND` : vérifié que le routage est par clé `PerSecond` (`cache_key.go:98`, `fixed_cache_impl.go:48-59`) ; la règle « toute la liste va dans un seul Redis » est une divergence raisonnable à documenter, car la référence n'a jamais deux limites par descripteur.

**D16 — `replaces`.** Verdict : **GARDER** (F19). Préciser deux détails vérifiés : une limite sans `name` est irremplaçable ; une limite `unlimited` qui est elle-même « remplacée » reste `OK` avec `limit_remaining = MaxUint32` (`ratelimit.go:258-264`), le remplacement n'ayant d'effet que sur `limitsToCheck`.

**D17 — Fonctionnalités Envoy sur les nouvelles fenêtres.** Verdict : **MODIFIER** (bloquant pour `quota_mode`). `quota_mode` n'est pas un simple indicateur : voir résumé point 2 et §5. Il faut décider ce que vaut « le groupe est over » quand un descripteur porte plusieurs limites (toutes ? une ?), et si `passedBackends` est émis par descripteur ou par limite. `share_threshold` : la clé Redis utilise le motif wildcard à la place de la valeur (`cache_key.go:76-80`) ; D25 doit reprendre cette substitution.

**D18 — Cache local.** Verdict : **GARDER** (F20). Préciser : TTL de l'entrée = durée de l'unité complète (pas le temps restant), entrée posée même pour les limites shadow, et `over_limit_with_local_cache` compte `hitsAddend` entier (`base_limiter.go:119-120`).

**D19 — `quota_mode` + `sliding`/`token_bucket` sans `max_debt`.** Verdict : **GARDER** comme avertissement, mais seulement après résolution de D17.

**D20 — `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` sur les nouveaux types.** Verdict : **REJETER** tel quel (majeur). Voir F18 : dans la référence, l'option **ne protège pas la clé qui dépasse**, elle épargne ses voisines dans la même requête. Trois sens possibles pour les nouveaux types : (1) reproduire littéralement (voisines à 0 quand une limite va dépasser) ; (2) « strict par requête » ; (3) l'option n'a aucun effet sur les nouveaux types et l'on documente d'utiliser `overflow`. Recommandation : (3), c'est le seul choix qui ne crée pas une troisième sémantique. « `strict` l'emporte » est alors inutile. L'équivalence avec `max_debt: 0` est fausse dans tous les cas.

**D21 — Remboursements.** Verdict : **GARDER**, en ajoutant les propriétés vérifiées de la référence (F3) : réponse toujours `OK`, `limit_remaining = max(0, limite − compteur)`, aucun effet sur le cache local, stat `total_negative_hits`, TTL remis à la durée complète, et **Memcached décrémente de façon asynchrone après la réponse** avec une valeur prédite (`cache_impl.go:141-150`).

**D22 — `near_limit` / `over_limit`.** Verdict : **GARDER**, en reproduisant exactement l'arithmétique legacy : `nearLimitThreshold = floor(float64(float32(limit) * float32(ratio)))` (`base_limiter.go:127`), répartition des `hitsAddend` entre `near_limit` et `over_limit` (`:208-237`), `within_limit` incrémenté de `hitsAddend` seulement si `OK`, `shadow_mode` (`:239-247`). Le passage par `float32` est un détail qui fera échouer la comparaison exacte si on calcule en `f64`.

**D23 — Limite envoyée par le client.** Verdict : **MODIFIER** (mineur). F14/F21 : la référence ignore `unit_multiplier` ; nos trois ajouts (lecture, renvoi, rejet du 0) sont des extensions. « Mode de comptage de la règle correspondante » demande une recherche de règle que la référence ne fait pas (`config_impl.go:471-487`) ; acceptable mais à écrire comme divergence. Risque agentgateway : `limitOverride` n'accepte pas `week` (`remoteratelimit.rs:518-526`) ; rien à faire côté serveur.

### D. Stockage, clés et horloge

**D24 — Backends.** Verdict : **GARDER**. Vérifié : Memcached n'a ni `STOP_CACHE_KEY…`, ni client par seconde, ni atomicité (décision sur `GET` + incrément asynchrone, `cache_impl.go:128-168`). Le harnais de comparaison (D32) doit tolérer ce modèle (deux requêtes concurrentes passent toutes deux à la limite). Clé Memcached ≤ 250 octets et sans espace/contrôle (`memcache.ErrMalformedKey`, commentaire `cache_impl.go:13-14`) ; la référence journalise l'erreur et répond `OK` : à reproduire.

**D25 — Clés des nouveaux types.** Verdict : **MODIFIER** (mineur). Ajouter la substitution `share_threshold` (D17), la limite de 250 octets Memcached pour `fixed` dans une liste, et un schéma d'échappement précis (Q4). Hash tag : fred route les scripts sur la première clé, et la forme `{…}suffixe` garantit que seul le contenu des accolades compte ✅.

**D26 — Identité d'une limite.** Verdict : **MODIFIER** (mineur). Incohérence interne : « le nom entre dans la clé seulement si deux limites ont la même forme » rend la clé de la **première** limite dépendante de l'ajout ultérieur d'une **seconde** : ajouter une limite `fixed 7d` nommée à côté d'une `fixed 7d` existante remet la première à zéro. Alternative : le nom entre **toujours** dans la clé quand il est présent, et il est obligatoire dans `rate_limits` (D29 en a besoin pour `~weekly` de toute façon).

**D27 — Horloge.** Verdict : **GARDER**. L'argument « TIME casse le routage Cluster » est juste si les clés de sous-tranches sont dérivées en script (non déclarées dans `KEYS`). « Échouait sur Dragonfly » : non vérifié ici, **hypothèse**. Ajouter : une seule lecture d'horloge par requête est aussi ce que fait la référence (`base_limiter.go:66`), sauf pour les en-têtes `LIMIT_*` qui relisent l'horloge (`ratelimit.go:594, 615`) : à reproduire pour le test exact.

### E. Configuration et observabilité

**D28 — Rechargement et xDS.** Verdict : **GARDER**, avec précision : le proto xDS amont **ne sait pas non plus** décrire `value_to_metric` ni `share_threshold` (`api/ratelimit/config/ratelimit/v3/rls_conf.proto:32-58`, `config_xds.go:16-32`). Donc « config xDS supportée pour les fonctionnalités Envoy » = pour les fonctionnalités que le proto porte. Détails du fournisseur fichier à reproduire : clés goruntime (`RUNTIME_APPDIRECTORY` préfixe, `RUNTIME_WATCH_ROOT` ⇒ `SymlinkRefresher` sur le lien `current`, sinon `DirectoryRefresher`, `file_provider.go:72-103`), scope de stats `runtime`.

**D29 — Métriques.** Verdict : **MODIFIER** (mineur). F23/F24 : documenter `USE_STATSD=true` par défaut. L'astuce `~` fonctionne pour les deux familles de règles du mapper par défaut (`*` et `[^\.]*` acceptent `~`), mais `SanitizeStatName` ne touche que `:`, `|` et les points des IPv4 (`utilities.go:80-88`) ; vérifier que `~` ne gêne ni gostats TCP ni DogStatsD (tags séparés par `|#`). Le label `limit=` à valeur vide est **équivalent à son absence** en Prometheus, donc sans effet sur les séries legacy ✅. Le mapper EG pointe un fichier d'une ConfigMap optionnelle ; si absent, `InitFromFile` échoue et l'erreur est ignorée (`prometheus_sink.go:136`) → aucune métrique mappée : à reproduire ou à documenter comme divergence. Voir Q5.

### F. Code, tests et livraison

**D30 — Stack.** Verdict : **GARDER**. Point d'attention : `fred` + TLS → choisir `rustls` pour que `cargo-zigbuild` produise un binaire sans OpenSSL (D33).

**D31 — Organisation.** Verdict : **GARDER**. Remarque : « chaque algorithme est une fonction pure réécrite en Lua » ne couvre pas le chemin legacy, qui n'est **pas** un script mais `INCRBY` + `EXPIRE` pipelinés avec décision côté client (`fixed_cache_impl.go:35-38`). Pour la parité octet pour octet des clés et des commandes observables (un `MONITOR` Redis doit montrer la même séquence), le backend legacy doit rester hors du modèle « script ».

**D32 — Tests.** Verdict : **GARDER**, trois ajouts. (1) Le programme Go doit aussi exposer l'`EXPIRATION_JITTER_MAX_SECONDS` et le `JitterRand` seedé ; les tests amont le font déjà (`fixed_cache_impl_test.go:828`). (2) Le test nocturne peut mettre `EXPIRATION_JITTER_MAX_SECONDS=0` par variable d'environnement (F26). (3) Les scripts `integration-test/*.sh` dorment 60 s (`quota.sh:26`) et dépendent d'un Envoy réel ; c'est un test de fumée, pas de parité.

**D33 — Image.** Verdict : **MODIFIER** (majeur, F27). (a) Ne pas interpréter `GRPC_SERVER_TLS_CA_CERT`. (b) Le `/healthcheck` n'est servi qu'après la connexion Redis initiale (retry infini par défaut, `driver_impl.go:280-348`, `runner.go:192` avant `srv.Start` `:227`) ; EG tolère 5 minutes (`StartupProbe` 30 × 10 s). (c) `HOST="::"` : la référence écoute en dual-stack via Go ; en Rust, binder `[::]` avec `IPV6_V6ONLY=0` explicitement. (d) `SO_REUSEPORT` est utilisé pour HTTP, debug et gRPC (`server_impl.go:171, 187, 209`) ; sans lui, deux instances sur le même hôte réseau se comportent différemment. (e) Le test « avec le Deployment généré par EG » exige EG lui-même (xDS SoTW TLS sur `envoy-gateway:18001`).

**D34 — Licence.** Verdict : **GARDER**. Vérifié : LICENSE Apache-2.0 en amont ; les protos Envoy portent des en-têtes à conserver.

---

## 4. Incohérences entre décisions

| Décisions | Incohérence |
|---|---|
| D1 ↔ D5, D23 | « Bizarreries à l'identique » inclut des paniques (unit `UNKNOWN`, env invalide au rechargement) ; D5 et D23 supposent des réponses d'erreur propres. |
| D15 ↔ D20 | D15 reproduit `STOP_CACHE_KEY…` exactement ; D20 lui donne un sens différent (« ne pas compter les hits refusés ») qui est l'inverse du comportement réel pour la clé dépassante (F18). |
| D20 ↔ D14 | « `strict` l'emporte » suppose que l'option et `overflow` sont du même ordre ; l'option est globale (env) et par requête, `overflow` est par descripteur. |
| D8 ↔ D9 | D8 reporte le décalage de fenêtre fixe ; D9 ajoute `recurring` pour la limite hebdomadaire. Or la limite hebdomadaire documentée est une fenêtre fixe décalée (F9). |
| D9 ↔ D14 ↔ F16 | Fenêtre ouverte « à la première requête qui compte des hits > 0 » + agentgateway qui envoie 0 hit puis amende après la réponse ⇒ l'ancre est posée à la fin de la première réponse, pas au premier message. |
| D13 ↔ D23 | D13 dit « vendoriser le `rls.proto` à jour » et D23 « rejeter 0 » comme si la référence le faisait ; la référence ne connaît pas le champ (F14). |
| D13 ↔ D2 (`LIMIT_PER_UNIT_HEADERS_ENABLED`) | Le suffixe `5Hours` est un regroupement par (unité, multiplicateur) ; la référence regroupe par unité. Pour les configs legacy la sortie doit rester inchangée. |
| D17 ↔ D19 ↔ D2 | `quota_mode` est traité comme un drapeau de script alors que sa sémantique amont est un regroupement cross-descripteurs calculé dans le service (`ratelimit.go:233-308`). |
| D25 ↔ D17 | D25 ne reprend pas la substitution de valeur par motif `share_threshold` que D17 étend à tous les types. |
| D26 ↔ D29 | D26 rend le nom optionnel dans la clé ; D29 en a besoin pour l'identifiant métrique `~nom`. Sans nom, le suffixe métrique n'est pas défini. |
| D26 (interne) | « Changer seulement le plafond garde les compteurs » mais ajouter une limite de même forme change la clé de la limite existante. |
| D18 ↔ D21 | D18 : « un remboursement efface l'entrée locale » ; la référence ne touche pas le cache local sur remboursement (`fixed_cache_impl.go:119-121` saute les négatifs). Divergence volontaire, à marquer comme telle pour les types legacy. |
| D31 ↔ D6 | Le modèle « tout algorithme = script Lua » ne s'applique pas au legacy, qui doit rester `INCRBY`+`EXPIRE` pour que les clés, TTL et séquences soient identiques. |

---

## 5. Manques

**Comportements de la référence non couverts par les décisions**

- **Groupes `quota_mode`** (`ratelimit.go:200-204, 233-308, 404-518`) : clés `backend_name` / `model_name_override`, `OVER_LIMIT` seulement si tous les groupes sont épuisés, `passedBackends` dans les métadonnées, exclusion des descripteurs passés d'un groupe épuisé.
- **Métadonnées dynamiques** (`ratelimit.go:432-570`) : `domain`, `descriptors[].entries` (`"k=v"`), `descriptors[].limit` (prototext Go), `hitsAddend`, `metadata` fusionné (structs fusionnés récursivement, listes **non** fusionnées, premier gagnant).
- **Endpoint `/json`** (`server_impl.go:84-140`) : protojson, 429 sur `OVER_LIMIT`, 500 sur `UNKNOWN`, 400 sur erreur de service.
- **Port debug 6070** : `/`, `/rlconfig` (format `Dump()`), `/stats` (expvar Go, sans équivalent), `/debug/pprof/*` (sans équivalent).
- **Service gRPC `grpc.health.v1`** (`server_impl.go:292`) avec le nom de service `ratelimit` ; `/healthcheck` renvoie 500 sans corps si KO (`health.go:82-89`) ; SIGTERM ⇒ unhealthy **et** arrêt gracieux immédiat (`health.go:71-78`, `runner.go:155-164`).
- **gRPC** : `GRPC_UDS`, `GRPC_MAX_CONNECTION_AGE` 24 h / `_GRACE` 1 h, interceptor de métriques `ratelimit_server.<méthode>.total_requests|response_time` (`metrics.go`), `otelgrpc` stats handler, mTLS avec `GRPC_CLIENT_TLS_CACERT` et vérification de SAN `GRPC_CLIENT_TLS_SAN`, rechargement à chaud du certificat serveur par surveillance du répertoire (`cert_provider.go`).
- **Redis** : `REDIS_AUTH_FILE` et IAM AWS (identifiants résolus à chaque connexion), `REDIS_CLOSE_CONNECTION_ON_READONLY_ERROR`, `REDIS_CLUSTER_PIPELINE_PARALLELISM`, `REDIS_STARTUP_*` (retry exponentiel, PING attendu `PONG`), `REDIS_POOL_ON_EMPTY_BEHAVIOR` (`CREATE`/`ERROR` ⇒ panique au démarrage), `REDIS_PIPELINE_WINDOW`, format `REDIS_URL` (liste pour cluster, `master,sentinel1,…` pour sentinel, `redis://user:pass@host`), `REDIS_AUTH` « user:pass », stats `redis_pool.cx_active|cx_total|cx_local_close`, groupement par clé en Cluster (`driver_impl.go:412-477`).
- **Memcached** : `MEMCACHE_SRV` + `MEMCACHE_SRV_REFRESH`, `MEMCACHE_MAX_IDLE_CONNS`, TLS, expiration absolue au-delà de 30 jours (`cache_impl.go:170-184`), incréments asynchrones avec `add` sur `ErrCacheMiss`.
- **Matching** : wildcard final **et médian** dans les valeurs (`config_impl.go:200-238`), repli clé seule quand `clé_valeur` ne matche pas, `detailed_metric` et `value_to_metric` (clé de stats enrichie, `:640-686`), `share_threshold` (clé Redis et clé de stats avec le motif).
- **Stats** : `domain_not_found`, `over_limit_with_local_cache`, `within_limit`, `total_negative_hits`, `global_shadow_mode`, `config_load_success|error`, `localcache.*` (8 jauges), `runtime.*` (goruntime), `go.*`, `memcache.*`, format `EXTRA_TAGS` de gostats, échappement `SanitizeStatName`, métriques propres de statsd_exporter (`statsd_exporter_events_*`).
- **Logs** : `LOG_FORMAT=json` avec `@timestamp` / `@message` (`runner.go:103-110`).
- **Traces** : noms de spans et attributs (`ShouldRateLimit Execution`, `Redis Pipeline Execution`, `request string`, `pipeline length`).
- **Validation de requête** : domaine vide ou liste vide ⇒ erreur de service (`ratelimit.go:209-210`) ; domaine inconnu ⇒ `OK` sans `current_limit` + `domain_not_found`.
- **Config** : `requests_per_unit: 0` accepté (tout hit est over-limit) ; `unlimited` + `unit` ⇒ erreur ; domaine dupliqué ⇒ erreur sauf `MERGE_DOMAIN_CONFIG` ; clés composées dupliquées ⇒ erreur.

**Cas limites non spécifiés pour les nouveaux types**

- `hits_addend` > plafond (une réponse LLM de 200 k tokens sur une limite de 100 k) : `fixed` compte et refuse ; `anchored` ouvre-t-il la fenêtre sur une requête refusée en mode `envoy` ? (D9 dit non pour les refusées, D14 dit que tout est compté : lequel ?)
- `is_negative_hits` sur `anchored` **fermée** ou jamais ouverte, et sur `sliding` quand la tranche la plus récente est vide.
- `unit_multiplier` avec `unit: MONTH` + `USE_CALENDAR_MONTH_RATE_LIMIT`.
- Deux limites de la même liste sur `REDIS_PERSECOND` (D15 dit « un seul Redis » : lequel ?).
- `share_threshold` + `rate_limits` : le motif remplace la valeur dans la partie `{…}` ?
- Changement de `buckets` sur un `sliding` existant : nouvelle forme ⇒ remise à zéro (D26) ; à confirmer.
- Débordements : `limit_remaining` est `uint32` ; un `token_bucket` de 5 M tokens avec `burst` 5 M rentre, mais un `fixed` à 5 G tokens non.

---

## 6. Réponses aux questions ouvertes

**Q1 — `envoy` par défaut pour `rate_limits` (D14).** Garder `envoy`. Argument de fait : agentgateway amende après coup sans lire le résultat (F16) ; tout autre défaut perd des tokens silencieusement. Argument de coût : pour `fixed` et `anchored`, sur-compter une requête refusée n'a **aucun effet visible** sur la décision (la fenêtre finit à heure fixe, `limit_remaining` est déjà 0), seulement sur `over_limit`/`total_hits`. Le seul type où le défaut `envoy` fait mal est `token_bucket` (dette). Donc : `envoy` partout, et un avertissement au chargement pour `token_bucket` + `envoy` sans `max_debt` (préférence).

**Q2 — Dette illimitée (D11).** Non acceptable par défaut. Scénario : limite 10 k tokens/min, réponse de 300 k tokens amendée par agentgateway ⇒ 30 minutes de blocage, pendant lesquelles chaque check à 0 hit est refusé. Anthropic lui-même borne l'estimation en ajustant « during the request » (F28) et ne bloque pas des heures. Recommandation : `max_debt` par défaut = `burst` (la dette est au plus un seau), `max_debt: unlimited` explicite pour qui veut l'exactitude. Les corrections au-delà du plafond sont tronquées et comptées dans une stat `debt_capped` pour rester observables.

**Q3 — Nombre de sous-tranches (D10).** Stocker les N tranches dans **un hash Redis par (descripteur, limite)** (`HINCRBY` champ = index de tranche, `HEXPIRE` ou TTL sur le hash, nettoyage des champs périmés dans le script) : le coût réseau devient indépendant de N (un script, une clé, un slot) ; le coût restant est O(N) CPU Lua et ~N × 16 octets mémoire. Avec ce stockage, la précision est le seul critère : retard maximal de retour de capacité = D/N. Proposition : `buckets: 12` par défaut (D/N = 5 s pour 1 min, 5 min pour 1 h, 25 min pour 5 h, 14 h pour 7 j), avec un plancher d'une tranche ≥ 1 s. 12 divise 60 et 3600, donc les frontières de tranche tombent sur des secondes entières pour les durées usuelles ; 10 ne divise pas 3600. Pour 7 j, recommander 56 ou 84 dans la doc (3 h / 2 h). Ce reste un choix (préférence) ; le compromis est documenté par la formule, pas par une mesure.

**Q4 — Échappement de `{` et `}` (D25).** Le schéma est sans collision si et seulement si le caractère d'échappement est lui-même échappé. Proposition : dans la partie `{…}`, remplacer `%` → `%25`, `{` → `%7B`, `}` → `%7D` dans **domaine, clés et valeurs** ; ne pas toucher `_` (la référence ne l'échappe pas et `clé="a_b",valeur="c"` collisionne déjà avec `clé="a",valeur="b_c"` en legacy, `cache_key.go:70-83` ; reproduire). Le suffixe commence au premier `}` non échappé ; comme toute `}` interne est encodée, le découpage est unique. Vérifier aussi que le résultat tient en 250 octets pour Memcached et qu'il n'y a ni espace ni caractère de contrôle (sinon erreur `ErrMalformedKey` à reproduire).

**Q5 — Réimplémenter le mapper statsd_exporter (D29).** Réaliste pour le **sous-ensemble** réellement utilisé : `match` glob (`*` = `[^.]*`), `match_type: regex`, `name` et `labels` avec `$n`, `match_metric_type`, `action: drop`, `timer_type`/`observer_type` + `buckets`/`quantiles`, section `defaults` (`match_type`, `glob_disable_ordering`, `ttl`). Deux pièges d'exactitude : (1) statsd_exporter évalue **tous les globs avant toutes les regex** (FSM), les regex ensuite dans l'ordre : l'ordre du fichier n'est pas l'ordre d'évaluation ; (2) les noms non mappés sont échappés (`.`→`_`, caractères invalides →`_`, préfixe `_` si chiffre initial) et les conflits de type émettent `statsd_exporter_events_conflict_total`. Meilleure approche : implémenter le sous-ensemble, **refuser au démarrage** toute clé de mapper non supportée, et valider par un test différentiel qui rejoue le même flux statsd dans un vrai `statsd_exporter` v0.26.1 et compare `/metrics`. Alternative sans réimplémentation : sortie statsd + sidecar statsd_exporter, mais ce n'est pas un drop-in pour EG qui passe `USE_PROMETHEUS=true` et un mapper (`resource.go:416-427`).

**Q6 — `recurring` (D9).** Non. La source officielle décrit la limite hebdomadaire comme une fenêtre fixe à heure de reset propre au compte (F9) ; ccusage implémente `session` (F10) ; aucune source ne décrit un enchaînement de fenêtres depuis la première requête. Remplacer par `fixed` + `offset` (D8), qui reproduit la limite hebdomadaire et coûte une addition dans le calcul de `bucketStart`.

**Q7 — Absent de D2.** Voir §5. En variables d'environnement, le « etc. » doit expliciter : `HOST`, `PORT`, `DEBUG_HOST`, `DEBUG_PORT`, `GRPC_HOST`, `GRPC_PORT`, `GRPC_UDS`, `GRPC_MAX_CONNECTION_AGE(_GRACE)`, `GRPC_SERVER_USE_TLS`, `GRPC_SERVER_TLS_CERT/KEY`, `GRPC_CLIENT_TLS_CACERT`, `GRPC_CLIENT_TLS_SAN`, `LOG_LEVEL`, `LOG_FORMAT`, `CONFIG_TYPE`, `FORCE_START_WITHOUT_INITIAL_CONFIG`, `CONFIG_GRPC_XDS_NODE_ID`, `CONFIG_GRPC_XDS_NODE_METADATA`, `CONFIG_GRPC_XDS_SERVER_URL`, `CONFIG_GRPC_XDS_SERVER_CONNECT_RETRY_INTERVAL`, `CONFIG_GRPC_XDS_CLIENT_ADDITIONAL_HEADERS`, `CONFIG_GRPC_XDS_SERVER_USE_TLS`, `CONFIG_GRPC_XDS_CLIENT_TLS_CERT/KEY`, `CONFIG_GRPC_XDS_SERVER_TLS_CACERT`, `CONFIG_GRPC_XDS_SERVER_TLS_SAN`, `XDS_CLIENT_BACKOFF_INITIAL_INTERVAL|MAX_INTERVAL|RANDOM_FACTOR|JITTER`, `XDS_CLIENT_MAX_MSG_SIZE_IN_BYTES`, `USE_DOG_STATSD`, `USE_DOG_STATSD_MOGRIFIERS`, `USE_STATSD`, `STATSD_HOST`, `STATSD_PORT`, `EXTRA_TAGS`, `STATS_FLUSH_INTERVAL`, `DISABLE_STATS`, `SANITIZE_DESCRIPTOR_METRIC_DOTS`, `USE_PROMETHEUS`, `PROMETHEUS_ADDR`, `PROMETHEUS_PATH`, `PROMETHEUS_MAPPER_YAML`, `PROMETHEUS_RESPONSE_TIME_AS_MILLISECONDS`, `RUNTIME_ROOT`, `RUNTIME_SUBDIRECTORY`, `RUNTIME_APPDIRECTORY`, `RUNTIME_IGNOREDOTFILES`, `RUNTIME_WATCH_ROOT`, `EXPIRATION_JITTER_MAX_SECONDS`, `LOCAL_CACHE_SIZE_IN_BYTES`, `NEAR_LIMIT_RATIO`, `CACHE_KEY_PREFIX`, `BACKEND_TYPE`, `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`, `USE_CALENDAR_MONTH_RATE_LIMIT`, `LIMIT_RESPONSE_HEADERS_ENABLED`, `LIMIT_LIMIT_HEADER`, `LIMIT_REMAINING_HEADER`, `LIMIT_RESET_HEADER`, `LIMIT_PER_UNIT_HEADERS_ENABLED`, `LIMIT_REQUEST_HEADERS_ENABLED`, `LIMIT_REQUEST_LIMIT|REMAINING|RESET_HEADER`, `HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED`, `REDIS_SOCKET_TYPE`, `REDIS_TYPE`, `REDIS_URL`, `REDIS_POOL_SIZE`, `REDIS_AUTH`, `REDIS_AUTH_FILE`, `REDIS_AWS_IAM_AUTH|CACHE_NAME|USER_ID|SERVERLESS|REGION`, `REDIS_TLS`, `REDIS_TLS_CLIENT_CERT|KEY`, `REDIS_TLS_CACERT`, `REDIS_TLS_SKIP_HOSTNAME_VERIFICATION`, `REDIS_PIPELINE_WINDOW`, `REDIS_PIPELINE_LIMIT` (déprécié, avertissement), `REDIS_CLUSTER_PIPELINE_PARALLELISM`, `REDIS_PERSECOND` et ses 17 variables miroir `REDIS_PERSECOND_*`, `REDIS_SENTINEL_AUTH`, `REDIS_HEALTH_CHECK_ACTIVE_CONNECTION`, `REDIS_CLOSE_CONNECTION_ON_READONLY_ERROR`, `REDIS_TIMEOUT`, `REDIS_POOL_ON_EMPTY_BEHAVIOR`, `REDIS_STARTUP_INITIAL_INTERVAL|MAX_INTERVAL|MAX_ELAPSED_TIME`, `MEMCACHE_HOST_PORT`, `MEMCACHE_MAX_IDLE_CONNS`, `MEMCACHE_SRV`, `MEMCACHE_SRV_REFRESH`, `MEMCACHE_TLS` et ses 4 variables, `SHADOW_MODE`, `QUOTA_MODE`, `RESPONSE_DYNAMIC_METADATA`, `MERGE_DOMAIN_CONFIG`, `TRACING_ENABLED`, `TRACING_SERVICE_NAME|NAMESPACE|INSTANCE_ID`, `TRACING_EXPORTER_PROTOCOL`, `TRACING_SAMPLING_RATE`, plus les `OTEL_EXPORTER_OTLP_*` lus par le SDK (`settings.go:18-289`). Et **`GRPC_SERVER_TLS_CA_CERT` doit figurer dans l'inventaire comme « ignoré, comme en amont »**.

**Q8 — Comportements d'agentgateway que les décisions cassent.**
1. **Ancre `anchored` posée après la réponse** (D9 × F16) : décrit en D9 ; c'est le seul cas où une décision change le résultat pour agentgateway.
2. **`strict` perd les amendements refusés** sans signal (F16) ; pas une casse, mais un sous-comptage silencieux à documenter.
3. **`x-ratelimit-reset` sur réponses autorisées** (`remoteratelimit.rs:496-499`) = reset du statut au plus petit `limit_remaining` ; pour `token_bucket` autorisé, D12 renvoie « temps jusqu'au seau plein », donc un client verra un reset lointain alors qu'il peut réessayer tout de suite. Conforme à Anthropic (F28), surprenant pour un client Envoy.
4. **Proto aplati sans `WEEK`** : une `current_limit.unit = WEEK (7)` arrive comme valeur d'enum inconnue ; agentgateway ne lit que `requests_per_unit`, `limit_remaining`, `duration_until_reset` (`remoteratelimit.rs:598-616`) ⇒ sans effet. `limitOverride` ne peut pas exprimer `week` ni de multiplicateur.
5. **`overall_code != OK` ⇒ refus** (`remoteratelimit.rs:483`) : un `UNKNOWN` est traité comme un refus 429. La référence ne renvoie jamais `UNKNOWN` ; ne pas le faire non plus.
6. **`failOpen` + amendement** : après une erreur RLS sur le premier appel, agentgateway amende quand même (`:373`) ; un service qui redémarre reçoit des `hits_addend` sans check préalable. Normal pour `envoy`, à tester pour `strict` (le `>=` à 0 hit n'intervient pas, l'amendement est > 0).
7. **Métadonnées dynamiques et `quota`** : agentgateway les ignore (`:473-480`) ; les groupes `quota_mode` ne lui servent à rien, mais `OVER_LIMIT` global dépend du calcul de groupes, donc il faut quand même le reproduire.

---

## 7. Risques d'implémentation

1. **Parité octet pour octet du chemin legacy Redis.** Séquence `INCRBY`/`EXPIRE`, deux phases `GET` puis `INCRBY` avec `STOP_CACHE_KEY…`, clés fantômes à 0, cache local posé avant le test shadow, arithmétique `float32` des seuils, `TTL = unité + jitter`, horloge relue pour les en-têtes. Dé-risquer : construire d'abord le harnais Go différentiel (D32.1) avec `MONITOR` Redis comparé commande par commande, puis implémenter le legacy, **avant** tout nouveau type.
2. **Groupes `quota_mode` et métadonnées dynamiques.** Sémantique récente (#1243), clés codées en dur, prototext Go non reproductible. Dé-risquer : spécifier le format `limit` cible (protojson ou texte stable) comme divergence documentée, écrire les cas de test depuis `test/service/ratelimit_test.go:1080-1330` et vérifier qu'Envoy et EG ne consomment pas `descriptors[].limit`.
3. **Scripts Lua des nouveaux types sous Redis Cluster.** Une clé par (descripteur, limite), hash tag, `now` en `ARGV`, TAT en microsecondes, nettoyage des tranches dans le script, comportement identique Redis 6/7/Valkey. Dé-risquer : tests de propriétés Rust ↔ Lua (D32.4) sur un Redis réel en mode cluster dans la CI (pas seulement `redis-server` mono-nœud), et un fuzz des séquences `check(0)` / `amend(n)` / `refund(n)` concurrentes.
4. **Mapper statsd_exporter et format gostats.** Ordre d'évaluation globs puis regex, échappement, types en conflit, framing TCP de gostats (hypothèse non vérifiée ici : compteurs envoyés en delta à chaque flush, reconnexion), tags DogStatsD. Dé-risquer : test différentiel contre `statsd_exporter` v0.26.1 et contre un collecteur statsd factice enregistrant le flux brut de la référence.
5. **Client xDS SoTW et déploiement Envoy Gateway.** ADS sur le type `type.googleapis.com/ratelimit.config.ratelimit.v3.RateLimitConfig`, ACK/NACK avec version et nonce, backoff, `Node` id/metadata, TLS mutuel avec les certificats EG, démarrage bloquant sans config si `FORCE_START…=false`. Dé-risquer : réutiliser `examples/xds-sotw-config-server` d'amont comme serveur de test, puis un test `kind` avec EG réel qui ne fait **que** remplacer l'image.

Ordre de construction recommandé : protos + serveur gRPC + chargeur YAML + Redis legacy + harnais différentiel → cache local, `STOP_CACHE_KEY…`, en-têtes, métadonnées, groupes quota → sinks de stats → xDS + image + test EG → Memcached → nouveaux types (`fixed` avec `offset`, `anchored`, `token_bucket`, `sliding`), chacun derrière la liste `rate_limits` uniquement.

---

## 8. Décisions rouvertes — arbitrage

Les cinq décisions proposées après la revue ont chacune été soumises à un relecteur adversarial indépendant (sources : mêmes dépôts et versions qu'en en-tête). Verdicts : **cinq AMENDER**, aucun REJETER, aucun GARDER tel quel. L'arbitrage ci-dessous retient l'esprit de chaque décision et en corrige la formulation.

### R1 — `quota_mode` avec plusieurs limites

**Décision proposée :** un descripteur est épuisé si au moins une de ses limites l'est ; son statut reflète la limite la plus restrictive ; `passedBackends` reste émis par descripteur.

**Arbitrage : AMENDER.** Le principe « OU des limites » est juste et donne une réduction identité pour `rate_limit:` legacy. Trois corrections :

1. **`passedBackends` n'est pas par descripteur** mais par **groupe** : dédupliqué par `quotaGroupKey` et filtré sur les groupes non épuisés (`ratelimit.go:343-355, 485-518`). Reprendre la règle amont telle quelle.
2. **« Plus restrictif » doit être un ordre total**, sinon `Retry-After` est faux. Contre-exemple : 5h épuisée (reset 2 h) et 7d épuisée (reset 3 j), toutes deux à `limit_remaining = 0` ; prendre la première donne `Retry-After: 7200`, le client revient dans 2 h et reste refusé 3 jours. Règle retenue : (a) s'il existe des statuts `OVER_LIMIT`, le représentant est celui au **plus grand `duration_until_reset`** ; sinon (b) celui au **plus petit `limit_remaining`** ; égalité → premier dans l'ordre de `rate_limits`. `current_limit`, `limit_remaining` et `duration_until_reset` viennent tous de ce seul statut.
3. **Ordre des opérations** : la réduction se fait dans la couche cache ; le `shadow_mode` du descripteur s'applique **après** la réduction et ne change que `Code` (`base_limiter.go:159-165`) ; le service reste identique à `ratelimit.go:233-355`. Un descripteur dont toutes les limites sont remplacées renvoie `{OK, current_limit: nil}`, **sort du groupe quota** sauf `QUOTA_MODE` global (`:267`), mais figure dans `passedBackends` s'il porte `backend_name`/`model_name_override` (`:485-518`).

Conséquence pour D13 : les en-têtes par unité (`RateLimit-Limit-5Hours`) se calculent sur l'**expansion par limite**, pas sur les statuts réduits, car `statuses[i]` doit rester 1:1 avec les descripteurs (Envoy `ratelimit_headers.cc:21-33`). Conséquence pour D14 : écrire « un descripteur `strict` dont la liste est épuisée n'est pas compté » plutôt que « une requête refusée n'est pas comptée ».

### R2 — Dette du `token_bucket`

**Décision proposée :** `max_debt` par défaut = `burst`, troncature au-delà comptée dans `debt_capped`, `max_debt: unlimited` disponible.

**Arbitrage : AMENDER.** Le plafond « un seau » est retenu : il reproduit le pardon de la référence `fixed` (un compteur à 300 k sur une limite de 100 k est remis à zéro à la fenêtre suivante) et borne le verrouillage à `duration`. Invariant à documenter : sur-admission × verrouillage = `X / r` ; aucune valeur de plafond n'est gratuite. Corrections :

1. **Unité** : accepter `max_debt` en jetons **ou en durée** (`max_debt: 10m` ⇒ `r × 600` jetons). L'opérateur raisonne en « jamais bloqué plus de X » et la forme durée survit à un changement de `requests_per_unit`. `0` et `unlimited` conservés.
2. **Stat** : `total_capped_hits` (nombre de **jetons** tronqués, miroir de `total_negative_hits`), pas `debt_capped`. Les suffixes amont sont des groupes nominaux incrémentés de `hitsAddend` (`base_limiter.go:74-76, 119-120, 155, 215-217`). L'ajouter au mapper par défaut livré (formes glob et regex), car un suffixe non mappé fuit en une métrique par descripteur.
3. **Adéquation** : avertissement au chargement quand `burst / r ≥ 24 h` : « `token_bucket` ne convient pas à un budget journalier ou hebdomadaire, utiliser `fixed` + `offset` ». Avec 1 M jetons / 7 j et une réponse de 3 M, le plafond laisse encore 7 jours de blocage pour obtenir **un** jeton, là où `fixed` rend un budget plein au reset.
4. **Doc `strict`** : avec une expression `cost` agentgateway, un amendement supérieur aux jetons restants est refusé **et le seau n'est pas touché** ; la réponse est gratuite. `strict` + `cost` ne limite donc presque rien ; à écrire noir sur blanc dans D14.

### R3 — Ouverture de la fenêtre `anchored`

**Décision proposée :** `open_on_check: true` par défaut ; en mode `envoy`, une requête comptée ouvre la fenêtre même refusée.

**Arbitrage : AMENDER.** Le mécanisme est le bon (le proto n'a aucun horodatage, `rls.proto:29-44, 211-242`, donc l'ancre ne peut venir que du check). Corrections :

1. **Énumération plutôt que booléen** : `open_on: check | first_hit`, défaut `check`. `open_on_check: false` ne dit pas ce qui ouvre.
2. **La seconde phrase est redondante** quand `open_on: check` (le check ouvre déjà) et ne décrit que `first_hit`. Formulation retenue : « `open_on: check` : l'ancre est posée par la première requête **présentée** alors qu'aucune fenêtre n'est ouverte, quels que soient `hits_addend` (0 inclus) et le verdict, **sauf en `strict`, où une requête refusée ne modifie aucun état, ancre comprise** (correction R8). `open_on: first_hit` : l'ancre est posée par la première requête dont les hits sont **effectivement comptés** selon `overflow` (en `envoy`, une requête refusée de taille > limite ouvre donc la fenêtre, comme `fixed` ; en `strict`, non). Une ancre posée n'est jamais déplacée ni prolongée ; elle est remplacée à la première requête présentée après `ancre + durée`. » Ouvrir une fenêtre inexistante n'est pas déplacer une fenêtre ouverte : pas de contradiction avec D9.
3. **Amendement tardif** (stream commencé à 4 h 55, fini à 5 h 05) : requête ordinaire sur fenêtre fermée, il **ouvre la fenêtre suivante et y est compté**. C'est exactement ccusage (`blocks.rs:71-76, 89-90`). Même règle pour l'amendement envoyé en `failOpen` sans check préalable (`remoteratelimit.rs:371-373`).
4. **Réponse d'un check ouvrant** : `OK`, `limit_remaining = limite − hits comptés`, `duration_until_reset = ceil(ancre + durée − now)`. Avec `start_rounding: 1h` c'est **moins** que la durée complète (ex. 4 h 01) ; remplacer « reset dans la durée complète » dans D9.
5. **Effets de bord à documenter** : une requête qui échoue en amont (400, 401, 529) ou qu'un prompt-guard de réponse rejette ouvre quand même la session (agentgateway amende 0 token, `llm/mod.rs:2636-2643, 2699-2702`) et rien ne peut la fermer (amendements négatifs abandonnés). Ce n'est pas nouveau : avec la tokenisation active, le D9 d'origine ouvrait déjà sur `input_tokens > 0`. Le prompt-guard **requête** n'atteint jamais le RLS (`httpproxy.rs:2952-3002`). Compteur `opened_without_hits` pour les fenêtres ouvertes à 0 hit et expirées sans hit.
6. **Script** : ancre et compteur dans la même clé, lecture-décision-écriture en un seul script → une seule ancre gagne sans `SET NX` ; borner `elapsed` à 0 (réplica en retard) ; décider la fermeture par `now ≥ ancre + durée` dans le script, pas par l'absence de clé ; TTL = durée + marge.

### R4 — Remplacer `recurring`

**Décision proposée :** supprimer `recurring` ; ajouter à `fixed` `offset: <durée>` et `offset: from_first_hit`.

**Arbitrage : AMENDER, et REJETER `from_first_hit`.** Le concept « fenêtre fixe dont l'origine est choisie » est retenu, ainsi que la suppression de `recurring`. Mais :

1. **`from_first_hit` contredit littéralement la source** : « Your reset day and time stay the same **regardless of when you start using Claude** or when your subscription begins ». Une phase dérivée du premier usage ne reproduit pas Claude ; elle reproduit `recurring` (φ = A mod D ⇒ fenêtres `[A+nD, A+(n+1)D)`), avec la même clé de phase, le même TTL (client inactif → nouvelle phase, ou clé éternelle en violation de D25), les mêmes 3 allers-retours sur Memcached, et un cas limite neuf pour les remboursements sur phase expirée. Rien n'est plus simple, seule la case YAML change.
2. **Alternative retenue : `offset: hashed`.** φ = `fnv1a64(contenu de la partie {…}, après substitution share_threshold) mod D`, arrondi à `offset_rounding` (défaut `1h`). Zéro Lua, zéro clé, zéro TTL, stable pour toujours, identique sur toutes les réplicas, survit à un `FLUSHALL`, fonctionne sur Memcached, fidèle à « assigned to your account ». Répartit les resets uniformément (évite la bousculade du lundi 00:00). Limite : on ne choisit pas l'heure de reset d'un compte donné. L'algorithme de hachage est figé dans la spec (tout changement = remise à zéro).
3. **`offset: <durée>` est UTC seulement.** Epoch est un jeudi ; lundi 00:00 UTC ⇒ `offset: 4d`. « Heure locale » n'est pas tenable à offset constant : lundi 00:00 Europe/Paris = dimanche 22:00 UTC en été, 23:00 UTC en hiver, et la semaine locale dure 167 h ou 169 h une fois l'an. La référence elle-même ne fait du calendaire qu'en UTC (`time.go:27-29`). Un `timezone:` IANA reste reporté, explicitement.
4. **Règles** : `0 ≤ offset < durée`, sinon erreur de config (pas de normalisation silencieuse) ; `offset` interdit avec `unit: month` quand `USE_CALENDAR_MONTH_RATE_LIMIT=true` (la durée est variable, `mod` n'a pas de sens) ; reset = `D − (now − φ).rem_euclid(D)` ; sur 5 h, aucun offset n'aligne sur la journée (24 n'est pas multiple de 5), à dire dans la doc.
5. **Identité (D26)** : `offset` entre dans la forme lisible (`fx604800o345600`, `fx604800oh`). `fixed 7d offset 0` et `fixed 7d offset 4d` sur un même descripteur sont deux limites distinctes.

```yaml
rate_limits:
  - name: weekly
    requests_per_unit: 1000000
    window: { type: fixed, duration: 7d, offset: hashed, offset_rounding: 1h }
  - name: monday_utc
    requests_per_unit: 1000000
    window: { type: fixed, duration: 7d, offset: 4d }   # défaut : 0 (epoch)
```

### R5 — Le nom dans la clé Redis

**Décision proposée :** le nom entre toujours dans la clé des limites de `rate_limits:` et devient obligatoire ; renommer remet à zéro.

**Arbitrage : AMENDER.** Le fond tient et supprime le piège D26. Trois précisions sans lesquelles la règle casse les groupes de remplacement, le mapper ou le découpage de la clé :

1. **Unicité : dans la liste d'un descripteur, et rien de plus.** L'amont n'impose aucune unicité (`config_impl.go:298-305`) et un même nom sur plusieurs descripteurs forme un **groupe de remplacement** (`ratelimit.go:170-189`, README « Replaces ») ; la clé et la métrique contiennent déjà le chemin du descripteur, donc pas de collision. Règles : deux limites de même nom dans une liste = erreur ; même nom dans deux descripteurs = légal ; une limite ne peut pas `replaces` un nom de sa propre liste ; `rate_limit:` legacy inchangé (nom optionnel, hors clé) ; pas de regex sur `replaces` (il peut viser un nom legacy quelconque). Risque documenté : toute limite de liste devient remplaçable, un nom banal (`default`) réutilisé partout + un `replaces: default` forme un groupe involontaire.
2. **La forme lisible reste nécessaire, le nom va en dernier.** Sans la forme, `fixed 7d → fixed 5h` garderait un compteur à TTL 7 j, et `fixed`/`token_bucket`/`sliding` stockent des types Redis différents (`WRONGTYPE`). Suffixe : `{…}_<forme>[_<bucketStart>]_<nom>`, forme ∈ `[a-z]{2}[a-z0-9]*` (type, durée, arrondi, mode, `buckets`, `offset` ; **pas** `requests_per_unit` ni `burst`), `bucketStart` pour `fixed` seulement, nom en dernier car il contient légitimement `_`. Memcached accepte `{ } ~ %` (`legalKey` : refus si > 250 o ou octet ≤ `' '` ou `0x7f`) ; borner le nom à 64 o et avertir au chargement si la partie statique dépasse 250 − 64.
3. **Jeu de caractères du nom** (`rate_limits:` uniquement) : `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`. Interdits parce que gostats écrit `name:value|c` sans échappement et `SanitizeStatName` ne traite que `:`, `|` et les points d'IPv4 (`utilities.go:80-88`) : `:` `|` espace ; `.` (séparateur de hiérarchie, jamais sanitisé pour un nom, F24) ; `~` (séparateur D29) ; `*` et `=`. Le suffixe métrique `~nom` est émis **même pour une liste à une seule limite**, sinon ajouter une 2e limite renommerait la série (le piège D26 déplacé vers les métriques).
4. **Pas d'auto-nom** (`fixed-7d`) : la métrique changerait à chaque réglage de durée, deux limites de même forme collisionneraient, et `replaces: fixed-7d` référencerait une chaîne implicite.
5. **Migration** : aucun reset supplémentaire si la grammaire du suffixe est figée dans un ADR avant la première release ; tout changement ultérieur = version majeure avec reset documenté.

### Points transverses issus de l'arbitrage

- Nouvelles stats à ajouter au mapper par défaut livré : `total_capped_hits` (R2), `opened_without_hits` (R3). Noter la divergence dans `docs/parity.md`.
- D26 (identité) devient : type + durée + arrondi + mode + `buckets` + `offset` + **nom**.
- D9 : supprimer « répond quota plein, reset dans la durée complète » ; remplacer par la réponse exacte de R3.4.
- D14 : reformuler `strict` (R1 conséquence, R2.4).
- D12 : pour un `fixed` à offset, le reset n'est plus `sec − now % sec` (R4.4).
- À écrire ensuite : un ADR « format des clés Redis des nouveaux types » (R5.5) et un ADR « origine des fenêtres fixes » (R4).

### R6 — Points résiduels sur `offset: hashed` (complément de R4)

**Problème signalé :** la phase dépendait de `CACHE_KEY_PREFIX` et du domaine ; changer le préfixe ou renommer le domaine déplace le jour de reset de tous les comptes. L'ADR doit aussi figer si le hachage porte sur la forme brute ou échappée, et `offset_rounding` doit diviser `duration`.

**Arbitrage :**

1. **Entrée du hachage : les entrées du descripteur seulement, ni préfixe ni domaine.** La doc Anthropic dit « assigned to your account » : la phase appartient à l'identité du compte, pas au déploiement. Entrée = la suite `clé=valeur` des entrées **après substitution `share_threshold`** (un motif partagé partage sa phase, comme il partage son compteur), sur la **forme brute** (avant l'échappement `%7B`/`%7D` de D25), jointe par `\0`. Conséquences : renommer le domaine, changer le préfixe ou migrer de `rate_limit:` vers `rate_limits:` ne déplace pas le jour de reset ; un même `user_id` dans deux domaines a le même reset, ce qui est le comportement d'un « compte ». Le hachage ignore le domaine mais la **clé** le contient toujours : les compteurs restent séparés par domaine.
2. **Algorithme figé** : FNV-1a 64 bits sur les octets UTF-8 de l'entrée ci-dessus ; φ = `(h mod (D / R)) × R` où R = `offset_rounding`. Changer l'algorithme ou l'entrée est une rupture majeure.
3. **`offset_rounding` doit diviser `duration`**, sinon erreur de config. Défaut : la plus grande valeur parmi `1h`, `1m`, `1s` qui divise `duration` (5h → 1h ; 90s → 1s ; 7d → 1h). `offset_rounding` > `duration` ou 0 = erreur.
4. **Lien avec l'offset constant** : `offset: <durée>` est lui aussi indépendant du préfixe et du domaine par construction. Les deux formes partagent la même règle `0 ≤ φ < D`.

À reporter dans l'ADR « origine des fenêtres fixes » (R4) : entrée du hachage, algorithme, règle d'arrondi, et la liste de ce qui déplace ou ne déplace pas la phase.

### R7 — Ouverture d'une `anchored` conditionnée aux autres limites (complément de R3)

**Décision proposée :** avec `open_on: check`, une limite `anchored` ne s'ouvre que si les autres limites du même descripteur ne rejettent pas la requête, pour éviter qu'un utilisateur au quota hebdomadaire épuisé ouvre des sessions de 5 h vides.

**Arbitrage : REJETER.** Garder R3 tel quel, documenter la limite, orienter vers `open_on: first_hit`. Quatre raisons :

1. **La règle ne couvre pas le cas visé.** Avec agentgateway, le quota partagé (org, modèle) est typiquement sur un **autre descripteur** (c'est la raison d'être des groupes `quota_mode`, `ratelimit.go:232-238`) ; D15 rend les descripteurs indépendants. Hebdo org épuisée sur `{org: acme}`, check à 0 hit : `{user: alice}` a toutes ses limites OK, la 5 h s'ouvre quand même. La corriger demanderait deux phases (dry-run puis commit) sur des hash tags différents : un aller-retour de plus sur toute requête portant une `anchored`, pas d'atomicité, verdict périmé par construction, et la reproduction de la verrue `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` (F18). Le proto n'a pas de message « commit ».
2. **Elle contredit `envoy` dès que `hits_addend > 0`.** D14 impose de compter une requête refusée ; sur fenêtre fermée, compter ⇒ ouvrir. « Ouvre seulement si les voisines ne refusent pas » force l'abandon des hits. Cas réels : tokenisation active (le check porte `input_tokens`, `remoteratelimit.rs:263-265`), amendement en `failOpen` sans check (`:371-373`), amendement tardif après `ancre + durée` pendant que l'hebdo s'est épuisée. La règle se réduit donc à une seule cellule : `envoy` × `hits = 0` × liste `OVER_LIMIT` × fenêtre fermée, uniquement pour la configuration `cost`.
3. **La prémisse est inversée.** La fenêtre fantôme a un budget **plein** (0 hit compté). Au reset hebdomadaire, l'utilisateur dispose de B pendant le résiduel (2 h 30 en espérance sous martèlement uniforme) puis de B à nouveau : il a **plus** de budget par unité de temps, pas moins. Le seul perdant est l'opérateur, pour au plus un budget B supplémentaire, une fois par reset hebdomadaire, par utilisateur ayant martelé pendant l'épuisement. Côté client, la fenêtre fantôme est invisible : `Retry-After` et `x-ratelimit-*` viennent de l'hebdo (`:598-616, :641-650`). Le compteur `opened_without_hits` (R3.5) mesure exactement ces fenêtres, puisqu'aucun amendement ne suit un refus (`:370`).
4. **Le couplage coûte plus qu'il ne rapporte.** L'état d'une `anchored` dépendrait des verdicts de ses voisines, avec un comportement différent selon que l'hebdo est dans la liste ou dans un autre descripteur. `open_on: first_hit` répond déjà au besoin sans couplage : le check à 0 hit n'ouvre jamais, l'amendement ouvre en fin de réponse (dérive bornée, pas de fantôme) ; sous tokenisation il donne le même résultat que la règle proposée.

**Texte à ajouter à D9/R3 :** « Avec `open_on: check`, une requête refusée par une autre limite, de la même liste ou d'un autre descripteur, ouvre quand même la fenêtre ; ces fenêtres sont comptées dans `opened_without_hits`. Si c'est indésirable, utiliser `open_on: first_hit`. Le comportement réel de Claude dans ce cas n'est pas documenté. »

**Deux points connexes fixés au passage :**
- **Shadow dans le script** : le verdict utilisé par toute règle d'ouverture ou de comptage est le verdict **post-shadow**, et le shadow **global** (`SHADOW_MODE`, appliqué au service dans la référence `ratelimit.go:332-335`) doit être transmis au script comme le shadow du descripteur (précision de D17).
- **`strict` × shadow** (non spécifié jusqu'ici) : le verdict effectif étant `OK`, la liste est comptée. Cohérent avec la référence, qui compte puis masque (`base_limiter.go:159-165`), et avec le sens du shadow (« se comporter comme si c'était autorisé »).

**Matrice de tests à reprendre dans la spec** (fenêtre fermée, liste `[5h anchored, 7d fixed]`, verdict liste × hits × mode × shadow → ouvre ? / compte ?) : hebdo OK / 0 / tous → oui / 0 ; hebdo OK / >0 / `envoy` → oui / oui ; 5h auto-refus (hits > limite) / >0 / `strict` → non / non ; hebdo OVER / 0 / `envoy` → **oui / 0** (différence avec la règle rejetée) ; hebdo OVER / 0 / `strict` → **non / 0** (R8) ; hebdo OVER / >0 / `envoy` → oui / oui ; hebdo OVER / >0 / `strict` → non / non ; hebdo OVER shadowée / >0 / `strict` → oui / oui ; hebdo remplacée par `replaces` → comme hebdo OK ; org OVER sur un autre descripteur / 0 → oui / 0 et `opened_without_hits` +1 ; fenêtre déjà ouverte → ancre jamais déplacée, comptage selon `overflow`.

### R8 — Correction de la matrice R7 (`strict` et ouverture)

**Problème :** la matrice de R7 faisait ouvrir la fenêtre par « hebdo OVER / 0 / `strict` » mais pas par « hebdo OVER / >0 / `strict` » ni « 5h auto-refus / >0 / `strict` », alors que R3.2 disait que `open_on: check` ouvre « quel que soit le verdict ». En `strict`, un check à 0 hit est lui aussi refusé quand une limite de la liste est épuisée (règle `>=`), donc les trois lignes décrivent le même cas.

**Arbitrage :** en `strict`, **une requête refusée ne modifie aucun état, ancre comprise** ; c'est le sens du tout-ou-rien. R3.2 et la ligne « hebdo OVER / 0 / `strict` » (désormais **non / 0**) sont corrigés en conséquence. Les fenêtres ouvertes sans hit (`opened_without_hits`) ne concernent donc que le mode `envoy`, ou un check accepté. Le shadow reste évalué avant : une limite shadowée ne rend pas la requête refusée (ligne « hebdo OVER shadowée / >0 / `strict` → oui / oui » inchangée).
