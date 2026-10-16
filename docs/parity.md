# Inventaire de parité avec envoy ratelimit

Référence : `envoyproxy/ratelimit` au commit `f4e7669` (2026-10-06). Règle (D3) : la première version ne sort que lorsque **chaque ligne** est `✅` avec un test qui la prouve, ou `≠` (divergence volontaire, justifiée). Les divergences volontaires sont listées en fin de document.

Légende des états : `⬜` à faire · `🚧` en cours · `✅` fait et prouvé · `≠` divergence volontaire · `—` ignoré comme en amont.

La section 1 est **générée** depuis `src/settings/settings.go` (134 variables) ; la régénérer à chaque montée de version de la référence.

## 1. Variables d'environnement

| Variable | Défaut amont | État | Test |
|---|---|---|---|
| `HOST` | `0.0.0.0` | ⬜ | |
| `PORT` | `8080` | ⬜ | |
| `DEBUG_HOST` | `0.0.0.0` | ⬜ | |
| `DEBUG_PORT` | `6070` | ⬜ | |
| `GRPC_UDS` | — | ⬜ | |
| `GRPC_HOST` | `0.0.0.0` | ⬜ | |
| `GRPC_PORT` | `8081` | ⬜ | |
| `GRPC_MAX_CONNECTION_AGE` | `24h` | ⬜ | |
| `GRPC_MAX_CONNECTION_AGE_GRACE` | `1h` | ⬜ | |
| `GRPC_SERVER_USE_TLS` | `false` | ⬜ | |
| `GRPC_SERVER_TLS_CERT` | — | ⬜ | |
| `GRPC_SERVER_TLS_KEY` | — | ⬜ | |
| `GRPC_CLIENT_TLS_CACERT` | — | ⬜ | |
| `GRPC_CLIENT_TLS_SAN` | — | ⬜ | |
| `LOG_LEVEL` | `WARN` | ⬜ | |
| `LOG_FORMAT` | `text` | ⬜ | |
| `CONFIG_TYPE` | `FILE` | ⬜ | |
| `FORCE_START_WITHOUT_INITIAL_CONFIG` | `false` | ⬜ | |
| `CONFIG_GRPC_XDS_NODE_ID` | `default` | ⬜ | |
| `CONFIG_GRPC_XDS_NODE_METADATA` | — | ⬜ | |
| `CONFIG_GRPC_XDS_SERVER_URL` | `localhost:18000` | ⬜ | |
| `CONFIG_GRPC_XDS_SERVER_CONNECT_RETRY_INTERVAL` | `3s` | ⬜ | |
| `CONFIG_GRPC_XDS_CLIENT_ADDITIONAL_HEADERS` | — | ⬜ | |
| `CONFIG_GRPC_XDS_SERVER_USE_TLS` | `false` | ⬜ | |
| `CONFIG_GRPC_XDS_CLIENT_TLS_CERT` | — | ⬜ | |
| `CONFIG_GRPC_XDS_CLIENT_TLS_KEY` | — | ⬜ | |
| `CONFIG_GRPC_XDS_SERVER_TLS_CACERT` | — | ⬜ | |
| `CONFIG_GRPC_XDS_SERVER_TLS_SAN` | — | ⬜ | |
| `XDS_CLIENT_BACKOFF_INITIAL_INTERVAL` | `10s` | ⬜ | |
| `XDS_CLIENT_BACKOFF_MAX_INTERVAL` | `60s` | ⬜ | |
| `XDS_CLIENT_BACKOFF_RANDOM_FACTOR` | `0.5` | ⬜ | |
| `XDS_CLIENT_BACKOFF_JITTER` | `true` | ⬜ | |
| `XDS_CLIENT_MAX_MSG_SIZE_IN_BYTES` | — | ⬜ | |
| `USE_DOG_STATSD` | `false` | ⬜ | |
| `USE_DOG_STATSD_MOGRIFIERS` | — | ⬜ | |
| `USE_STATSD` | `true` | ⬜ | |
| `STATSD_HOST` | `localhost` | ⬜ | |
| `STATSD_PORT` | `8125` | ⬜ | |
| `EXTRA_TAGS` | — | ⬜ | |
| `STATS_FLUSH_INTERVAL` | `10s` | ⬜ | |
| `DISABLE_STATS` | `false` | ⬜ | |
| `SANITIZE_DESCRIPTOR_METRIC_DOTS` | `false` | ⬜ | |
| `USE_PROMETHEUS` | `false` | ⬜ | |
| `PROMETHEUS_ADDR` | `:9090` | ⬜ | |
| `PROMETHEUS_PATH` | `/metrics` | ⬜ | |
| `PROMETHEUS_MAPPER_YAML` | — | ⬜ | |
| `PROMETHEUS_RESPONSE_TIME_AS_MILLISECONDS` | `false` | ⬜ | |
| `RUNTIME_ROOT` | `/srv/runtime_data/current` | ⬜ | |
| `RUNTIME_SUBDIRECTORY` | — | ⬜ | |
| `RUNTIME_APPDIRECTORY` | `config` | ⬜ | |
| `RUNTIME_IGNOREDOTFILES` | `false` | ⬜ | |
| `RUNTIME_WATCH_ROOT` | `true` | ⬜ | |
| `EXPIRATION_JITTER_MAX_SECONDS` | `300` | ⬜ | |
| `LOCAL_CACHE_SIZE_IN_BYTES` | `0` | ⬜ | |
| `NEAR_LIMIT_RATIO` | `0.8` | ⬜ | |
| `CACHE_KEY_PREFIX` | — | ⬜ | |
| `BACKEND_TYPE` | `redis` | ⬜ | |
| `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` | `false` | ⬜ | |
| `USE_CALENDAR_MONTH_RATE_LIMIT` | `false` | ⬜ | |
| `LIMIT_RESPONSE_HEADERS_ENABLED` | `false` | ⬜ | |
| `LIMIT_LIMIT_HEADER` | `RateLimit-Limit` | ⬜ | |
| `LIMIT_REMAINING_HEADER` | `RateLimit-Remaining` | ⬜ | |
| `LIMIT_RESET_HEADER` | `RateLimit-Reset` | ⬜ | |
| `LIMIT_PER_UNIT_HEADERS_ENABLED` | `false` | ⬜ | |
| `LIMIT_REQUEST_HEADERS_ENABLED` | `false` | ⬜ | |
| `LIMIT_REQUEST_LIMIT_HEADER` | `RateLimit-Limit` | ⬜ | |
| `LIMIT_REQUEST_REMAINING_HEADER` | `RateLimit-Remaining` | ⬜ | |
| `LIMIT_REQUEST_RESET_HEADER` | `RateLimit-Reset` | ⬜ | |
| `HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED` | `false` | ⬜ | |
| `REDIS_SOCKET_TYPE` | `unix` | ⬜ | |
| `REDIS_TYPE` | `SINGLE` | ⬜ | |
| `REDIS_URL` | `/var/run/nutcracker/ratelimit.sock` | ⬜ | |
| `REDIS_POOL_SIZE` | `10` | ⬜ | |
| `REDIS_AUTH` | — | ⬜ | |
| `REDIS_AUTH_FILE` | — | ⬜ | |
| `REDIS_AWS_IAM_AUTH` | `false` | ⬜ | |
| `REDIS_AWS_IAM_CACHE_NAME` | — | ⬜ | |
| `REDIS_AWS_IAM_USER_ID` | — | ⬜ | |
| `REDIS_AWS_IAM_SERVERLESS` | `false` | ⬜ | |
| `REDIS_AWS_IAM_REGION` | — | ⬜ | |
| `REDIS_TLS` | `false` | ⬜ | |
| `REDIS_TLS_CLIENT_CERT` | — | ⬜ | |
| `REDIS_TLS_CLIENT_KEY` | — | ⬜ | |
| `REDIS_TLS_CACERT` | — | ⬜ | |
| `REDIS_TLS_SKIP_HOSTNAME_VERIFICATION` | `false` | ⬜ | |
| `REDIS_PIPELINE_WINDOW` | `0` | ⬜ | |
| `REDIS_PIPELINE_LIMIT` | `0` | ⬜ | |
| `REDIS_CLUSTER_PIPELINE_PARALLELISM` | `1` | ⬜ | |
| `REDIS_PERSECOND` | `false` | ⬜ | |
| `REDIS_PERSECOND_SOCKET_TYPE` | `unix` | ⬜ | |
| `REDIS_PERSECOND_TYPE` | `SINGLE` | ⬜ | |
| `REDIS_PERSECOND_URL` | `/var/run/nutcracker/ratelimitpersecond.sock` | ⬜ | |
| `REDIS_PERSECOND_POOL_SIZE` | `10` | ⬜ | |
| `REDIS_PERSECOND_AUTH` | — | ⬜ | |
| `REDIS_PERSECOND_AUTH_FILE` | — | ⬜ | |
| `REDIS_PERSECOND_AWS_IAM_AUTH` | `false` | ⬜ | |
| `REDIS_PERSECOND_AWS_IAM_CACHE_NAME` | — | ⬜ | |
| `REDIS_PERSECOND_AWS_IAM_USER_ID` | — | ⬜ | |
| `REDIS_PERSECOND_AWS_IAM_SERVERLESS` | `false` | ⬜ | |
| `REDIS_PERSECOND_AWS_IAM_REGION` | — | ⬜ | |
| `REDIS_PERSECOND_TLS` | `false` | ⬜ | |
| `REDIS_SENTINEL_AUTH` | — | ⬜ | |
| `REDIS_PERSECOND_SENTINEL_AUTH` | — | ⬜ | |
| `REDIS_PERSECOND_PIPELINE_WINDOW` | `0` | ⬜ | |
| `REDIS_PERSECOND_PIPELINE_LIMIT` | `0` | ⬜ | |
| `REDIS_PERSECOND_CLUSTER_PIPELINE_PARALLELISM` | `1` | ⬜ | |
| `REDIS_HEALTH_CHECK_ACTIVE_CONNECTION` | `false` | ⬜ | |
| `REDIS_CLOSE_CONNECTION_ON_READONLY_ERROR` | `false` | ⬜ | |
| `REDIS_TIMEOUT` | `10s` | ⬜ | |
| `REDIS_PERSECOND_TIMEOUT` | `10s` | ⬜ | |
| `REDIS_POOL_ON_EMPTY_BEHAVIOR` | `WAIT` | ⬜ | |
| `REDIS_PERSECOND_POOL_ON_EMPTY_BEHAVIOR` | `WAIT` | ⬜ | |
| `REDIS_STARTUP_INITIAL_INTERVAL` | `1s` | ⬜ | |
| `REDIS_STARTUP_MAX_INTERVAL` | `30s` | ⬜ | |
| `REDIS_STARTUP_MAX_ELAPSED_TIME` | `0` | ⬜ | |
| `MEMCACHE_HOST_PORT` | — | ⬜ | |
| `MEMCACHE_MAX_IDLE_CONNS` | `2` | ⬜ | |
| `MEMCACHE_SRV` | — | ⬜ | |
| `MEMCACHE_SRV_REFRESH` | `0` | ⬜ | |
| `MEMCACHE_TLS` | `false` | ⬜ | |
| `MEMCACHE_TLS_CLIENT_CERT` | — | ⬜ | |
| `MEMCACHE_TLS_CLIENT_KEY` | — | ⬜ | |
| `MEMCACHE_TLS_CACERT` | — | ⬜ | |
| `MEMCACHE_TLS_SKIP_HOSTNAME_VERIFICATION` | `false` | ⬜ | |
| `SHADOW_MODE` | `false` | ⬜ | |
| `QUOTA_MODE` | `false` | ⬜ | |
| `RESPONSE_DYNAMIC_METADATA` | `false` | ⬜ | |
| `MERGE_DOMAIN_CONFIG` | `false` | ⬜ | |
| `TRACING_ENABLED` | `false` | ⬜ | |
| `TRACING_SERVICE_NAME` | `RateLimit` | ⬜ | |
| `TRACING_SERVICE_NAMESPACE` | — | ⬜ | |
| `TRACING_SERVICE_INSTANCE_ID` | — | ⬜ | |
| `TRACING_EXPORTER_PROTOCOL` | `http` | ⬜ | |
| `TRACING_SAMPLING_RATE` | `1` | ⬜ | |
| `GRPC_SERVER_TLS_CA_CERT` | (injectée par Envoy Gateway, lue par aucun code amont) | — ignorée, comme en amont | |
| `OTEL_EXPORTER_OTLP_*` | (lues par le SDK OpenTelemetry) | ⬜ | |

## 2. Configuration YAML

| Élément | État | Test |
|---|---|---|
| Clés reconnues : `domain`, `key`, `value`, `descriptors`, `rate_limit`, `unit`, `requests_per_unit`, `unlimited`, `shadow_mode`, `quota_mode`, `name`, `replaces`, `detailed_metric`, `value_to_metric`, `share_threshold`, `metadata` | ⬜ | |
| Clé inconnue ⇒ erreur de config (liste blanche récursive, `metadata` exclu) | ⬜ | |
| Unités `second` … `year` (insensibles à la casse), mois = 30 j, année = 365 j | ⬜ | |
| `unlimited` + `unit` ⇒ erreur ; `requests_per_unit: 0` accepté | ⬜ | |
| `replaces` : nom vide ou auto-remplacement ⇒ erreur ; aucune unicité des noms | ⬜ | |
| Matching : `clé_valeur` exacte, puis wildcard final **et médian**, puis clé seule | ⬜ | |
| `share_threshold` (motif dans la clé Redis et la clé de stats) | ⬜ | |
| `detailed_metric`, `value_to_metric` (clé de stats enrichie) | ⬜ | |
| Domaine dupliqué ⇒ erreur, sauf `MERGE_DOMAIN_CONFIG` ; clé composée dupliquée ⇒ erreur | ⬜ | |
| Rechargement : erreur ⇒ `config_load_error` + ancienne config conservée | ⬜ | |
| Démarrage sans config valide ⇒ erreur gRPC à chaque appel (pas d'arrêt) | ⬜ | |
| Fournisseur fichier : goruntime, `SymlinkRefresher` / `DirectoryRefresher`, scope `runtime.*` | ⬜ | |
| Fournisseur xDS SotW (`RateLimitConfig`, ACK/NACK, backoff, mTLS) | ⬜ | |

## 3. API gRPC et HTTP

| Élément | État | Test |
|---|---|---|
| `envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit` (v2 absente) | ⬜ | |
| `hits_addend` requête (0 ⇒ 1 sauf addend par descripteur) et par descripteur (0 = vérifier) | ⬜ | |
| `is_negative_hits` : plancher 0, toujours `OK`, TTL remis à la durée complète | ⬜ | |
| `limit` envoyée par le client : remplace la config, sans shadow / `replaces` / `unlimited` | ⬜ | |
| Champs de réponse : `overall_code`, `statuses[]`, `current_limit` (avec `name`), `limit_remaining`, `duration_until_reset` | ⬜ | |
| Domaine vide ou liste vide ⇒ erreur ; domaine inconnu ⇒ `OK` + `domain_not_found` | ⬜ | |
| En-têtes `LIMIT_*` réponse, par unité, et requête | ⬜ | |
| Métadonnées dynamiques (`RESPONSE_DYNAMIC_METADATA`) : `domain`, `descriptors`, `hitsAddend`, `metadata` fusionné | ⬜ | |
| `quota_mode` : groupes `backend_name` / `model_name_override`, `OVER_LIMIT` si tous épuisés, `passedBackends` par groupe | ⬜ | |
| `replaces` : deux passes, non transitif | ⬜ | |
| Shadow mode par règle et global (`global_shadow_mode`) | ⬜ | |
| `POST /json` : protojson, 200 / 429 / 400 / 500 | ⬜ | |
| `/healthcheck` : 500 sans corps si KO ; SIGTERM ⇒ unhealthy + arrêt gracieux | ⬜ | |
| `grpc.health.v1`, service `ratelimit` | ⬜ | |
| Port debug : `/`, `/rlconfig` (format `Dump()`), `/stats`, `/debug/pprof/*` | ⬜ | |
| `GRPC_UDS`, âge max des connexions, mTLS client + SAN, rechargement à chaud du certificat serveur | ⬜ | |

## 4. Algorithme et stockage

| Élément | État | Test |
|---|---|---|
| Clé `prefix + domaine_clé_valeur_…_bucketStart`, alignée epoch ; mois calendaire UTC | ⬜ | |
| Redis : pipeline `INCRBY` + `EXPIRE (unité + jitter)` | ⬜ | |
| `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` : pipeline `GET` puis `INCRBY`, voisines à 0, clés fantômes | ⬜ | |
| Cache local des clés over-limit (posé avant le test shadow, TTL = unité) | ⬜ | |
| Seuil `near_limit` en `float32` ; répartition near / over des `hitsAddend` | ⬜ | |
| `REDIS_PERSECOND` (routage des clés `second`) | ⬜ | |
| Redis single / sentinel / cluster, TLS, AUTH (fichier), AWS IAM, retry de démarrage | ⬜ | |
| Memcached : `GET` puis incréments asynchrones, `add` sur absence, SRV, TLS, expiration absolue > 30 j, clé ≤ 250 o | ⬜ | |

## 5. Métriques, logs, traces

| Élément | État | Test |
|---|---|---|
| Par clé : `total_hits`, `over_limit`, `near_limit`, `over_limit_with_local_cache`, `within_limit`, `shadow_mode`, `total_negative_hits` | ⬜ | |
| `domain_not_found`, `global_shadow_mode`, `config_load_success`, `config_load_error` | ⬜ | |
| `call.should_rate_limit.redis_error` / `service_error` | ⬜ | |
| `ratelimit_server.<méthode>.total_requests` / `response_time` | ⬜ | |
| `redis_pool.cx_*`, `localcache.*`, `memcache.*`, `runtime.*` | ⬜ | |
| Sinks exclusifs : statsd **TCP** (défaut), DogStatsD (+ mogrifiers), Prometheus natif + mapper statsd_exporter, log | ⬜ | |
| `SanitizeStatName`, `EXTRA_TAGS`, `STATS_FLUSH_INTERVAL` | ⬜ | |
| `LOG_FORMAT=json` (`@timestamp`, `@message`) | ⬜ | |
| Traces OTLP : spans `ShouldRateLimit Execution`, `Redis Pipeline Execution` et leurs attributs | ⬜ | |

## 6. Image et déploiement

| Élément | État | Test |
|---|---|---|
| Binaire `/bin/ratelimit`, distroless sans shell, amd64 + arm64 | ⬜ | |
| UID quelconque (65534), FS en lecture seule, aucune capability | ⬜ | |
| Ports 8080 / 8081 / 6070, `RUNTIME_ROOT=/srv/runtime_data/current` | ⬜ | |
| Écoute dual-stack (`HOST="::"`), `SO_REUSEPORT` | ⬜ | |
| `/healthcheck` servi après la connexion Redis initiale | ⬜ | |
| Deployment généré par Envoy Gateway (xDS TLS) fonctionne en ne changeant que l'image | ⬜ | |

## 7. Divergences volontaires

| Comportement amont | Notre comportement | Raison |
|---|---|---|
| `limit` client avec `unit: UNKNOWN` ⇒ panique du processus | erreur gRPC `INVALID_ARGUMENT` | D1 : on ne reproduit pas les crashs |
| Variable d'environnement invalide relue au rechargement ⇒ panique | erreur de config | D1 |
| `descriptors[].limit` des métadonnées en prototext Go (instable) | format stable documenté | non reproductible octet pour octet |
| `unit_multiplier` ignoré dans les overrides et absent des réponses | lu, appliqué, renvoyé ; 0 rejeté | extension (proto Envoy à jour) |
| Stats Go (`go.*`), `/stats` expvar, pprof | stats du processus Rust, équivalents documentés | pas d'équivalent en Rust |
| Remboursement sans effet sur le cache local | efface l'entrée locale (nouveaux types uniquement) | D18 |
