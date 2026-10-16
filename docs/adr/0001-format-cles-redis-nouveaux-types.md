# ADR 0001 — Format des clés de stockage des limites `rate_limits:`

- **Statut :** accepté (2026-10-16)
- **Sources :** `docs/design-review.md` D6, D25, D26, R5, Q4 ; référence `envoyproxy/ratelimit@f4e7669` `src/limiter/cache_key.go:66-94`

## Contexte

Les limites legacy (`rate_limit:`) doivent garder la clé amont **octet pour octet** pour que l'ancien et le nouveau service partagent un Redis pendant une migration : `CACHE_KEY_PREFIX + domaine + "_" + (clé "_" valeur "_")* + bucketStart`.

Les limites de la nouvelle liste `rate_limits:` ont d'autres besoins :

- tout-ou-rien (`overflow: strict`) et scripts Lua multi-clés ⇒ toutes les clés d'un descripteur dans **un même slot** Redis Cluster ;
- plusieurs limites par descripteur ⇒ la clé doit identifier la limite ;
- types de fenêtre aux structures Redis différentes (compteur, hash de sous-tranches, TAT) ⇒ changer de type ne doit jamais relire une ancienne valeur (`WRONGTYPE`) ;
- clés lisibles par un opérateur (`redis-cli --scan`).

Ce format est figé avant la première release : tout changement ultérieur est une version majeure avec remise à zéro documentée.

## Décision

### Forme générale

```
{<prefix><domaine>_<clé>_<valeur>_…}_<forme>[_<bucketStart>]_<nom>
```

1. **Hash tag `{…}`** : la partie descripteur, construite comme la clé legacy (préfixe, domaine, paires `clé_valeur_`, substitution du motif `share_threshold` à la valeur), sans `bucketStart`. Elle seule détermine le slot ; toutes les limites et toutes les fenêtres d'un descripteur partagent donc le slot.
2. **Échappement dans `{…}`** : `%` → `%25`, `{` → `%7B`, `}` → `%7D`, appliqué au préfixe, au domaine, aux clés et aux valeurs. Le caractère d'échappement étant lui-même échappé, le découpage est unique : la partie descripteur se termine au premier `}` non échappé. `_` n'est **pas** échappé, comme en amont (la collision legacy `a_b`/`c` ↔ `a`/`b_c` est reproduite, pas corrigée).
3. **`<forme>`** : `[a-z]{2}[a-z0-9]*`, encodage lisible de l'identité de la fenêtre : type, durée en secondes, puis selon le type arrondi, `buckets`, `offset`, mode d'ouverture. Exemples : `fx604800`, `fx604800o345600`, `fx604800oh3600` (offset haché, arrondi 1 h), `an18000r3600`, `sl3600b12`, `tb60`. `requests_per_unit`, `burst` et `max_debt` n'en font **pas** partie : les changer conserve les compteurs.
4. **`<bucketStart>`** : uniquement pour `fixed` (début de fenêtre, offset inclus). Les autres types ont une clé stable dont l'état contient le temps.
5. **`<nom>`** : obligatoire dans `rate_limits:`, toujours en **dernière** position car il peut contenir `_`. Jeu de caractères `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$` (interdits : `:` `|` espace `.` `~` `*` `=`, à cause du format statsd et du séparateur métrique `~`).

### Règles associées

- **Identité d'une limite** = type + durée + arrondi + mode d'ouverture + `buckets` + `offset` + nom. Changer l'un d'eux repart de zéro ; renommer une limite remet donc ses compteurs à zéro (acte explicite).
- **Unicité des noms** : dans la liste d'un descripteur uniquement. Le même nom sur deux descripteurs est légal et forme un groupe de remplacement (`replaces`), comme en amont. Une limite ne peut pas `replaces` un nom de sa propre liste.
- **TTL** : toute clé en a un (durée de la fenêtre + marge), y compris les ancres `anchored`.
- **Memcached** (`fixed` uniquement) : la clé complète doit faire ≤ 250 octets et ne contenir aucun octet ≤ `0x20` ni `0x7f` ; au-delà, comportement amont (erreur journalisée, réponse `OK`). Avertissement au chargement si la partie statique dépasse 250 − 64 octets.
- **`REDIS_PERSECOND`** : une liste qui contient une limite à la seconde est routée **en entier** vers un seul client Redis (divergence documentée : l'amont n'a jamais deux limites par descripteur).

## Conséquences

- Déplacer une limite de `rate_limit:` vers `rate_limits:` remet son compteur à zéro **une fois** (clé différente). Documenté dans le guide de migration.
- Un descripteur très sollicité concentre toutes ses limites sur un nœud du cluster, comme une clé legacy aujourd'hui.
- Les clés restent lisibles et triables par forme et par nom, ce qui facilite le débogage en production.

## Alternatives rejetées

- **Réutiliser la clé legacy pour un `fixed` dans une liste** : conserve les compteurs lors d'une migration, mais la clé legacy n'a pas de hash tag et change de slot à chaque fenêtre ; mélangée à d'autres limites, la liste devient multi-slot et le tout-ou-rien impossible en Cluster.
- **Hacher la forme** : clés opaques, débogage impossible, risque de placer le hachage du mauvais côté du hash tag.
- **Nom optionnel, ajouté seulement pour désambiguïser deux limites de même forme** : ajouter une seconde limite changeait la clé de la première et remettait ses compteurs à zéro sans prévenir.
- **Nom généré automatiquement (`fixed-7d`)** : la clé et la métrique changeraient à chaque réglage de durée, et `replaces` viserait une chaîne implicite.
