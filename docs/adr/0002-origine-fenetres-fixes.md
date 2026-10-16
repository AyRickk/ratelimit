# ADR 0002 — Origine des fenêtres fixes (`offset`)

- **Statut :** accepté (2026-10-16)
- **Sources :** `docs/design-review.md` D8, D9, Q6, R4, R6 ; [What is the Max plan](https://support.claude.com/en/articles/11049741-what-is-the-max-plan) ; ccusage `rust/crates/ccusage/src/blocks.rs`

## Contexte

La référence aligne toutes les fenêtres fixes sur l'epoch Unix (`bucketStart = (now / D) × D`), ou sur le début du mois UTC avec `USE_CALENDAR_MONTH_RATE_LIMIT`.

Nous voulons reproduire les limites de Claude. La documentation officielle décrit la limite hebdomadaire ainsi : « The weekly limit resets at a fixed time each week that is assigned to your account. Your reset day and time stay the same regardless of when you start using Claude or when your subscription begins ». C'est une **fenêtre fixe dont l'origine dépend du compte**, et non une chaîne de fenêtres ancrées sur le premier usage. La session de 5 h, elle, est une fenêtre `anchored` (voir D9) et n'est pas concernée par cet ADR.

Un premier projet prévoyait un mode `anchored` `recurring` puis un `offset: from_first_hit`. Les deux dérivent la phase du premier usage, ce qui contredit littéralement la source, et ils exigent une clé de phase avec TTL (un client inactif changerait de jour de reset) ou sans TTL (fuite mémoire).

## Décision

`fixed` accepte une origine optionnelle :

```yaml
window: { type: fixed, duration: 7d }                                   # origine = epoch (défaut, comportement amont)
window: { type: fixed, duration: 7d, offset: 4d }                       # lundi 00:00 UTC (l'epoch est un jeudi)
window: { type: fixed, duration: 7d, offset: hashed, offset_rounding: 1h }  # origine propre à chaque « compte »
```

Avec une phase φ, le début de fenêtre vaut `now − (now − φ).rem_euclid(D)` et `duration_until_reset = D − (now − φ).rem_euclid(D)`, arrondi à la seconde supérieure (D12).

### `offset: <durée>`

- Constant, en **UTC uniquement**.
- Règle : `0 ≤ offset < duration`, sinon erreur de config (pas de normalisation silencieuse).
- L'heure locale n'est pas tenable avec un offset constant (changements d'heure : semaine locale de 167 h ou 169 h). Une option `timezone:` IANA est explicitement reportée.

### `offset: hashed`

- **Entrée du hachage** : les entrées du descripteur seulement, sous forme `clé=valeur` jointes par `\0`, **après** substitution du motif `share_threshold`, en forme **brute** (avant l'échappement de l'ADR 0001). Ni préfixe ni domaine.
- **Algorithme figé** : FNV-1a 64 bits sur les octets UTF-8 de cette entrée. φ = `(h mod (D / R)) × R`, avec R = `offset_rounding`.
- **`offset_rounding`** doit diviser `duration`, et `0 < R ≤ D`, sinon erreur de config. Défaut : la plus grande valeur parmi `1h`, `1m`, `1s` qui divise la durée (5h → 1h ; 90s → 1s ; 7d → 1h).
- Aucun état : ni Lua, ni clé, ni TTL. Stable dans le temps, identique sur toutes les réplicas, survit à un `FLUSHALL`, fonctionne sur Memcached.

### Règles communes

- `offset` est interdit avec `unit: month` quand `USE_CALENDAR_MONTH_RATE_LIMIT=true` (durée variable).
- `offset` entre dans la forme de la clé (ADR 0001) : `fixed 7d` et `fixed 7d offset 4d` sur un même descripteur sont deux limites distinctes.
- Sur une durée qui ne divise pas 24 h (ex. 5 h), aucun offset n'aligne les fenêtres sur la journée ; à dire dans la documentation.

## Ce qui déplace (ou non) le jour de reset en `hashed`

| Changement | Phase déplacée ? |
|---|---|
| Renommer le domaine, changer `CACHE_KEY_PREFIX` | non |
| Migrer de `rate_limit:` vers `rate_limits:` | non (mais le compteur repart de zéro, ADR 0001) |
| Même descripteur dans deux domaines | même phase (les compteurs restent séparés) |
| Ajouter, retirer ou renommer une entrée du descripteur | **oui** |
| Changer `duration` ou `offset_rounding` | **oui** |
| Changer l'algorithme ou l'entrée du hachage | **oui, pour tous** : rupture majeure |

## Conséquences

- La limite hebdomadaire de Claude est reproductible sans état supplémentaire. Les resets sont répartis uniformément, ce qui évite la ruée du lundi 00:00.
- On ne peut pas choisir l'heure de reset d'un compte particulier.
- Le mode `recurring` d'`anchored` est supprimé.

## Alternatives rejetées

- **`anchored` en mode `recurring`** et **`offset: from_first_hit`** : la phase dépend du premier usage, contrairement à la source ; il faut une clé de phase (TTL ⇒ un client inactif change de jour de reset ; sans TTL ⇒ fuite) ; trois allers-retours sur Memcached.
- **Hacher la clé complète (préfixe et domaine compris)** : renommer un domaine ou changer de préfixe déplacerait le jour de reset de tous les comptes.
- **Fuseau horaire IANA en v1** : sémantique ambiguë autour des changements d'heure ; reporté.
