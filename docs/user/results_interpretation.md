# Interpreting results

## Build summary

Dans l'UI build:

- `Links`: nombre final de liens.
- variantes (`with_link_code`, `without_link_code`).
- stats sources linking (si disponibles).

## Fichiers importants

- `ent_links`: couples d'entites liees.
- `attr_triples_1` / `attr_triples_2`: attributs cibles/source.
- `rel_triples_1` / `rel_triples_2`: relations internes.
- `BUILD_STATS.json`: metriques globales.
- `BUILD_CONFIG.json`: config exacte du run.

## SAKEY Explorer

- `support`: nb de sujets couverts par la combinaison.
- `coverage`: proportion de sujets couverts (samplee si gros volume).
- `type=almost_key`: candidat potentiellement discriminant.
- `type=non_key`: combinaison non discriminante.

## Signaux d'alerte

- Liens tres faibles vs historique classe.
- Absence complete `attr_*`/`rel_*`.
- Variantes incoherentes pour meme config.
