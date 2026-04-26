# Tutoriel utilisateur

## 1. Ouvrir l'application

- Aller sur `http://<host>:8501`.
- Verifier que l'etat est OK (jobs/builds visibles).

## 2. Configurer un job

Champs principaux:

- `Class name`: classe WDC (ex: `Airport`, `Book`, `Museum`).
- `Parts spec`: `all` ou sous-ensemble (`0-2`, `0,1,2`).
- `Matching mode`: `property`, `sameAs`, ou combinaisons (OR via mapping rules).
- `Target endpoint`: `wikidata`, `dbpedia`, `yago`.
- `Property mapping rules`: regles de linking.
- `Ignore align cache`: force recalcul alignement.

## 3. Lancer et suivre

- Cliquer `Generate benchmark`.
- Suivre progression dans `Jobs`:
  - `align` puis `build`.
- Consulter les logs en direct depuis le job.

## 4. Lire les resultats

Quand `build` est termine:

- Ouvrir le build dans `History`.
- Verifier les stats (`links`, sources de linking, etc.).
- Telecharger le build si besoin.

Fichiers importants:

- `ent_links`: liens trouves.
- `attr_triples_*`: attributs.
- `rel_triples_*`: relations.
- `BUILD_STATS.json`: statistiques de run.

## 5. SAKEY Explorer (assist)

- Ouvrir `SAKEY Explorer` depuis le dashboard.
- Lancer SAKEY sur une classe.
- Trier/filtrer les cles candidates:
  - ordre par `coverage` ou `support`,
  - filtre `only almost keys`,
  - recherche texte (`iata`, `isbn`, etc.).

Attention: SAKEY est une aide d'exploration, pas une modification automatique du pipeline de build.

## 6. Bonnes pratiques

- Commencer avec une classe/petit `parts_spec` pour valider la config.
- Prioriser Wikidata pour iterer rapidement.
- Relancer sans cache seulement en cas de changement de logique de linking.
