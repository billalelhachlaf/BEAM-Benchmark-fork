# Prise En Main BEAM-App

Ce fichier est un support personnel pour comprendre le projet, expliquer le code, et guider des chercheurs qui devront modifier l'application.

Il n'est pas destiné à être versionné dans git.

## 1. Idee Generale

BEAM-App sert à construire des datasets d'alignement d'entités au format BEAM à partir de:

- classes WDC,
- endpoints SPARQL comme Wikidata, DBpedia, YAGO ou un endpoint custom,
- règles de matching configurées depuis une interface web.

L'application a deux processus principaux:

- `webapp`: interface web FastAPI, APIs, dashboard, pages de résultats, téléchargements.
- `worker`: processus de fond qui prend les jobs en file d'attente et lance le pipeline.

Le flux normal est:

1. L'utilisateur configure un job dans l'UI.
2. Le webapp valide le formulaire.
3. Le webapp insère un job dans SQLite (`jobs.db`).
4. Le worker récupère le job.
5. Le pipeline lance l'étape `align`.
6. Le pipeline lance l'étape `build`.
7. Les fichiers finaux sont écrits dans `data/<ClassName>/beam_<timestamp>/`.
8. Le webapp affiche le build terminé dans History.

## 2. Commandes De Base

Démarrer:

```bash
docker compose up -d --build
```

Ouvrir:

```text
http://localhost:8501
```

Vérifier Docker:

```bash
docker compose ps
docker compose logs --tail=200 webapp
docker compose logs --tail=300 worker
```

Lancer les tests:

```bash
pytest
bash scripts/docs_check.sh
```

Smoke test rapide:

```bash
curl -fsS http://127.0.0.1:8501/ >/tmp/beam_home.html
curl -fsS http://127.0.0.1:8501/api/dashboard >/tmp/beam_dashboard.json
```

## 3. Ce Qui Est Source vs Runtime

Code source à modifier:

- `webapp/`
- `worker/`
- `beam/`
- `scripts/`
- `tests/`
- `docs/`
- `catalog/`

Données générées à ne pas committer:

- `data/`
- `Download/`
- `logs/`
- `reports/`
- `jobs.db*`
- `.run/`
- `docker-data/`

Règle importante: ne jamais committer les outputs de jobs, caches, datasets téléchargés, DB locale ou fichiers temporaires.

## 4. Structure Du Code

### Webapp

Point d'entrée:

- `webapp/main.py`

Ce fichier est volontairement court. Il crée l'app FastAPI, charge les constantes, puis charge les modules.

Modules principaux:

- `webapp/modules/forms_and_inputs.py`
  - valeurs par défaut du formulaire,
  - validation,
  - normalisation des paramètres.

- `webapp/modules/routes_pages_sakey_builds.py`
  - pages principales,
  - page tutorial,
  - page SAKey,
  - build detail,
  - link explorer,
  - dashboard API.

- `webapp/modules/routes_jobs_downloads_ws.py`
  - API preflight,
  - création de jobs,
  - cancel/rerun/delete,
  - téléchargements,
  - websocket logs.

- `webapp/modules/jobs_dashboard.py`
  - état dashboard,
  - résumé des jobs,
  - résumé des builds.

- `webapp/modules/builds.py`
  - scan du dossier `data/`,
  - résumé des builds,
  - stats,
  - résolution des artefacts.

- `webapp/modules/preflight.py`
  - diagnostic avant lancement,
  - vérifie qu'une config a des chances de produire des liens.

- `webapp/modules/link_explorer.py`
  - lecture des fichiers `ent_links`,
  - détail d'un lien,
  - payload des noeuds WDC/target.

- `webapp/modules/sakey_core.py`
  - cycle de vie des runs SAKey.

- `webapp/modules/sakey_artifacts.py`
  - artefacts et rapports SAKey.

Templates:

- `webapp/templates/index.html`
  - shell court de la page principale.

- `webapp/templates/partials/index_create.html`
  - wizard de création.

- `webapp/templates/partials/index_dashboard.html`
  - Jobs + History.

- `webapp/templates/partials/index_script_*.html`
  - JavaScript de la page principale, découpé.

- `webapp/templates/link_explorer.html`
  - shell du link explorer.

- `webapp/templates/partials/link_explorer_*`
  - UI, style, script du link explorer.

### Worker

Point d'entrée:

- `worker/run.py`

Modules:

- `worker/run_modules/progress_helpers.py`
  - parsing et formatage de progression.

- `worker/run_modules/runner.py`
  - polling des jobs,
  - transitions d'état,
  - cancellation,
  - lancement align/build.

### Pipeline

Point d'entrée:

- `beam/pipeline.py`

Modules:

- `beam/pipeline_modules/discovery_and_alignment.py`
  - découverte des parts,
  - préparation align,
  - gestion cache align.

- `beam/pipeline_modules/graph_building.py`
  - helpers pour extraction et préparation de graphes.

- `beam/pipeline_modules/orchestrator.py`
  - fonction centrale `generate_benchmark(...)`.

### Alignement

Point d'entrée:

- `scripts/align.py`

Modules:

- `scripts/align_modules/normalization_and_matching.py`
  - normalisation,
  - matching exact/fuzzy,
  - helpers de comparaison.

- `scripts/align_modules/target_endpoint.py`
  - endpoint,
  - prefixes,
  - extraction/format de valeurs target.

- `scripts/align_modules/target_fetching.py`
  - requêtes SPARQL,
  - récupération de candidats.

- `scripts/align_modules/matching_execution.py`
  - exécution du matching.

### Build BEAM

Point d'entrée:

- `scripts/build_beam_files.py`

Modules:

- `scripts/build_beam_files_modules/parsing_and_outputs.py`
  - parsing NQ/NT,
  - split attr/rel,
  - stats,
  - écriture des fichiers BEAM.

- `scripts/build_beam_files_modules/enrichment.py`
  - labels/descriptions.

- `scripts/build_beam_files_modules/cli.py`
  - interface CLI.

### Database

- `beam/db.py`

Contient:

- schéma SQLite,
- jobs,
- subjobs,
- events/logs,
- metadata WDC classes.

## 5. Suivre Un Job Dans Le Code

Pour expliquer le projet, c'est le chemin le plus important.

### 1. Soumission UI

Fichier:

```text
webapp/modules/routes_jobs_downloads_ws.py
```

Fonction:

```text
create_job(...)
```

Elle:

- récupère les champs du formulaire,
- nettoie les valeurs,
- valide via `forms_and_inputs.py`,
- insère le job avec `db.insert_job(...)`.

### 2. Stockage SQLite

Fichier:

```text
beam/db.py
```

Le job est stocké avec:

- paramètres JSON,
- status,
- phase,
- timestamps,
- logs/events.

### 3. Worker

Fichier:

```text
worker/run_modules/runner.py
```

Il:

- poll les jobs `queued`,
- respecte `MAX_CONCURRENT_JOBS`,
- passe le job en `running`,
- lance align puis build,
- écrit les events et progress.

### 4. Pipeline

Fichier:

```text
beam/pipeline_modules/orchestrator.py
```

Fonction clé:

```text
generate_benchmark(...)
```

Elle coordonne:

- sélection des parts WDC,
- alignement,
- vérification du nombre de liens,
- génération BEAM,
- finalisation du job.

### 5. Align

Dossier:

```text
scripts/align_modules/
```

Rôle:

- lire les valeurs WDC,
- construire les candidats côté endpoint,
- lancer les requêtes SPARQL,
- comparer les valeurs,
- produire les liens WDC -> target.

### 6. Build

Dossier:

```text
scripts/build_beam_files_modules/
```

Rôle:

- lire `ent_links`,
- lire les triples,
- séparer attributs et relations,
- produire les fichiers BEAM,
- écrire stats et metadata.

### 7. Affichage

Fichiers:

```text
webapp/modules/builds.py
webapp/modules/jobs_dashboard.py
webapp/modules/routes_pages_sakey_builds.py
```

Ils:

- scannent `data/`,
- préparent les summaries,
- alimentent History,
- alimentent le dashboard,
- servent les downloads.

## 6. Comment Modifier Sans Se Perdre

### Modifier un champ du wizard

Regarder:

- `webapp/templates/partials/index_create.html`
- `webapp/modules/forms_and_inputs.py`
- `webapp/modules/routes_jobs_downloads_ws.py`
- `tests/test_webapp_routes.py`
- `tests/test_webapp_routes_modules/`

Tests:

```bash
pytest tests/test_webapp_routes.py
```

### Modifier la validation

Regarder:

- `webapp/modules/forms_and_inputs.py`

Tests:

```bash
pytest tests/test_presets.py tests/test_webapp_routes.py
```

### Modifier le matching

Regarder:

- `scripts/align_modules/`
- `beam/pipeline_modules/`

Tests:

```bash
pytest tests/test_align.py tests/test_pipeline.py
```

### Modifier le format de sortie BEAM

Regarder:

- `scripts/build_beam_files_modules/`

Tests:

```bash
pytest tests/test_build_beam_files.py tests/test_pipeline.py
```

### Modifier le worker

Regarder:

- `worker/run_modules/`
- `beam/db.py`

Tests:

```bash
pytest tests/test_worker_recovery.py
```

### Modifier l'UI

Regarder:

- `webapp/templates/partials/`
- `webapp/static/css/`
- `webapp/static/js/`

Tests:

```bash
pytest tests/test_webapp_routes.py
docker compose restart webapp
curl -fsS http://127.0.0.1:8501/app/create >/tmp/beam_create.html
```

## 7. Tests A Connaitre

Tests web:

```bash
pytest tests/test_webapp_routes.py
```

Tests align:

```bash
pytest tests/test_align.py
```

Tests build:

```bash
pytest tests/test_build_beam_files.py
```

Tests pipeline:

```bash
pytest tests/test_pipeline.py
```

Tests worker:

```bash
pytest tests/test_worker_recovery.py
```

Tous les tests:

```bash
pytest
```

Docs:

```bash
bash scripts/docs_check.sh
```

## 8. Regles De Structure

Les fichiers doivent rester lisibles:

- objectif: 500 à 1000 lignes max,
- ne pas remettre toute la logique dans un seul fichier,
- garder les entrypoints courts,
- ajouter la logique dans le module de domaine correspondant.

Ne pas ajouter de logique métier dans:

- `webapp/main.py`,
- `worker/run.py`,
- `beam/pipeline.py`,
- `scripts/align.py`,
- `scripts/build_beam_files.py`.

Ces fichiers servent surtout à garder les imports et commandes existants stables.

## 9. Debug Rapide

Webapp ne répond pas:

```bash
docker compose ps
docker compose logs --tail=200 webapp
```

Worker bloqué:

```bash
docker compose logs --tail=300 worker
```

Job reste `queued`:

- vérifier que `worker` tourne,
- regarder les logs worker,
- vérifier `MAX_CONCURRENT_JOBS`.

Job finit avec `0 links`:

- vérifier `Property mapping rules`,
- changer `Pattern search scope`,
- relaxer `Target class filter`,
- relancer avec `Ignore align cache`.

Erreur endpoint:

- réduire les parts,
- augmenter timeout si nécessaire,
- relancer car les endpoints SPARQL peuvent throttler.

## 10. Plan De Presentation Aux Chercheurs

Ordre conseillé:

1. Montrer l'UI `/app/create`, `/app/jobs`, `/app/history`, `/tutorial`.
2. Faire un petit job test.
3. Montrer où le job est créé dans `routes_jobs_downloads_ws.py`.
4. Montrer comment le worker récupère le job dans `worker/run_modules/runner.py`.
5. Montrer `generate_benchmark(...)` dans `beam/pipeline_modules/orchestrator.py`.
6. Montrer les modules `scripts/align_modules/`.
7. Montrer les modules `scripts/build_beam_files_modules/`.
8. Montrer les outputs dans `data/<ClassName>/beam_<timestamp>/`.
9. Montrer les tests associés.
10. Expliquer la règle: modifier le module de domaine, pas l'entrypoint.

## 11. Definition Of Done Avant De Donner Le Code

Avant de dire qu'une modification est prête:

```bash
pytest
bash scripts/docs_check.sh
docker compose restart webapp worker
curl -fsS http://127.0.0.1:8501/ >/tmp/beam_home.html
curl -fsS http://127.0.0.1:8501/api/dashboard >/tmp/beam_dashboard.json
```

Résultat attendu:

- tests verts,
- docs check OK,
- Docker up,
- webapp healthy,
- root page en 200,
- dashboard API en 200.
