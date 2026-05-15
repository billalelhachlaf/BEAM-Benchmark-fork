# Guide de demarrage BEAM-App

Updated: 2026-05-12  
Audience: utilisateurs, chercheurs et developpeurs qui decouvrent le projet  
Prerequisites: Docker installe pour lancer l'application localement

Ce guide donne une vue d'ensemble rapide de BEAM-App: a quoi sert l'application, comment la lancer, ou regarder dans le code, et comment verifier qu'une modification est prete.

## 1. Idee generale

BEAM-App sert a construire des datasets d'alignement d'entites au format BEAM a partir de:

- classes WDC,
- endpoints SPARQL comme Wikidata, DBpedia, YAGO ou un endpoint custom,
- regles de matching configurees depuis une interface web.

L'application a deux processus principaux:

- `webapp`: interface web FastAPI, APIs, dashboard, pages de resultats, telechargements.
- `worker`: processus de fond qui prend les jobs en file d'attente et lance le pipeline.

Le flux normal est:

1. L'utilisateur configure un job dans l'UI.
2. Le webapp valide le formulaire.
3. Le webapp insere un job dans SQLite (`jobs.db`).
4. Le worker recupere le job.
5. Le pipeline lance l'etape `align`.
6. Le pipeline lance l'etape `build`.
7. Les fichiers finaux sont ecrits dans `data/<ClassName>/beam_<timestamp>/`.
8. Le webapp affiche le build termine dans History.

## 2. Commandes de base

Demarrer:

```bash
docker compose up -d --build
```

Ouvrir:

```text
http://localhost:8501
```

Verifier Docker:

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

## 3. Source et runtime

Code source a modifier:

- `webapp/`
- `worker/`
- `beam/`
- `scripts/`
- `tests/`
- `docs/`
- `catalog/`

Donnees generees a ne pas committer:

- `data/`
- `Download/`
- `logs/`
- `reports/`
- `jobs.db*`
- `.run/`
- `docker-data/`

Regle importante: ne jamais committer les outputs de jobs, caches, datasets telecharges, DB locale ou fichiers temporaires.

## 4. Structure du code

### Webapp

Point d'entree:

- `webapp/main.py`

Ce fichier cree l'app FastAPI, charge les constantes, puis charge les modules.

Modules principaux:

- `webapp/modules/forms_and_inputs.py`: valeurs par defaut, validation et normalisation des parametres.
- `webapp/modules/routes_pages_sakey_builds.py`: pages principales, page tutorial, page SAKey, build detail, link explorer et dashboard API.
- `webapp/modules/routes_jobs_downloads_ws.py`: API preflight, creation de jobs, cancel/rerun/delete, telechargements et websocket logs.
- `webapp/modules/jobs_dashboard.py`: etat dashboard, resume des jobs et resume des builds.
- `webapp/modules/builds.py`: scan du dossier `data/`, resume des builds, stats et resolution des artefacts.
- `webapp/modules/preflight.py`: diagnostic avant lancement.
- `webapp/modules/link_explorer.py`: lecture des fichiers `ent_links`, detail d'un lien, payload des noeuds WDC/target.
- `webapp/modules/sakey_core.py`: cycle de vie des runs SAKey.
- `webapp/modules/sakey_artifacts.py`: artefacts et rapports SAKey.

Templates principaux:

- `webapp/templates/index.html`: shell court de la page principale.
- `webapp/templates/partials/index_create.html`: wizard de creation.
- `webapp/templates/partials/index_dashboard.html`: Jobs + History.
- `webapp/templates/partials/index_script_*.html`: JavaScript de la page principale, decoupe.
- `webapp/templates/link_explorer.html`: shell du link explorer.
- `webapp/templates/partials/link_explorer_*`: UI, style et script du link explorer.

### Worker

Point d'entree:

- `worker/run.py`

Modules:

- `worker/run_modules/progress_helpers.py`: parsing et formatage de progression.
- `worker/run_modules/runner.py`: polling des jobs, transitions d'etat, cancellation et lancement align/build.

### Pipeline

Point d'entree:

- `beam/pipeline.py`

Modules:

- `beam/pipeline_modules/discovery_and_alignment.py`: decouverte des parts, preparation align, gestion cache align.
- `beam/pipeline_modules/graph_building.py`: helpers pour extraction et preparation de graphes.
- `beam/pipeline_modules/orchestrator.py`: fonction centrale `generate_benchmark(...)`.

### Alignement

Point d'entree:

- `scripts/align.py`

Modules:

- `scripts/align_modules/normalization_and_matching.py`: normalisation, matching exact/fuzzy, helpers de comparaison.
- `scripts/align_modules/target_endpoint.py`: endpoint, prefixes, extraction et format de valeurs target.
- `scripts/align_modules/target_fetching.py`: requetes SPARQL, recuperation de candidats.
- `scripts/align_modules/matching_execution.py`: execution du matching.

### Build BEAM

Point d'entree:

- `scripts/build_beam_files.py`

Modules:

- `scripts/build_beam_files_modules/parsing_and_outputs.py`: parsing NQ/NT, split attr/rel, stats et ecriture des fichiers BEAM.
- `scripts/build_beam_files_modules/enrichment.py`: labels et descriptions.
- `scripts/build_beam_files_modules/cli.py`: interface CLI.

### Database

Point d'entree:

- `beam/db.py`

Ce module contient le schema SQLite, les jobs, les subjobs, les events/logs et les metadata WDC classes.

## 5. Suivre un job dans le code

Pour expliquer le projet, le chemin le plus important est la chaine de creation puis d'execution d'un job.

### 1. Soumission UI

Fichier:

```text
webapp/modules/routes_jobs_downloads_ws.py
```

Fonction:

```text
create_job(...)
```

Elle recupere les champs du formulaire, nettoie les valeurs, valide via `forms_and_inputs.py`, puis insere le job avec `db.insert_job(...)`.

### 2. Stockage SQLite

Fichier:

```text
beam/db.py
```

Le job est stocke avec ses parametres JSON, son status, sa phase, ses timestamps et ses logs/events.

### 3. Worker

Fichier:

```text
worker/run_modules/runner.py
```

Le worker poll les jobs `queued`, respecte `MAX_CONCURRENT_JOBS`, passe le job en `running`, lance align puis build, et ecrit les events de progression.

### 4. Pipeline

Fichier:

```text
beam/pipeline_modules/orchestrator.py
```

Fonction cle:

```text
generate_benchmark(...)
```

Elle coordonne la selection des parts WDC, l'alignement, la verification du nombre de liens, la generation BEAM et la finalisation du job.

### 5. Align

Dossier:

```text
scripts/align_modules/
```

Role: lire les valeurs WDC, construire les candidats cote endpoint, lancer les requetes SPARQL, comparer les valeurs, puis produire les liens WDC -> target.

### 6. Build

Dossier:

```text
scripts/build_beam_files_modules/
```

Role: lire `ent_links`, lire les triples, separer attributs et relations, produire les fichiers BEAM, puis ecrire stats et metadata.

### 7. Affichage

Fichiers:

```text
webapp/modules/builds.py
webapp/modules/jobs_dashboard.py
webapp/modules/routes_pages_sakey_builds.py
```

Ils scannent `data/`, preparent les summaries, alimentent History et le dashboard, puis servent les downloads.

## 6. Modifier sans se perdre

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

## 7. Tests a connaitre

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

## 8. Regles de structure

Les fichiers doivent rester lisibles:

- objectif: 500 a 1000 lignes max,
- ne pas remettre toute la logique dans un seul fichier,
- garder les entrypoints courts,
- ajouter la logique dans le module de domaine correspondant.

Ne pas ajouter de logique metier dans:

- `webapp/main.py`
- `worker/run.py`
- `beam/pipeline.py`
- `scripts/align.py`
- `scripts/build_beam_files.py`

Ces fichiers servent surtout a garder les imports et commandes existants stables.

## 9. Debug rapide

Webapp ne repond pas:

```bash
docker compose ps
docker compose logs --tail=200 webapp
```

Worker bloque:

```bash
docker compose logs --tail=300 worker
```

Job reste `queued`:

- verifier que `worker` tourne,
- regarder les logs worker,
- verifier `MAX_CONCURRENT_JOBS`.

Job finit avec `0 links`:

- verifier `Property mapping rules`,
- changer `Pattern search scope`,
- relaxer `Target class filter`,
- relancer avec `Ignore align cache`.

Erreur endpoint:

- reduire les parts,
- augmenter timeout si necessaire,
- relancer car les endpoints SPARQL peuvent throttler.

## 10. Plan de presentation aux chercheurs

Ordre conseille:

1. Montrer l'UI `/app/create`, `/app/jobs`, `/app/history`, `/tutorial`.
2. Faire un petit job test.
3. Montrer ou le job est cree dans `routes_jobs_downloads_ws.py`.
4. Montrer comment le worker recupere le job dans `worker/run_modules/runner.py`.
5. Montrer `generate_benchmark(...)` dans `beam/pipeline_modules/orchestrator.py`.
6. Montrer les modules `scripts/align_modules/`.
7. Montrer les modules `scripts/build_beam_files_modules/`.
8. Montrer les outputs dans `data/<ClassName>/beam_<timestamp>/`.
9. Montrer les tests associes.
10. Expliquer la regle: modifier le module de domaine, pas l'entrypoint.

## 11. Definition of done

Avant de dire qu'une modification est prete:

```bash
pytest
bash scripts/docs_check.sh
docker compose restart webapp worker
curl -fsS http://127.0.0.1:8501/ >/tmp/beam_home.html
curl -fsS http://127.0.0.1:8501/api/dashboard >/tmp/beam_dashboard.json
```

Resultat attendu:

- tests verts,
- docs check OK,
- Docker up,
- webapp healthy,
- root page en 200,
- dashboard API en 200.
