# Architecture (admin)

## Vue d'ensemble

BEAM-App repose sur 2 processus principaux:

1. `webapp` (FastAPI)
- UI dashboard, APIs, logs websockets, exploration SAKEY.

2. `worker`
- Consomme les jobs de `jobs.db`.
- Execute `align` puis `build`.

## Flux run

1. L'utilisateur soumet un job via UI.
2. Job enregistre en base (`jobs.db`) et passe `queued`.
3. Worker prend le job -> phase `align`.
4. Si alignement valide -> phase `build`.
5. Sorties ecrites dans `data/<Class>/beam_<timestamp>/`.

## Composants principaux

- `webapp/main.py`: point d'entree FastAPI stable.
- `webapp/modules/`: routes UI/API et services web.
- `worker/run.py`: point d'entree worker stable.
- `worker/run_modules/`: moteur d'execution queue.
- `beam/pipeline.py`: point d'entree pipeline stable.
- `beam/pipeline_modules/`: pipeline de haut niveau.
- `scripts/align.py`: point d'entree align stable.
- `scripts/align_modules/`: logique de matching/linking.
- `scripts/build_beam_files.py`: point d'entree build stable.
- `scripts/build_beam_files_modules/`: generation fichiers BEAM.

## Donnees et etat

- `jobs.db`: queue, sous-jobs, statuts, historique.
- `Download/`: parts WDC et caches align.
- `data/`: artefacts builds et rapports.
- `logs/`: logs webapp/worker.

## Concurrence

- Jobs pipeline: controles via `MAX_CONCURRENT_JOBS`.
- SAKEY Explorer: controle via `SAKEY_MAX_CONCURRENT`.
