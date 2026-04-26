# Setup, installation, run (admin)

## 1. Prerequis

- Linux avec Python 3.12+ recommande.
- Acces reseau sortant vers endpoints et sources de donnees.
- `git`, `curl`, `bash` disponibles.

## 2. Installation

```bash
git clone <repo-url>
cd BEAM-App
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Option tests:

```bash
pip install -r requirements-dev.txt
```

## 3. Demarrage standard

```bash
bash scripts/run_server.sh
```

Verification:

```bash
bash scripts/check_health.sh
```

URL:

- local: `http://127.0.0.1:8501`
- remote: `http://<host>:8501`

## 4. Arret / restart

```bash
bash scripts/stop_server.sh
bash scripts/restart_server.sh
```

## 5. Variables d'environnement utiles

- `MAX_CONCURRENT_JOBS`: concurrence worker jobs.
- `JOB_POLL_INTERVAL`: frequence de polling worker.
- `SAKEY_MAX_CONCURRENT`: concurrence runs SAKEY.
- `WEBAPP_HOST`: host bind webapp (par defaut `0.0.0.0`).

Exemple:

```bash
MAX_CONCURRENT_JOBS=2 SAKEY_MAX_CONCURRENT=1 bash scripts/restart_server.sh
```

## 6. Verification fonctionnelle rapide (smoke test)

1. Ouvrir le dashboard.
2. Lancer un build sur une petite classe locale/test.
3. Verifier passage `queued -> running -> done`.
4. Verifier telechargement build.

## 7. Notes de securite

- Ne pas exposer `jobs.db` ni les dossiers `Download/` et `data/` au web public.
- Utiliser un reverse proxy pour l'exposition internet.
