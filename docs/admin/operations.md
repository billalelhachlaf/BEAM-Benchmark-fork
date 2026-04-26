# Operations (admin)

## Fichiers et dossiers critiques

- `jobs.db`: etat queue/jobs.
- `logs/webapp.log`, `logs/worker.log`: logs runtime.
- `Download/`: donnees WDC + cache align.
- `data/`: builds generes.

## Commandes operations

Etat:

```bash
bash scripts/check_health.sh
```

Restart complet:

```bash
bash scripts/restart_server.sh
```

Nettoyage jobs arretes/anciens (via UI recommande):

- supprimer jobs `cancelled/error` inutiles,
- conserver les builds de reference (star/favoris).

## Backup minimum

Sauvegarder regulierement:

- `jobs.db`
- `data/`
- `Download/` (si recalcul couteux)

Exemple:

```bash
tar -czf backup_beam_$(date +%F).tgz jobs.db data Download
```

## Upgrade procedure

```bash
git pull
source .venv/bin/activate
pip install -r requirements.txt
bash scripts/restart_server.sh
bash scripts/check_health.sh
```

## Supervision simple

- watch sur `check_health.sh`
- alerte si webapp non joignable
- alerte si worker arrete
