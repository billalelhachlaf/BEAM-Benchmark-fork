# Backup et restore (admin)

## A sauvegarder

- `docker-data/jobs.db`
- `docker-data/data/`
- `docker-data/Download/` (optionnel mais recommande si downloads/caches couteux)
- `docker-data/logs/`
- `catalog/wdc_classes_catalog.json`

## Backup rapide

```bash
mkdir -p backups
tar -czf backups/beam_backup_$(date +%F_%H%M%S).tgz \
  docker-data catalog/wdc_classes_catalog.json
```

## Restore

1. Arreter services:

```bash
docker compose down
```

2. Restaurer archive:

```bash
tar -xzf backups/beam_backup_<DATE>.tgz -C /home/<user>/BEAM-App
```

3. Redemarrer:

```bash
docker compose up -d
docker compose exec webapp bash scripts/check_health.sh
```

## Validation post-restore

- Dashboard accessible.
- Jobs historiques visibles.
- Builds historiques consultables.
- Un smoke-run court passe.
