# Backup et restore (admin)

## A sauvegarder

- `jobs.db`
- `data/`
- `Download/` (optionnel mais recommande si downloads/caches couteux)
- `catalog/wdc_classes_catalog.json`

## Backup rapide

```bash
mkdir -p backups
tar -czf backups/beam_backup_$(date +%F_%H%M%S).tgz \
  jobs.db data Download catalog/wdc_classes_catalog.json logs
```

## Restore

1. Arreter services:

```bash
bash scripts/stop_server.sh
```

2. Restaurer archive:

```bash
tar -xzf backups/beam_backup_<DATE>.tgz -C /home/<user>/BEAM-App
```

3. Redemarrer:

```bash
bash scripts/restart_server.sh
bash scripts/check_health.sh
```

## Validation post-restore

- Dashboard accessible.
- Jobs historiques visibles.
- Builds historiques consultables.
- Un smoke-run court passe.
