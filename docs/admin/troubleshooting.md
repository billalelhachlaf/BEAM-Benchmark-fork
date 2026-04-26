# Troubleshooting (admin)

## Webapp inaccessible

Symptome: `ERR_CONNECTION_REFUSED`.

Checks:

```bash
bash scripts/check_health.sh
ss -ltnp | rg ':8501'
tail -n 100 logs/webapp.log
```

Causes frequentes:

- webapp non lance,
- bind sur mauvais host,
- port bloque par firewall/proxy.

## Jobs bloques en queue

Checks:

```bash
tail -n 200 logs/worker.log
python - <<'PY'
import sqlite3
c=sqlite3.connect('jobs.db')
print(c.execute("select status,count(*) from jobs group by status").fetchall())
PY
```

Actions:

- redemarrer worker,
- annuler jobs bloques,
- relancer sans cache si necessaire.

## Builds avec 0 liens

Verifier:

- classe cible,
- patterns de linking,
- endpoint/prop cible,
- mode (`sameAs` vs `property`).

Action:

- tester un mapping plus robuste,
- forcer align sans cache,
- verifier le filtre de classe endpoint.

## SAKEY en erreur

Verifier:

- qualite des parts (triples valides),
- support minimum trop strict,
- ressources machine (RAM/CPU),
- logs `run.log` et output SAKEY.
