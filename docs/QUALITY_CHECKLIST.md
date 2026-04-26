# Checklist qualite BEAM-App

## 1. Fonctionnalite

- [ ] Le webapp demarre et repond sur `:8501`.
- [ ] Le worker demarre et consomme les jobs.
- [ ] Un job complet (align + build) passe sur une petite classe.
- [ ] Les jobs annulables (`cancel`, `cancel align`, `cancel build`) fonctionnent.
- [ ] Les builds termines sont telechargeables.
- [ ] Les erreurs sont visibles dans l'UI et les logs.

## 2. Structure projet

- [ ] Dossiers principaux documentes (`webapp`, `worker`, `scripts`, `beam`, `docs`, `tests`).
- [ ] Les scripts d'exploitation sont dans `scripts/`.
- [ ] Les fichiers runtime volumineux ne sont pas versionnes.
- [ ] Les conventions de nommage des builds/jobs sont explicites.

## 3. Documentation admin

- [ ] Guide setup/install valide sur machine vierge.
- [ ] Guide run/restart/stop valide.
- [ ] Guide operations (logs, DB, backup) valide.
- [ ] Guide troubleshooting couvre les erreurs frequentes.

## 4. Documentation user

- [ ] Tutoriel "premier build" reproduisible.
- [ ] Explication claire des champs de config.
- [ ] Explication des sorties (`ent_links`, `attr_*`, `rel_*`, `BUILD_STATS`).
- [ ] FAQ couvrant les cas frequents.

## 5. Limites et risques

- [ ] Limites endpoint SPARQL documentees.
- [ ] Limites de performance/grand volume documentees.
- [ ] Limites de qualite des donnees (bruit, blank nodes, langue) documentees.
- [ ] Contournements recommandes fournis.

## 6. Validation finale

- [ ] `pytest` vert sur les tests critiques webapp.
- [ ] `scripts/check_health.sh` OK.
- [ ] Relecture docs par un admin + un utilisateur.
