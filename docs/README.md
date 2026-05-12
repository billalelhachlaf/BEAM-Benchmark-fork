# BEAM-App Documentation Index

This documentation is organized by audience and depth.

## 1. Start Here

- Repository entrypoint: [../README.md](../README.md)
- In-app onboarding: open `/tutorial` from the dashboard top bar
- Project structure and boundaries: [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)
- Quality checklist: [QUALITY_CHECKLIST.md](QUALITY_CHECKLIST.md)
- Documentation style guide: [DOC_STYLE.md](DOC_STYLE.md)
- Audit template: [AUDIT_TEMPLATE.md](AUDIT_TEMPLATE.md)

## 2. Admin Documentation

- Overview: [admin/README.md](admin/README.md)
- Setup/install/run: [admin/setup_install_run.md](admin/setup_install_run.md)
- Docker deployment: [admin/docker_deploy.md](admin/docker_deploy.md)
- Architecture: [admin/architecture.md](admin/architecture.md)
- Operations: [admin/operations.md](admin/operations.md)
- Backup/restore: [admin/backup_restore.md](admin/backup_restore.md)
- Troubleshooting: [admin/troubleshooting.md](admin/troubleshooting.md)
- VM deployment: [DEPLOYMENT_VM.md](DEPLOYMENT_VM.md)

## 3. User Documentation

- Overview: [user/README.md](user/README.md)
- Step-by-step tutorial: [user/tutorial.md](user/tutorial.md)
- Recipes by use case: [user/recipes.md](user/recipes.md)
- Results interpretation: [user/results_interpretation.md](user/results_interpretation.md)
- FAQ: [user/faq.md](user/faq.md)

Recommended user path:
1. Open in-app `/tutorial`.
2. Run one small benchmark job.
3. Use `user/recipes.md` to refine matching strategy.
4. Use `user/results_interpretation.md` to evaluate outputs.

## 4. Processing and Algorithms (Code-Level)

- Index: [algorithms/README.md](algorithms/README.md)
- End-to-end pipeline: [algorithms/pipeline_end_to_end.md](algorithms/pipeline_end_to_end.md)
- Alignment stage details: [algorithms/alignment_stage.md](algorithms/alignment_stage.md)
- Build stage details: [algorithms/build_stage.md](algorithms/build_stage.md)

## 5. Developer Documentation

- Developer index: [dev/README.md](dev/README.md)
- How to modify code safely: [dev/how_to_modify_code.md](dev/how_to_modify_code.md)
- API and route map: [dev/api_route_map.md](dev/api_route_map.md)

## 6. Verification and Demonstration Assets

- Verification playbook: [verification/README.md](verification/README.md)
- Screenshot plan/proposals: [verification/screenshots_plan.md](verification/screenshots_plan.md)
- Current limits: [limits.md](limits.md)
- Wikidata reference: [wikidata_reference.md](wikidata_reference.md)

## 7. UI Design Guidelines

- Uncodixfy ruleset: [uncodixfy/Uncodixfy.md](uncodixfy/Uncodixfy.md)
- Skill format: [uncodixfy/SKILL.md](uncodixfy/SKILL.md)
- Upstream README (mirrored): [uncodixfy/README_UPSTREAM.md](uncodixfy/README_UPSTREAM.md)
- License: [uncodixfy/LICENSE](uncodixfy/LICENSE)

## Documentation Done Criteria

- An admin can deploy and operate without external help.
- A user can run a full build and interpret outputs.
- A developer can explain every processing stage with code references.
- Validation steps are explicit and executable.
