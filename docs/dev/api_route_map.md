# API and Route Map

Primary code location: `webapp/main.py`.

## HTML Pages

- `GET /`: main dashboard and job form.
- `GET /help`: in-app user/admin quick help.
- `GET /sakey`: SAKEY explorer page.
- `GET /builds/{class_name}/{build_name}`: build detail page.
- `GET /builds/{class_name}/{build_name}/links`: link explorer page.

## Operational APIs

- `GET /api/dashboard`
- `GET /api/class_parts/{class_name}`
- `GET /api/preflight`
- `POST /jobs`
- `POST /jobs/{job_id}/cancel`
- `POST /jobs/{job_id}/cancel_subjob/{subjob_type}`
- `POST /jobs/{job_id}/rerun`
- `POST /jobs/{job_id}/rerun_nocache`
- `POST /jobs/{job_id}/rerun_align`
- `POST /jobs/{job_id}/rerun_build`
- `POST /jobs/{job_id}/delete`

## Build Artifact APIs

- `GET /builds/{class_name}/{build_name}/download`
- `POST /builds/{class_name}/{build_name}/delete`
- `GET /api/builds/{class_name}/{build_name}/links`
- `GET /api/builds/{class_name}/{build_name}/link`
- `GET /api/builds/{class_name}/{build_name}/node`

## Realtime

- `WS /ws/logs/{job_id}`
