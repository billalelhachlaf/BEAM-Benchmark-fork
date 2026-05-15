# Algorithms Documentation

This section explains all processing stages, from job submission to final dataset artifacts.

## Read Order

1. [pipeline_end_to_end.md](pipeline_end_to_end.md)
2. [alignment_stage.md](alignment_stage.md)
3. [build_stage.md](build_stage.md)

## Code Entry Points

- Web job creation: `webapp/modules/routes_jobs_downloads_ws.py` (`create_job` route).
- Worker loop: `worker/run.py` and `worker/run_modules/`.
- Pipeline orchestration: `beam/pipeline.py` and `beam/pipeline_modules/`.
- Alignment implementation: `scripts/align.py` and `scripts/align_modules/`.
- Build implementation: `scripts/build_beam_files.py` and `scripts/build_beam_files_modules/`.
