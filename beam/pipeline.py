from pathlib import Path

_MODULES = (
    'discovery_and_alignment.py',
    'graph_building.py',
    'orchestrator.py',
)


def _load_modules() -> None:
    module_dir = Path(__file__).with_name("pipeline_modules")
    namespace = globals()
    for module_name in _MODULES:
        module_path = module_dir / module_name
        code = module_path.read_text()
        exec(compile(code, str(module_path), "exec"), namespace)


_load_modules()
