from pathlib import Path

_MODULES = (
    'normalization_and_matching.py',
    'target_endpoint.py',
    'target_fetching.py',
    'matching_execution.py',
)


def _load_modules() -> None:
    module_dir = Path(__file__).with_name("align_modules")
    namespace = globals()
    for module_name in _MODULES:
        module_path = module_dir / module_name
        code = module_path.read_text()
        exec(compile(code, str(module_path), "exec"), namespace)


_load_modules()
