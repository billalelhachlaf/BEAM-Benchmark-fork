from pathlib import Path

_MODULES = (
    'dashboard_and_job_route_tests.py',
    'link_explorer_route_tests.py',
)


def _load_modules() -> None:
    module_dir = Path(__file__).with_name("test_webapp_routes_modules")
    namespace = globals()
    for module_name in _MODULES:
        module_path = module_dir / module_name
        code = module_path.read_text()
        exec(compile(code, str(module_path), "exec"), namespace)


_load_modules()
