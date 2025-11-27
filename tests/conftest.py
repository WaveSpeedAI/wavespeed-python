import pytest


def pytest_addoption(parser):
    parser.addoption("--no-skip", action="store_true", default=False, help="disable skip marks")
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        skip_slow = pytest.mark.skip(reason="need --runslow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    if config.getoption("--no-skip"):
        for test in items:
            test.own_markers = [marker for marker in test.own_markers if marker.name not in ("skip", "skipif")]
