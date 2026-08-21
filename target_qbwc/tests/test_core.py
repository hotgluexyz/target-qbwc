"""Tests standard target features using the built-in SDK tests library."""

import pytest
from hotglue_singer_sdk.testing import get_standard_target_tests

from target_qbwc.target import TargetQbwc


@pytest.fixture
def config():
    """Return a minimal target config for SDK standard tests."""
    return {"token": "test-token", "is_sandbox": True}


def test_standard_target_tests(config):
    """Run standard target tests from the SDK."""
    tests = get_standard_target_tests(TargetQbwc, config=config)
    for test in tests:
        test()
