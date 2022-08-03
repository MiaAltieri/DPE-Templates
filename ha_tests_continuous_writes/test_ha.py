#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.


import pytest
from pytest_operator.plugin import OpsTest

import logging

logger = logging.getLogger(__name__)

from tests.integration.ha_tests.helpers import (
    app_name,
    clear_db_writes,
    count_writes,
    start_continous_writes,
    stop_continous_writes,
)

ANOTHER_DATABASE_APP_NAME = "another-database-a"
MONGOD_PROCESS = "/usr/bin/mongod"


@pytest.fixture()
async def continuous_writes(ops_test: OpsTest):
    """Starts continuous write operations to MongoDB for test and clears writes at end of test."""
    await start_continous_writes(ops_test, 1)
    yield
    await clear_db_writes(ops_test)


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest) -> None:
    """Build and deploy DB."""
    # it is possible for users to provide their own cluster for HA testing. Hence check if there
    # is a pre-existing cluster.
    if await app_name(ops_test):
        return

    my_charm = await ops_test.build_charm(".")
    await ops_test.model.deploy(my_charm, num_units=3)
    await ops_test.model.wait_for_idle()


async def test_something(ops_test, continuous_writes):
    """This tests something while using the continuous writes fixture.

    When this test starts continuous writes are made the the DB. When this test ends (successfully
    or unsuccessfully) the writes to the DB are cleared.
    """

    # TODO: test something here related to DB

    # verify that the no writes were skipped
    total_expected_writes = await stop_continous_writes(ops_test)
    actual_expected_writes = await count_writes(ops_test)
    assert total_expected_writes["number"] == actual_expected_writes
