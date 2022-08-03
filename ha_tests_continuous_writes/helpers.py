# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import subprocess
from pathlib import Path
import yaml
from pymongo import MongoClient
from pytest_operator.plugin import OpsTest

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
PORT = 27017
APP_NAME = METADATA["name"]


async def get_password(ops_test: OpsTest, app) -> str:
    """Use the charm action to retrieve the password from provided unit.

    Returns:
        String with the password stored on the peer relation databag.
    """
    # can retrieve from any unit running unit so we pick the first
    unit_name = ops_test.model.applications[app].units[0].name
    unit_id = unit_name.split("/")[1]

    action = await ops_test.model.units.get(f"{app}/{unit_id}").run_action("get-admin-password")
    action = await action.wait()
    return action.results["admin-password"]


async def app_name(ops_test: OpsTest) -> str:
    """Returns the name of the cluster running MongoDB.

    This is important since not all deployments of the MongoDB charm have the application name
    "mongodb".

    Note: if multiple clusters are running MongoDB this will return the one first found.
    """
    status = await ops_test.model.get_status()
    for app in ops_test.model.applications:
        # note that format of the charm field is not exactly "mongodb" but instead takes the form
        # of `local:focal/mongodb-6`
        if "mongodb" in status["applications"][app]["charm"]:
            return app

    return None


async def clear_db_writes(ops_test: OpsTest) -> bool:
    """Stop the DB process and remove any writes to the test collection."""
    await stop_continous_writes(ops_test)

    # TODO REPLACE THIS: mongodb remove collection from database
    app = await app_name(ops_test)
    password = await get_password(ops_test, app)
    hosts = [unit.public_address for unit in ops_test.model.applications[app].units]
    hosts = ",".join(hosts)
    connection_string = f"mongodb://operator:{password}@{hosts}/admin?replicaSet={app}"
    client = MongoClient(connection_string)
    db = client["new-db"]
    test_collection = db["test_collection"]  # collection for continuous writes
    test_collection.drop()
    test_collection = db["test_ubuntu_collection"]  # collection for replication tests
    test_collection.drop()
    client.close()


async def start_continous_writes(ops_test: OpsTest, starting_number: int) -> None:
    """Starts continuous writes to MongoDB with available replicas.

    In the future this should be put in a dummy charm.
    """
    # TODO REPLACE THIS: mongod connection_string
    app = await app_name(ops_test)
    password = await get_password(ops_test, app)
    hosts = [unit.public_address for unit in ops_test.model.applications[app].units]
    hosts = ",".join(hosts)
    connection_string = f"mongodb://operator:{password}@{hosts}/admin?replicaSet={app}"

    # run continuous writes in the background.
    subprocess.Popen(
        [
            "python3",
            "tests/integration/ha_tests/continuous_writes.py",
            connection_string,
            str(starting_number),
        ]
    )


async def stop_continous_writes(ops_test: OpsTest) -> int:
    """Stops continuous writes to MongoDB and returns the last written value.

    In the future this should be put in a dummy charm.
    """
    # stop the process
    proc = subprocess.Popen(["pkill", "-9", "-f", "continuous_writes.py"])

    # wait for process to be killed
    proc.communicate()

    # TODO REPLACE THIS: mongo ops to find the last written value
    app = await app_name(ops_test)
    password = await get_password(ops_test, app)
    hosts = [unit.public_address for unit in ops_test.model.applications[app].units]
    hosts = ",".join(hosts)
    connection_string = f"mongodb://operator:{password}@{hosts}/admin?replicaSet={app}"
    client = MongoClient(connection_string)
    db = client["new-db"]
    test_collection = db["test_collection"]
    last_written_value = test_collection.find_one(sort=[("number", -1)])
    client.close()

    return last_written_value


# TODO REPLACE THIS: mongo ops to count DB entries
async def count_writes(ops_test: OpsTest) -> int:
    """New versions of pymongo no longer support the count operation, instead find is used."""
    app = await app_name(ops_test)
    password = await get_password(ops_test, app)
    hosts = [unit.public_address for unit in ops_test.model.applications[app].units]
    hosts = ",".join(hosts)
    connection_string = f"mongodb://operator:{password}@{hosts}/admin?replicaSet={app}"

    client = MongoClient(connection_string)
    db = client["new-db"]
    test_collection = db["test_collection"]
    return sum(1 for _ in test_collection.find())
