"""Sample of how to enable self healing for machine charms. 

This is a simplified sample from how the MongoDB machine charm does this. see:
https://github.com/canonical/mongodb-operator/blob/main/lib/charms/mongodb_libs/v0/machine_helpers.py

To add this to your code:
- call update_mongod_service after installing the DB (installing the DB should write the original service file)
- change constants


"""
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
from charms.operator_libs_linux.v1 import systemd


# systemd gives files in /etc/systemd/system/ precedence over those in /lib/systemd/system/ hence
# our changed file in /etc will be read while maintaining the original one in /lib.
MONGOD_SERVICE_UPSTREAM_PATH = "/lib/systemd/system/mongod.service"
MONGOD_SERVICE_DEFAULT_PATH = "/etc/systemd/system/mongod.service"

# restart options specify that systemd should attempt to restart the service on failure.
RESTART_OPTIONS = ["Restart=always\n", "RestartSec=5s\n"]
# limits ensure that the process will not continously retry to restart if it continously fails to
# restart.
RESTARTING_LIMITS = ["StartLimitIntervalSec=500\n", "StartLimitBurst=5\n"]


def update_mongod_service() -> None:
    """Updates the mongod service file.

    This should be called by the install hook as to set up the service file before the start hook.
    """
    with open(MONGOD_SERVICE_UPSTREAM_PATH, "r") as up_service_file:
        db_service = up_service_file.readlines()

    # self healing is implemented via systemd
    add_self_healing(db_service)

    # systemd gives files in /etc/systemd/system/ precedence over those in /lib/systemd/system/
    # hence our changed file in /etc will be read while maintaining the original one in /lib.
    with open(MONGOD_SERVICE_DEFAULT_PATH, "w") as service_file:
        service_file.writelines(db_service)

    # changes to service files are only applied after reloading
    systemd.daemon_reload()


def add_self_healing(service_lines):
    """Updates the service file to auto-restart the DB service on service failure.

    Options for restarting allow for auto-restart on crashed services, i.e. DB killed, DB frozen,
    DB terminated.
    """
    for index, line in enumerate(service_lines):
        if "[Unit]" in line:
            service_lines.insert(index + 1, RESTARTING_LIMITS[0])
            service_lines.insert(index + 1, RESTARTING_LIMITS[1])

        if "[Service]" in line:
            service_lines.insert(index + 1, RESTART_OPTIONS[0])
            service_lines.insert(index + 1, RESTART_OPTIONS[1])
