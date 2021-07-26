#!/usr/bin/python3

"""Submit a job to Graham, translating sshfs mounts."""

import argparse
import logging
import subprocess
from pathlib import Path, PurePath


class SshfsError(Exception):
    """Exception raised when an sshfs error is encountered."""

    def __init__(self, message):
        Exception.__init__()
        self.message = message

    def __str__(self):
        return self.message


parser = argparse.ArgumentParser()
parser.add_argument("address")
parser.add_argument("app")
parser.add_argument("bids_dir")
parser.add_argument("out_dir")
parser.add_argument("analysis_level")

args = parser.parse_args()

all_mounts = str(
    subprocess.run(
        ["findmnt", "-l"],
        check=True,
        stdout=subprocess.PIPE,
    ).stdout,
    encoding="utf-8",
).splitlines()

sshfs_mounts = {
    Path(mount.split()[0]): mount.split()[1]
    for mount in all_mounts
    if "sshfs" in mount
}

# check if bids_dir and out_dir are children of an active sshfs mount
bids_dir = Path(args.bids_dir).resolve(strict=True)
out_dir = Path(args.out_dir).resolve(strict=True)

bids_dir_remote = None
out_dir_remote = None

for local_path in sshfs_mounts.keys():
    try:
        resolved_mount = local_path.resolve()
    except FileNotFoundError as err:
        raise SshfsError(
            "sshfs mount {} no longer exists.".format(local_path)
        ) from err

    if str(bids_dir).startswith(str(resolved_mount)):
        logging.debug(
            "sshfs mount %s is a parent of bids_dir %s", local_path, bids_dir
        )

        bids_dir_remote = PurePath(
            sshfs_mounts[local_path].split(":")[1]
        ) / bids_dir.relative_to(local_path)
        logging.debug("Changing to remote dir %s", bids_dir_remote)
    if str(out_dir).startswith(str(resolved_mount)):
        logging.debug(
            "sshfs mount %s is a parent of out_dir %s", local_path, bids_dir
        )
        out_dir_remote = PurePath(
            sshfs_mounts[local_path].split(":")[1]
        ) / out_dir.relative_to(local_path)
        logging.debug("Changing to remote dir %s", sshfs_mounts[local_path])

if any(remote_dir is None for remote_dir in [bids_dir_remote, out_dir_remote]):
    if bids_dir_remote is None:
        err_path = bids_dir_remote
    if out_dir_remote is None:
        err_path = (
            out_dir_remote
            if bids_dir_remote is None
            else " or ".join([str(bids_dir_remote), str(out_dir_remote)])
        )
    raise SshfsError(
        "Could not translate local path {} to an sshfs mount.".format(err_path)
    )

subprocess.run(
    [
        "ssh",
        args.address,
        "source",
        "~/.bash_profile",
        ";",
        "bidsBatch",
        args.app,
        bids_dir_remote,
        out_dir_remote,
        args.analysis_level,
    ],
    check=True,
    stdout=subprocess.PIPE,
)
