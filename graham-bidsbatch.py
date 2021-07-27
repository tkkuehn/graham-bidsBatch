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
local_group = parser.add_argument_group("Local")
local_group.add_argument(
    "address", help="destination (i.e. <user>@graham.computecanada.ca)"
)
bidsbatch_group = parser.add_argument_group("bidsBatch")
bidsbatch_group.add_argument(
    "-s",
    help="single-subject mode, run on a single subject instead",
    metavar="subjid",
)
bidsbatch_group.add_argument(
    "-t", action="store_true", help="test-mode, don't actually submit any jobs"
)
bidsbatch_group.add_argument(
    "-A",
    help="account to use for allocation (default: ctb-akhanf)",
    metavar="account",
)
bidsbatch_group.add_argument(
    "-j", help="sets required resources", metavar="job-template"
)
bidsbatch_group.add_argument(
    "app", help="one of the available apps on Graham."
)
bidsbatch_group.add_argument("bids_dir")
bidsbatch_group.add_argument("out_dir")
bidsbatch_group.add_argument(
    "analysis_level", choices=["participant", "group"]
)

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
        f"-s {args.s}" if args.s is not None else "",
        "-t" if args.t else "",
        f"-A {args.A}" if args.A is not None else "",
        f"-j {args.j}" if args.j is not None else "",
        args.app,
        bids_dir_remote,
        out_dir_remote,
        args.analysis_level,
    ],
    check=True,
    stdout=subprocess.PIPE,
)
