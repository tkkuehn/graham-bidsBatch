#!/usr/bin/python3

"""Submit a job to Graham, translating sshfs mounts."""

import argparse
import logging
import subprocess
from pathlib import Path, PurePath


DOMAIN = "graham.computecanada.ca"


class SshfsError(Exception):
    """Exception raised when an sshfs error is encountered."""

    def __init__(self, message):
        Exception.__init__()
        self.message = message

    def __str__(self):
        return self.message


def gen_parser():
    """Generate a parser for the script."""
    parser = argparse.ArgumentParser()
    local_group = parser.add_argument_group("Local")
    local_group.add_argument(
        "username",
        help="Username (will ssh to <username>@graham.computecanada.ca)",
    )
    bidsbatch_group = parser.add_argument_group("bidsBatch")
    bidsbatch_group.add_argument(
        "-s",
        help="single-subject mode, run on a single subject instead",
        metavar="subjid",
    )
    bidsbatch_group.add_argument(
        "-t",
        action="store_true",
        help="test-mode, don't actually submit any jobs",
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
    return parser


def find_sshfs_parent(child_dir, username):
    """Find all active sshfs mounts and check that one is a parent of child_dir.

    This requires "findmnt" to be installed.

    Arguments
    ---------
    bids_dir : str
        Location of BIDS dataset.
    out_dir : str
        Desired output directory.
    """
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
        if all(["sshfs" in mount, f"{username}@{DOMAIN}" in mount])
    }

    # check if child_dir is a child of an active sshfs mount
    child_dir = Path(child_dir).resolve(strict=True)
    remote_dir = None

    for local_path in sshfs_mounts.keys():
        try:
            resolved_mount = local_path.resolve()
        except FileNotFoundError as err:
            raise SshfsError(
                "sshfs mount {} no longer exists.".format(local_path)
            ) from err

        if str(child_dir).startswith(str(resolved_mount)):
            logging.debug(
                "sshfs mount %s is a parent of dir %s",
                local_path,
                child_dir,
            )

            remote_dir = PurePath(
                sshfs_mounts[local_path].split(":")[1]
            ) / child_dir.relative_to(local_path)
            logging.debug("Changing to remote dir %s", remote_dir)

    if remote_dir is None:
        raise SshfsError(
            "Could not translate local path {} to an sshfs mount.".format(
                child_dir
            )
        )

    return remote_dir


def run_bidsbatch(args, bids_dir_remote, out_dir_remote):
    """SSH to Graham and run bidsBatch with the provided args.

    Parameters
    ----------
    args : Namespace
        Parsed input arguments, including "address", "s", "A", "j", "app",
        and "analysis_level".
    bids_dir_remote : PurePath
        Remote directory containing the BIDS dataset to be processed.
    out_dir_remote : PurePath
        Remote directory for the BIDS app output.
    """
    subprocess.run(
        [
            "ssh",
            f"{args.username}@{DOMAIN}",
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


def main():
    """Process CLI arguments and run bidsBatch accordingly."""
    parser = gen_parser()
    args = parser.parse_args()
    bids_dir_remote = find_sshfs_parent(args.bids_dir, args.username)
    out_dir_remote = find_sshfs_parent(args.out_dir, args.username)
    run_bidsbatch(args, bids_dir_remote, out_dir_remote)


if __name__ == "__main__":
    main()
