#!/usr/bin/python3

import argparse
import subprocess

all_mounts = str(
    subprocess.run(
        ["findmnt", "-l"],
        check=True,
        stdout=subprocess.PIPE,
    ).stdout,
    encoding="utf-8",
).splitlines()

sshfs_mounts = [
    {mount.split()[0]: mount.split()[1]} for mount in all_mounts if "sshfs" in mount
]

parser = argparse.ArgumentParser()
parser.add_argument("address")
parser.add_argument("app")
parser.add_argument("bids_dir")
parser.add_argument("out_dir")
parser.add_argument("analysis_level")

args = parser.parse_args()

subprocess.run(
    [
        "ssh",
        args.address,
        "source",
        "~/.bash_profile",
        ";",
        "bidsBatch",
        args.app,
        args.bids_dir,
        args.out_dir,
        args.analysis_level,
    ],
    check=True,
    stdout=subprocess.PIPE,
)
