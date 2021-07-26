#!/usr/bin/python3

"""Submit a job to Graham, translating sshfs mounts."""

import argparse
import subprocess
from pathlib import Path

all_mounts = str(
    subprocess.run(
        ["findmnt", "-l"],
        check=True,
        stdout=subprocess.PIPE,
    ).stdout,
    encoding="utf-8",
).splitlines()

sshfs_mounts = [
    {Path(mount.split()[0]): mount.split()[1]}
    for mount in all_mounts
    if "sshfs" in mount
]

parser = argparse.ArgumentParser()
parser.add_argument("address")
parser.add_argument("app")
parser.add_argument("bids_dir")
parser.add_argument("out_dir")
parser.add_argument("analysis_level")

args = parser.parse_args()

# check if bids_dir and out_dir are children of an active sshfs mount
bids_dir = Path(args.bids_dir).resolve(strict=True)
out_dir = Path(args.out_dir).resolve(strict=True)

for local_path in sshfs_mounts.keys():
    if local_path.parts == bids_dir.parts[:len(local_path.parts)]:
        print("Parent found")

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
