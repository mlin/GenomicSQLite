#!/usr/bin/env python3
"""
Assert that ELF shared objects' required symbol versions stay at or below the
project's compatibility ceilings -- the manylinux_2_28 floor asserted by CI for
the portable libraries (GLIBC <= 2.28, GLIBCXX <= 3.4.25).

The check parses the dynamic version needs (the "Version References" of
objdump -p: exactly the (library, version) requirements the dynamic loader
enforces) instead of scraping version strings out of the symbol table.

Usage:
    check_elf_version_floor.py [--glibc X.Y] [--glibcxx X.Y.Z] FILE [FILE...]

Exit status: 0 if every file is within the ceilings, 1 otherwise (with a
diagnostic naming the file and the offending requirement).
"""

import argparse
import re
import subprocess
import sys

NEED_RE = re.compile(r"\b(GLIBC|GLIBCXX)_(\d+(?:\.\d+)*)\s*$")


def version_key(text):
    return tuple(int(part) for part in text.split("."))


def max_version_needs(path):
    """Map each of GLIBC/GLIBCXX to the highest version `path` requires."""
    proc = subprocess.run(["objdump", "-p", path], capture_output=True, text=True, check=True)
    needs = {}
    in_refs = False
    for line in proc.stdout.splitlines():
        if line.startswith("Version References:"):
            in_refs = True
            continue
        if in_refs:
            if line and not line[0].isspace():
                break  # next top-level section header
            match = NEED_RE.search(line)
            if match:
                lib, ver = match.group(1), match.group(2)
                if lib not in needs or version_key(ver) > version_key(needs[lib]):
                    needs[lib] = ver
    return needs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--glibc", default="2.28", help="GLIBC ceiling [2.28]")
    parser.add_argument("--glibcxx", default="3.4.25", help="GLIBCXX ceiling [3.4.25]")
    parser.add_argument("files", nargs="+")
    args = parser.parse_args()
    ceilings = {"GLIBC": args.glibc, "GLIBCXX": args.glibcxx}

    failed = False
    for path in args.files:
        needs = max_version_needs(path)
        summary = ", ".join(
            f"max {lib} need {needs.get(lib, 'none')}" for lib in ("GLIBC", "GLIBCXX")
        )
        print(f"{path}: {summary}")
        for lib, ceiling in ceilings.items():
            if lib in needs and version_key(needs[lib]) > version_key(ceiling):
                print(
                    f"  ERROR: {path} requires {lib}_{needs[lib]}, "
                    f"above the {lib} {ceiling} ceiling",
                    file=sys.stderr,
                )
                failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
