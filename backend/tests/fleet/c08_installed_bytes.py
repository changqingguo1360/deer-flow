"""Compare installed payload bytes inside each actual C08 Runner process."""

import hashlib
import json
from pathlib import Path


def verify_members(site_packages, inventory):
    root = Path(site_packages)
    count = 0
    for wheel in inventory.values():
        for member, expected in wheel["members"].items():
            # pip rewrites RECORD to describe actual installed locations.
            # Every other wheel payload/metadata byte is immutable here.
            if member.endswith(".dist-info/RECORD"):
                continue
            parts = Path(member).parts
            if any(part in {"..", "."} for part in parts) or Path(member).is_absolute():
                raise ValueError("Invalid frozen wheel member")
            path = root / member
            if ".data" in parts[0]:
                if len(parts) < 3 or parts[1] not in {"purelib", "platlib"}:
                    raise ValueError("Unsupported installed wheel data member")
                path = root.joinpath(*parts[2:])
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                raise ValueError("Installed wheel member differs: " + member)
            count += 1
    return count


def verify_installed():
    import sysconfig

    from fleet.c08_linux_fixture import receipt

    inventory_path = Path("/opt/deerflow/wheel-inventory.json")
    inventory = json.loads(inventory_path.read_bytes())
    if len(inventory) != 6:
        raise ValueError("Required six-wheel installed inventory is missing")
    count = verify_members(sysconfig.get_path("purelib"), inventory)
    receipt("original-installed-complete-wheel-bytes", inventory_sha256=hashlib.sha256(inventory_path.read_bytes()).hexdigest(), wheel_count=len(inventory), compared_members=count)
