"""Extra actual stock-bash descendants; original C04 setsid probe stays intact."""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path


def writer(path):
    path = Path(path)
    raw = Path("/proc/self/stat").read_text()
    start_ticks = int(raw[raw.rfind(")") + 2 :].split()[19])
    path.with_suffix(".identity.json").write_text(json.dumps({"pid": os.getpid(), "ppid": os.getppid(), "start_ticks": start_ticks, "uid": os.getuid(), "pid_namespace": os.readlink("/proc/self/ns/pid")}))
    with path.open("ab", buffering=0) as sink:
        end = time.monotonic() + 90
        while time.monotonic() < end:
            sink.write(b"actual-linux-tick\n")
            time.sleep(0.02)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--writer", action="store_true")
    parser.add_argument("--doublefork", action="store_true")
    args = parser.parse_args()
    if args.writer:
        if args.doublefork:
            if os.fork():
                return
            os.setsid()
            if os.fork():
                return
        writer(args.output)
        return
    # Preserve the original C04 child's real Linux identity without changing
    # its original tool probe source or background-survival behavior.
    role = Path(args.output).name.removesuffix("-extra")
    original = Path(args.output).parent / (role + "-probe.child.json")
    original_identity = json.loads(original.read_text())
    pid = original_identity["pid"]
    raw = Path("/proc", str(pid), "stat").read_text()
    tail = raw[raw.rfind(")") + 2 :].split()
    assert int(raw.split(" ", 1)[0]) == pid
    (Path(args.output).parent / (role + "-probe.identity.json")).write_text(json.dumps({**original_identity, "ppid": int(tail[1]), "start_ticks": int(tail[19]), "pid_namespace": os.readlink(f"/proc/{pid}/ns/pid")}))
    for mode in ("background", "doublefork"):
        path = args.output + "." + mode + ".ticks"
        child = subprocess.Popen(
            [sys.executable, "-m", "fleet.c08_linux_probe", "--writer", "--output", path, *(["--doublefork"] if mode == "doublefork" else [])], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True
        )
        deadline = time.monotonic() + 3
        while not Path(path).exists() or not Path(path).stat().st_size:
            if time.monotonic() >= deadline:
                raise RuntimeError("Actual Linux descendant did not acknowledge writing")
            time.sleep(0.01)
        if mode == "doublefork":
            child.wait(timeout=3)


if __name__ == "__main__":
    main()
