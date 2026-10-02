"""Run with python -I -S: harden before site, Fleet, harness or provider imports."""

import ctypes
import json
import os
import resource
import sys

MAX_BOOTSTRAP_BYTES = 1048576


def harden():
    if sys.platform != "linux":
        raise RuntimeError("Agent execution requires Linux process isolation")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(4, 0, 0, 0, 0) != 0 or libc.prctl(3, 0, 0, 0, 0) != 0:
        raise RuntimeError("Agent process isolation unavailable")
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    if resource.getrlimit(resource.RLIMIT_CORE) != (0, 0):
        raise RuntimeError("Agent core isolation unavailable")


def read_private_bootstrap():
    # The controller sends one bounded line and closes stdin. No credential has
    # ever entered argv/environment or a mounted configuration file.
    data = sys.stdin.buffer.readline(MAX_BOOTSTRAP_BYTES + 1)
    if not data.endswith(b"\n") or len(data) > MAX_BOOTSTRAP_BYTES:
        raise ValueError("Invalid private bootstrap length")
    os.close(0)
    fd = os.open(os.devnull, os.O_RDONLY)
    if fd != 0:
        os.dup2(fd, 0)
        os.close(fd)
    sys.stdin = open(0, closefd=False)
    return json.loads(data)


def main():
    try:
        harden()
        compatibility_only = "--compatibility" in sys.argv[1:]
        payload = None if compatibility_only else read_private_bootstrap()
        import site

        site.main()
        if compatibility_only:
            import argparse

            from deerflow_ecs_fleet.worker.agent_environment import worker_compatibility

            parser = argparse.ArgumentParser()
            parser.add_argument("--compatibility", action="store_true", required=True)
            parser.add_argument("--provider", required=True)
            arguments = parser.parse_args()
            print(worker_compatibility(arguments.provider).model_dump_json(), flush=True)
            return 0
        from deerflow_ecs_fleet.worker.agent_runner import bootstrap_main

        return bootstrap_main(payload, argv=sys.argv[1:])
    except BaseException:
        print("Agent trusted bootstrap failed", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
