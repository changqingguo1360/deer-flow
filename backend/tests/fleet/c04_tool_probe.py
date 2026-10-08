"""Executed by actual bash tools; probes the real same-UID process boundary."""

import argparse
import ctypes
import errno
import json
import os
import sys
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
parser.add_argument("--port", type=int, required=True)
args = parser.parse_args()
result = {"pid": os.getpid(), "ppid": os.getppid(), "host": __import__("socket").gethostname(), "uid": os.getuid()}
if sys.platform == "linux":
    result["cgroup"] = Path("/proc/self/cgroup").read_text()
    result["runner_cgroup"] = Path("/proc/1/cgroup").read_text()
    result["pid_namespace"] = os.readlink("/proc/self/ns/pid")
    for part in ("environ", "fd", "mem"):
        try:
            path = Path("/proc/1") / part
            path.iterdir().__next__() if part == "fd" else path.open("rb").close()
        except PermissionError:
            result[part] = "denied"
        else:
            result[part] = "readable"
    libc = ctypes.CDLL(None, use_errno=True)
    result["ptrace"] = "denied" if libc.ptrace(16, 1, None, None) == -1 and ctypes.get_errno() == errno.EPERM else "unexpected"
    import psycopg

    for password in (None, "wrong-c04-password"):
        try:
            psycopg.connect(host="host.docker.internal", port=args.port, user="fleet_c04", dbname="postgres", password=password, connect_timeout=2).close()
        except psycopg.OperationalError as error:
            authentication_rejected = error.sqlstate == "28P01" or "password authentication failed" in str(error) or "no password supplied" in str(error)
            result["db_without_password" if password is None else "db_wrong_password"] = "denied" if authentication_rejected else "probe_error"
        else:
            result["db_without_password" if password is None else "db_wrong_password"] = "authorized"
    result["raw_config_absent"] = not any(Path(path).exists() for path in ("/workspace/config.yaml", "/opt/deerflow/config.yaml", "/run/control-password"))
    result["control_env_absent"] = not any(key in os.environ for key in ("DATABASE_URL", "REDIS_URL", "FLEET_NODE_TOKEN", "FLEET_ATTEMPT_TOKEN", "PGPASSWORD"))
    import subprocess
    import time

    marker = str(Path(args.output).with_suffix(".ticks"))
    child_code = (
        "import os,sys,time; from pathlib import Path; p=Path(sys.argv[1]); "
        "p.with_suffix('.child.json').write_text(__import__('json').dumps("
        "{'pid':os.getpid(),'cgroup':Path('/proc/self/cgroup').read_text(),'uid':os.getuid()})); "
        "f=p.open('a'); end=time.monotonic()+60;\n"
        "while time.monotonic()<end: f.write('tick\\n'); f.flush(); time.sleep(.02)"
    )
    child = subprocess.Popen([sys.executable, "-c", child_code, marker], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True, start_new_session=True)
    deadline = time.monotonic() + 3
    while not Path(marker).exists() or not Path(marker).read_text().strip():
        if child.poll() is not None or time.monotonic() >= deadline:
            raise RuntimeError("Actual background child did not acknowledge first effect")
        time.sleep(0.01)
    result["background_pid"] = child.pid
Path(args.output).write_text(json.dumps(result, sort_keys=True))
