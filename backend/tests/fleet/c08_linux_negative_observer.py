"""Host-only actual installed collector negative observations, with real owners."""

import asyncio
import copy
import json
import time

from deerflow_ecs_fleet.worker.containers import DockerError
from deerflow_ecs_fleet.worker.workspace_collector import COLLECTOR_PATH


async def observe_containment_negatives(driver, grant, output, request):
    cid = (await driver.inspect(grant["process_ref"]))["Id"]
    workload = grant["execution_profile"]["user"]
    unknown_script = (
        "import os,json,pathlib,sys; s=pathlib.Path('/proc/self/stat').read_text(); "
        "print(json.dumps({'pid':os.getpid(),'start_ticks':int(s[s.rfind(')')+2:].split()[19]),"
        "'uid':os.getuid(),'pid_namespace':os.readlink('/proc/self/ns/pid')}),flush=True); sys.stdin.buffer.read()"
    )
    unknown = await asyncio.create_subprocess_exec(
        driver.executable, "exec", "-i", "--user", workload, cid, "python", "-I", "-S", "-c", unknown_script, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    helper = None
    try:
        unknown_identity = json.loads(await asyncio.wait_for(unknown.stdout.readline(), 5))
        try:
            await driver.quiesce_workspace(grant, output_dir=output, request=request, deadline=time.monotonic() + 10)
        except DockerError as error:
            unknown_rejection = str(error)
        else:
            raise AssertionError("Actual unknown child was accepted")
        assert unknown.returncode is None
        unknown.stdin.close()
        await asyncio.wait_for(unknown.wait(), 5)
        assert unknown.returncode == 0

        # The original fixed readonly collector is born with another UID and
        # blocks on its private stdin. The workload never receives that pipe.
        helper_user = "0:0" if int(workload.split(":", 1)[0]) else "65534:65534"
        helper = await asyncio.create_subprocess_exec(
            driver.executable, "exec", "-i", "--user", helper_user, cid, "python", "-I", "-S", COLLECTOR_PATH, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        probe = """import os,json,pathlib,ctypes,time
candidates=[]
for p in pathlib.Path('/proc').iterdir():
 if p.name.isdecimal() and int(p.name) not in (1,os.getpid()):
  try:
   uid=int(next(s for s in (p/'status').read_text().splitlines() if s.startswith('Uid:')).split()[1])
   if uid!=os.getuid(): candidates.append(int(p.name))
  except FileNotFoundError: pass
assert len(candidates)==1,candidates
pid=candidates[0]; result={'helper_pid':pid,'workload_uid':os.getuid(),'pid_namespace':os.readlink('/proc/self/ns/pid')}
for name,path,mode in [('private_fd_read',f'/proc/{pid}/fd/0',os.O_RDONLY),('private_fd_write',f'/proc/{pid}/fd/0',os.O_WRONLY),('memory',f'/proc/{pid}/mem',os.O_RDONLY)]:
 try:
  fd=os.open(path,mode);os.close(fd);result[name]='ALLOWED'
 except PermissionError: result[name]='denied'
lib=ctypes.CDLL(None,use_errno=True); result['ptrace']='denied' if lib.ptrace(16,pid,0,0)==-1 and ctypes.get_errno()==1 else 'ALLOWED'
print(json.dumps(result))
"""
        assert "\x00" not in probe
        compile(probe, "trusted-negative-observer", "exec")
        await asyncio.sleep(0.1)
        result = json.loads(await driver.checked("exec", "--user", workload, cid, "python", "-I", "-S", "-c", probe))
        assert all(result[key] == "denied" for key in ("private_fd_read", "private_fd_write", "memory", "ptrace"))
        assert helper.returncode is None
        helper.stdin.write(b"{}\n")
        await helper.stdin.drain()
        helper.stdin.close()
        helper_stdout, helper_stderr = await asyncio.wait_for(helper.communicate(), 5)
        assert helper.returncode != 0 and not helper_stdout
        return {
            "original_container_id": cid,
            "unknown_identity": unknown_identity,
            "unknown_rejection": unknown_rejection,
            "unknown_alive_after_rejection": True,
            "unknown_original_stdin_eof_reaped": True,
            "helper_user": helper_user,
            "private_access_probe": result,
            "invalid_receipt_input_exit": helper.returncode,
            "invalid_receipt_input_stderr": helper_stderr.decode(errors="replace"),
        }
    finally:
        for owner in (unknown, helper):
            if owner is not None and owner.returncode is None:
                owner.stdin.close()
                await asyncio.wait_for(owner.wait(), 10)


async def observe_frozen_profile_negatives(driver, grant, output, request):
    actual = await driver.inspect(grant["process_ref"])
    results = []
    for field, value in (
        ("user", "502:502" if grant["execution_profile"]["user"] != "502:502" else "503:503"),
        ("memory_mib", grant["execution_profile"]["memory_mib"] + 1),
        ("pids_limit", grant["execution_profile"]["pids_limit"] + 1),
    ):
        changed = copy.deepcopy(grant)
        changed["execution_profile"][field] = value
        try:
            await driver.quiesce_workspace(changed, output_dir=output, request=request, deadline=time.monotonic() + 5)
        except DockerError as error:
            results.append({"field": field, "rejection": str(error)})
        else:
            raise AssertionError("Actual immutable installed profile mismatch was accepted")
    after = await driver.inspect(grant["process_ref"])
    assert after["Id"] == actual["Id"] and after["State"]["StartedAt"] == actual["State"]["StartedAt"] and after["State"]["Running"]
    return {
        "container_id": actual["Id"],
        "started_at": actual["State"]["StartedAt"],
        "actual_user": actual["Config"]["User"],
        "actual_memory": actual["HostConfig"]["Memory"],
        "actual_pids_limit": actual["HostConfig"]["PidsLimit"],
        "actual_pid_mode": actual["HostConfig"]["PidMode"],
        "actual_namespace_mode": actual["HostConfig"]["CgroupnsMode"],
        "mismatches": results,
        "original_container_unchanged_alive": True,
    }


async def observe_namespace_uid_escape(driver, grant, output, request):
    cid = (await driver.inspect(grant["process_ref"]))["Id"]
    probe = """import os,json,ctypes
lib=ctypes.CDLL(None,use_errno=True)
before={'uid':os.getuid(),'namespace':os.readlink('/proc/self/ns/pid')}
result={}
for name,operation in [('unshare_new_pidns',lambda:lib.unshare(0x20000000)),('join_pidns',lambda:lib.setns(os.open('/proc/self/ns/pid',os.O_RDONLY),0))]:
 ctypes.set_errno(0);code=operation();result[name]={'code':code,'errno':ctypes.get_errno()}
try:
 os.setuid(0 if os.getuid() else 65534);result['setuid']='ALLOWED'
except PermissionError: result['setuid']='denied'
print(json.dumps({'before':before,'after':{'uid':os.getuid(),'namespace':os.readlink('/proc/self/ns/pid')},'operations':result}))
"""
    compile(probe, "actual-namespace-escape", "exec")
    result = json.loads(await driver.checked("exec", "--user", grant["execution_profile"]["user"], cid, "python", "-I", "-S", "-c", probe))
    assert result["before"] == result["after"]
    assert result["operations"]["setuid"] == "denied"
    assert all(result["operations"][name] == {"code": -1, "errno": 1} for name in ("unshare_new_pidns", "join_pidns"))
    return result
