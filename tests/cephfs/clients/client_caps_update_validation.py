import json
import random
import shlex
import string
import time
import traceback
from threading import Event, Lock, Thread

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from tests.cephfs.cephfs_volume_management import wait_for_process
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.cephfs.snapshot_clone.cg_snap_utils import CG_Snap_Utils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def test_setup(fs_util, ceph_cluster, client):
    """
    This method is Setup to create test configuration - subvolumegroup,subvolumes,nfs servers
    """
    log.info("Create fs volume if doesn't exist")
    default_fs = "cephfs"
    cephfs_common_utils = CephFSCommonUtils(ceph_cluster)
    mds_nodes = ceph_cluster.get_ceph_objects("mds")
    fs_util.create_fs(client, default_fs)

    log.info("Configure/Verify 5 MDS for FS Volume, 2 Active, 3 Standyby")
    out, rc = client.exec_command(sudo=True, cmd=f"ceph fs get {default_fs}")
    out_list = out.split("\n")
    for line in out_list:
        if "max_mds" in line:
            str, max_mds_val = line.split()
            log.info("max_mds before test %s", max_mds_val)
    out, rc = client.exec_command(
        sudo=True, cmd=f"ceph fs status {default_fs} --format json"
    )
    output = json.loads(out)
    mds_cnt = len(output["mdsmap"])
    if mds_cnt < 5:
        cmd = f'ceph orch apply mds {default_fs} --placement="5'
        for mds_nodes_iter in mds_nodes:
            cmd += f" {mds_nodes_iter.node.hostname}"
        cmd += '"'
        log.info("Adding 5 MDS to cluster")
        out, rc = client.exec_command(sudo=True, cmd=cmd)
    client.exec_command(
        sudo=True,
        cmd=f"ceph fs set {default_fs} max_mds 2",
    )

    wait_time_secs = 300
    if cephfs_common_utils.wait_for_healthy_ceph(client, wait_time_secs):
        assert False, "Cluster health is not OK even after waiting for sometime"

    nfs_servers = ceph_cluster.get_ceph_objects("nfs")
    nfs_server = nfs_servers[0].node.hostname
    nfs_name = "cephfs-nfs"

    fs_util.create_nfs(
        client,
        nfs_cluster_name=nfs_name,
        nfs_server_name=nfs_server,
    )
    if wait_for_process(client=client, process_name=nfs_name, ispresent=True):
        log.info("ceph nfs cluster created successfully")
    else:
        raise CommandFailed("Failed to create nfs cluster")
    nfs_export_name = "/export_" + "".join(
        random.choice(string.digits) for i in range(3)
    )
    log.info(
        "Create subvolumegroup, Create subvolume in subvolumegroup and default grou"
    )
    subvolumegroup = {"vol_name": default_fs, "group_name": "subvolgroup_1"}
    fs_util.create_subvolumegroup(client, **subvolumegroup)
    sv_def = {
        "vol_name": default_fs,
        "subvol_name": "sv_def",
        "size": "5368706371",
    }
    fs_util.create_subvolume(client, **sv_def)

    sv_non_def = {
        "vol_name": default_fs,
        "subvol_name": "sv_non_def",
        "group_name": "subvolgroup_1",
        "size": "5368706371",
    }
    fs_util.create_subvolume(client, **sv_non_def)
    sv_list = [sv_def, sv_non_def]
    setup_params = {
        "default_fs": default_fs,
        "subvolumegroup": subvolumegroup,
        "sv_list": sv_list,
        "nfs_name": nfs_name,
        "nfs_export_name": nfs_export_name,
        "nfs_server": nfs_server,
        "max_mds_def": max_mds_val,
    }
    return setup_params


def normalize_path(path):
    """Strip trailing slash except for root."""
    if not path or path == "/":
        return path
    return path.rstrip("/")


def join_io_threads(write_procs, io_error_slots, timeout=600):
    """
    Join IO threads with a bound. io_error_slots[i] is a one-element list
    updated by the thread on failure. Return 0 if all finished cleanly, 1 if
    any are still alive after timeout or recorded an exception.
    """
    rc = 0
    for idx, proc in enumerate(write_procs):
        proc.join(timeout=timeout)
        if proc.is_alive():
            log.error("IO thread still running after %ss (index=%s)", timeout, idx)
            rc = 1
        err = None
        if idx < len(io_error_slots):
            err = io_error_slots[idx][0]
        if err is not None:
            log.error("IO thread index=%s failed: %s", idx, err)
            rc = 1
    return rc


def run_io_local(fs_util, mnt_info, sv_list, wait=True, join_timeout=600):
    """
    Warmup IO for Test2/Test3 via fs_util.run_ios (unchanged shared helper).

    Returns: (write_procs, io_error_slots)
    """
    write_procs = []
    io_error_slots = []
    rand_str = "".join(random.choice(string.digits) for i in range(3))
    for sv in sv_list:
        sv_name = sv["subvol_name"]
        mnt_path = mnt_info[sv_name]["path"]
        mnt_client = mnt_info[sv_name]["client"]
        io_path = f"{mnt_path}/run_ios_{rand_str}"
        mnt_client.exec_command(sudo=True, cmd=f"mkdir -p {io_path}")
        err_slot = [None]

        def _io_target(client=mnt_client, path=io_path, slot=err_slot):
            try:
                fs_util.run_ios(client, path, ["smallfile"])
            except Exception as exc:
                log.error("IO thread failed on %s: %s", path, exc)
                log.error(traceback.format_exc())
                slot[0] = exc

        proc = Thread(target=_io_target)
        io_error_slots.append(err_slot)
        proc.start()
        write_procs.append(proc)
    if wait:
        if join_io_threads(write_procs, io_error_slots, timeout=join_timeout) == 1:
            raise CommandFailed(
                "Background IO threads failed or timed out: "
                f"{[slot[0] for slot in io_error_slots]}"
            )
    return write_procs, io_error_slots


def _io_progress_snapshot(io_progress):
    """Copy of counters/in-flight map for logging (caller holds lock)."""
    return {
        "completions": io_progress["completions"],
        "in_flight": dict(io_progress["in_flight"]),
        "started": io_progress["started"],
        "finished": io_progress["finished"],
    }


def log_io_progress(prefix, io_progress, lock):
    """Log create-loop counters; grep logs for [quiesce-io]."""
    with lock:
        snap = _io_progress_snapshot(io_progress)
    log.info(
        "[quiesce-io] %s completions=%s started=%s finished=%s in_flight=%s",
        prefix,
        snap["completions"],
        snap["started"],
        snap["finished"],
        snap["in_flight"],
    )
    return snap


def start_quiesce_io(mnt_info, sv_list):
    """
    Test1-only: background smallfile *create* loops (local to this module).

    Each successful create completion increments an in-process counter. Under
    CG quiesce, create exec_command blocks, so the counter must not rise.
    Does not call cephfs_utilsV1.run_ios and does not find() on CephFS.
    """
    write_procs = []
    io_error_slots = []
    # in_flight[tag] = batch seq currently inside exec_command, or None
    io_progress = {
        "completions": 0,
        "started": 0,
        "finished": 0,
        "in_flight": {},
    }
    lock = Lock()
    stop_event = Event()
    rand_str = "".join(random.choice(string.digits) for _ in range(3))
    t0 = time.monotonic()

    for sv in sv_list:
        sv_name = sv["subvol_name"]
        mnt_path = mnt_info[sv_name]["path"]
        mnt_client = mnt_info[sv_name]["client"]
        io_path = f"{mnt_path}/run_ios_{rand_str}"
        err_slot = [None]
        with lock:
            io_progress["in_flight"][sv_name] = None

        def _create_loop(
            client=mnt_client,
            path=io_path,
            slot=err_slot,
            tag=sv_name,
        ):
            sync_dir = f"/var/tmp/smallfile_caps_{rand_str}_{tag}"
            try:
                client.exec_command(
                    sudo=True,
                    cmd=f"mkdir -p {shlex.quote(path)} {shlex.quote(sync_dir)}",
                )
                while not stop_event.is_set():
                    cmd = (
                        "python3 /home/cephuser/smallfile/smallfile_cli.py "
                        "--operation create --threads 4 --file-size 1024 "
                        f"--files 5 --top {shlex.quote(path)} "
                        f"--network-sync-dir {shlex.quote(sync_dir)}"
                    )
                    with lock:
                        io_progress["started"] += 1
                        batch = io_progress["started"]
                        io_progress["in_flight"][tag] = batch
                    log.info(
                        "[quiesce-io] CREATE_START sv=%s batch=%s path=%s "
                        "t+%.3fs stop=%s",
                        tag,
                        batch,
                        path,
                        time.monotonic() - t0,
                        stop_event.is_set(),
                    )
                    start_mono = time.monotonic()
                    try:
                        client.exec_command(sudo=True, cmd=cmd, timeout=600)
                    except Exception as exc:
                        elapsed = time.monotonic() - start_mono
                        with lock:
                            io_progress["in_flight"][tag] = None
                        if stop_event.is_set():
                            log.info(
                                "[quiesce-io] CREATE_ABORT sv=%s batch=%s "
                                "after %.3fs (stop set): %s",
                                tag,
                                batch,
                                elapsed,
                                exc,
                            )
                            break
                        log.error(
                            "[quiesce-io] CREATE_FAIL sv=%s batch=%s after %.3fs: %s",
                            tag,
                            batch,
                            elapsed,
                            exc,
                        )
                        log.error(traceback.format_exc())
                        slot[0] = exc
                        break
                    elapsed = time.monotonic() - start_mono
                    with lock:
                        io_progress["finished"] += 1
                        io_progress["completions"] += 1
                        io_progress["in_flight"][tag] = None
                        completions = io_progress["completions"]
                    log.info(
                        "[quiesce-io] CREATE_DONE sv=%s batch=%s path=%s "
                        "took=%.3fs completions=%s t+%.3fs stop=%s",
                        tag,
                        batch,
                        path,
                        elapsed,
                        completions,
                        time.monotonic() - t0,
                        stop_event.is_set(),
                    )
            finally:
                with lock:
                    io_progress["in_flight"][tag] = None
                log.info(
                    "[quiesce-io] writer exit sv=%s t+%.3fs",
                    tag,
                    time.monotonic() - t0,
                )
                try:
                    client.exec_command(
                        sudo=True,
                        cmd=f"rm -rf {shlex.quote(sync_dir)}",
                        check_ec=False,
                    )
                except Exception:
                    pass

        proc = Thread(target=_create_loop, name=f"quiesce-io-{sv_name}")
        io_error_slots.append(err_slot)
        proc.start()
        write_procs.append(proc)
        log.info(
            "[quiesce-io] started writer thread=%s sv=%s path=%s",
            proc.name,
            sv_name,
            io_path,
        )

    return write_procs, io_error_slots, io_progress, lock, stop_event


def verify_io_completions_paused(io_progress, lock, duration=20, interval=5):
    """
    While QUIESCED, successful writer completions must not increase.

    Uses the in-process counter from start_quiesce_io (no CephFS find/ls).
    Returns True if paused, False if completions increased.
    """
    baseline_snap = log_io_progress("baseline under QUIESCED", io_progress, lock)
    baseline = baseline_snap["completions"]

    elapsed = 0
    while elapsed < duration:
        time.sleep(interval)
        elapsed += interval
        snap = log_io_progress(f"pause sample after {elapsed}s", io_progress, lock)
        current = snap["completions"]
        if current > baseline:
            log.error(
                "[quiesce-io] completions rose under QUIESCED: %s -> %s "
                "(in_flight=%s); new CREATE_DONE after baseline",
                baseline,
                current,
                snap["in_flight"],
            )
            return False
    log.info(
        "[quiesce-io] no completion progress during settle window "
        "(baseline=%s in_flight=%s)",
        baseline,
        baseline_snap["in_flight"],
    )
    return True


def get_quiesce_set_state(cg_snap_util, client, qs_id_val, fs_name):
    """Return quiesce set state name, or None if query fails."""
    try:
        qs_query = cg_snap_util.get_qs_query(client, qs_id_val, fs_name=fs_name)
        return qs_query["sets"][qs_id_val]["state"]["name"]
    except Exception as exc:
        log.warning("Failed to query quiesce set %s: %s", qs_id_val, exc)
        return None


def safe_quiesce_release(cg_snap_util, client, qs_id_val, fs_name):
    """Release quiesce set; log and continue if already expired/released."""
    try:
        cg_snap_util.cg_quiesce_release(
            client, qs_id_val, fs_name=fs_name, if_await=True
        )
    except CommandFailed as exc:
        log.warning(
            "Quiesce release of %s failed (set may have expired): %s", qs_id_val, exc
        )


def find_client_session(client, fs_util, fs_name, subvol_path, mnt_client):
    """
    Find MDS client session by exact subvol root path + mount client hostname.
    Searches all active MDS. Returns dict with id, num_caps, mds — or None if
    missing or ambiguous (more than one distinct client id).

    With max_mds>1 the same client id often appears on multiple MDS ranks with
    different num_caps; keep the rank that holds the most caps for that id.
    """
    expect_root = normalize_path(subvol_path)
    expect_host = mnt_client.node.hostname
    matches = []
    mds_list = fs_util.get_active_mdss(client, fs_name=fs_name)
    retry_exec_command = retry(CommandFailed, tries=3, delay=10)(client.exec_command)

    for mds_name in mds_list:
        out, _ = retry_exec_command(
            sudo=True, cmd=f"ceph tell mds.{mds_name} client ls --f json"
        )
        client_ls = json.loads(out)
        for entry in client_ls:
            meta = entry.get("client_metadata") or {}
            root = normalize_path(meta.get("root", ""))
            hostname = meta.get("hostname", "")
            if root == expect_root and root != "/" and hostname == expect_host:
                log.info(
                    "Matched client session id=%s mds=%s root=%s hostname=%s num_caps=%s",
                    entry["id"],
                    mds_name,
                    root,
                    hostname,
                    entry["num_caps"],
                )
                matches.append(
                    {
                        "id": entry["id"],
                        "num_caps": entry["num_caps"],
                        "mds": mds_name,
                    }
                )

    if len(matches) == 0:
        log.error(
            "No client session found for root=%s hostname=%s", expect_root, expect_host
        )
        return None

    # Same session id on multiple MDS: pick the rank with the most caps.
    by_id = {}
    for match in matches:
        cid = match["id"]
        if cid not in by_id or match["num_caps"] > by_id[cid]["num_caps"]:
            by_id[cid] = match
    unique = list(by_id.values())

    if len(unique) > 1:
        log.error(
            "Ambiguous client sessions for root=%s hostname=%s: %s",
            expect_root,
            expect_host,
            unique,
        )
        return None

    chosen = unique[0]
    if len(matches) > 1:
        log.info(
            "Session id=%s seen on %s MDS entries; using mds=%s num_caps=%s",
            chosen["id"],
            len(matches),
            chosen["mds"],
            chosen["num_caps"],
        )
    return chosen


def client_session_exists(client, fs_util, fs_name, client_id):
    """Return True if client_id is present on any active MDS."""
    mds_list = fs_util.get_active_mdss(client, fs_name=fs_name)
    retry_exec_command = retry(CommandFailed, tries=3, delay=10)(client.exec_command)
    for mds_name in mds_list:
        out, _ = retry_exec_command(
            sudo=True, cmd=f"ceph tell mds.{mds_name} client ls --f json"
        )
        client_ls = json.loads(out)
        for entry in client_ls:
            if entry.get("id") == client_id:
                return True
    return False


def client_caps_quiesce_test(
    client,
    subvol_path,
    fs_name,
    qs_set,
    cg_snap_util,
    fs_util,
    mnt_client,
    io_threads,
    io_progress,
    io_lock,
):
    """
    Test1: CG quiesce — pass on QUIESCED + no writer completions.

    Intentional change: aggregate client ls num_caps is not a pass/fail check.
    Quiesce revokes write caps only; num_caps need not decrease (MDS/dev).
    Logged before/after for visibility only.
    """
    session = find_client_session(client, fs_util, fs_name, subvol_path, mnt_client)
    if not session:
        log.error("Failed to locate unique client session before quiesce")
        return 1

    caps_before_quiesce = session["num_caps"]
    client_id = session["id"]
    log.info(
        "Before quiesce: client_id=%s, num_caps=%s", client_id, caps_before_quiesce
    )

    log.info("CG quiesce on members:%s", qs_set)
    log_io_progress("before cg_quiesce --await", io_progress, io_lock)
    rand_str = "".join(random.choice(string.digits) for i in range(3))
    qs_id_val = f"caps_test_{rand_str}"
    qs_out = cg_snap_util.cg_quiesce(
        client, qs_set, qs_id=qs_id_val, fs_name=fs_name, timeout=600, expiration=600
    )
    if qs_out == 1:
        log.error("CG quiesce command failed validation")
        return 1

    # Always release once quiesce succeeded (covers early returns and exceptions).
    try:
        log_io_progress("right after cg_quiesce returned", io_progress, io_lock)
        qs_state = get_quiesce_set_state(cg_snap_util, client, qs_id_val, fs_name)
        log.info("Quiesce set %s state after await: %s", qs_id_val, qs_state)
        if qs_state != "QUIESCED":
            log.error("Expected QUIESCED, got %s", qs_state)
            return 1

        if not any(t.is_alive() for t in io_threads):
            log.error(
                "IO threads finished before quiesce validation; cannot verify pause"
            )
            return 1
        for idx, thr in enumerate(io_threads):
            log.info(
                "[quiesce-io] thread[%s] name=%s alive=%s",
                idx,
                thr.name,
                thr.is_alive(),
            )

        # Keep settle well inside quiesce expiration (default 600s).
        pause_ok = verify_io_completions_paused(
            io_progress, io_lock, duration=20, interval=5
        )
        qs_state_after = get_quiesce_set_state(cg_snap_util, client, qs_id_val, fs_name)
        log.info(
            "Quiesce set %s state after IO pause check: %s", qs_id_val, qs_state_after
        )
        log_io_progress("after pause check / before release", io_progress, io_lock)

        if not pause_ok:
            if qs_state_after != "QUIESCED":
                log.error(
                    "IO completions rose after quiesce left QUIESCED "
                    "(state=%s); pause check unreliable past expiration",
                    qs_state_after,
                )
            else:
                log.error("IO did not pause while quiesce set was QUIESCED")
            return 1
        if qs_state_after != "QUIESCED":
            log.error(
                "Quiesce set left QUIESCED during IO pause check (state=%s)",
                qs_state_after,
            )
            return 1

        # num_caps drop not asserted under quiesce: write-only revoke (MDS/dev).
        session_after = find_client_session(
            client, fs_util, fs_name, subvol_path, mnt_client
        )
        if not session_after:
            log.warning(
                "Could not re-locate client session after quiesce for num_caps logging"
            )
        else:
            caps_after_quiesce = session_after["num_caps"]
            log.info(
                "num_caps before quiesce: %s, after QUIESCED+IO pause: %s "
                "(informational; write-only revoke, drop not required)",
                caps_before_quiesce,
                caps_after_quiesce,
            )
        return 0
    finally:
        log_io_progress("entering release finally", io_progress, io_lock)
        safe_quiesce_release(cg_snap_util, client, qs_id_val, fs_name)
        log_io_progress("after release finally", io_progress, io_lock)


def client_caps_mds_failover_test(
    client, fs_name, cephfs_common_utils, fs_util, subvol_path, mnt_client
):
    """
    This method is for Test2: Client caps validation during MDS failover
    """
    log.info("Capture Caps info before mds failover")
    session_before = find_client_session(
        client, fs_util, fs_name, subvol_path, mnt_client
    )
    if not session_before:
        log.error("Failed to locate unique client session before MDS failover")
        return 1
    caps_before_mds_fail = session_before["num_caps"]
    log.info(
        "Before MDS fail: client_id=%s, num_caps=%s",
        session_before["id"],
        caps_before_mds_fail,
    )

    log.info("Perform MDS failover")
    if mds_failover(cephfs_common_utils, client, fs_name):
        log.error("MDS failover helper failed")
        return 1

    log.info("Capture Caps info after mds failover")
    session_after = None
    for attempt in range(3):
        session_after = find_client_session(
            client, fs_util, fs_name, subvol_path, mnt_client
        )
        if session_after:
            break
        log.info("Client session not found after failover, retry %s/3", attempt + 1)
        time.sleep(30)

    if not session_after:
        log.error("Failed to locate client session after MDS failover")
        return 1

    caps_after_mds_fail = session_after["num_caps"]
    log.info(
        "Client Caps Before MDS fail:%s,Clients Caps After MDS fail:%s (client_id after=%s)",
        caps_before_mds_fail,
        caps_after_mds_fail,
        session_after["id"],
    )
    if caps_after_mds_fail >= caps_before_mds_fail:
        log.error("Clients caps are not updated during MDS failover")
        return 1
    return 0


def client_caps_evict_test(
    client, subvol_path, mnt_client, mnt_pt, fs_name, fs_util, mnt_info, sv_name
):
    """
    Test3: Client session removed after eviction.

    Remount is cleanup/recovery only (not the pass/fail). New mount path is
    written back to mnt_info so finally umounts the live mount.
    """
    session = find_client_session(client, fs_util, fs_name, subvol_path, mnt_client)
    if not session:
        log.error("Failed to locate unique client session before eviction")
        return 1

    client_id = session["id"]
    mds_name = session["mds"]
    caps_before_evict = session["num_caps"]
    log.info(
        "Evicting Client with ID : %s on mds.%s (num_caps=%s)",
        client_id,
        mds_name,
        caps_before_evict,
    )
    cmd = f"ceph tell mds.{mds_name} client evict id={client_id}"
    client.exec_command(sudo=True, cmd=cmd, timeout=120)

    session_gone = False
    for attempt in range(12):
        if not client_session_exists(client, fs_util, fs_name, client_id):
            session_gone = True
            break
        log.info(
            "Session %s still present after evict, attempt %s/12",
            client_id,
            attempt + 1,
        )
        time.sleep(5)
    if not session_gone:
        log.error("Client session id=%s still present after eviction", client_id)
        return 1
    log.info(
        "Client session id=%s removed after eviction (caps before evict were %s)",
        client_id,
        caps_before_evict,
    )

    log.info("Unmount and Remount %s as Client session was evicted", subvol_path)
    mnt_client.exec_command(sudo=True, cmd=f"umount -l {mnt_pt}", check_ec=False)
    mnt_params = {
        "mnt_path": subvol_path,
        "client": mnt_client,
        "fs_util": fs_util,
        "fs_name": fs_name,
        "export_created": 0,
    }
    new_path, _ = fs_util.mount_ceph("fuse", mnt_params)
    mnt_info[sv_name]["path"] = new_path
    log.info("Updated mnt_info[%s] path after remount: %s", sv_name, new_path)
    return 0


def mds_failover(cephfs_common_utils, client, fs_name):
    """
    Helper routine to perform Rank 0 MDS failover of given FS
    """
    out, rc = client.exec_command(
        sudo=True, cmd=f"ceph fs status {fs_name} --format json"
    )
    output = json.loads(out)
    active_mds_list = [
        mds["name"]
        for mds in output["mdsmap"]
        if (mds["state"] == "active") and (mds["rank"] == 0)
    ]
    active_mds = active_mds_list[0]
    log.info("FS status before MDS failover:%s", output)
    out, rc = client.exec_command(cmd=f"ceph mds fail {active_mds}", client_exec=True)
    log.info(out)
    time.sleep(5)
    log.info("Waiting for atleast 2 active MDS after failing Rank 0 MDS")
    if wait_for_two_active_mds(client, fs_name):
        log.error("Wait for 2 active MDS failed")
        return 1
    wait_time_secs = 300
    if cephfs_common_utils.wait_for_healthy_ceph(client, wait_time_secs):
        log.error("Cluster health is not OK even after waiting for sometime")
        return 1
    return 0


def wait_for_two_active_mds(client, fs_name, max_wait_time=180, retry_interval=10):
    """
    Wait until two active MDS (Metadata Servers) are found or the maximum wait time is reached.
    Args:
        max_wait_time (int): Maximum wait time in seconds (default: 180 seconds).
        retry_interval (int): Interval between retry attempts in seconds (default: 5 seconds).
    Returns:
        int: 0 if two active MDS are found within the specified time, 1 if not.
    """
    start_time = time.time()
    while time.time() - start_time < max_wait_time:
        out, rc = client.exec_command(
            cmd=f"ceph fs status {fs_name} -f json", client_exec=True
        )
        log.info(out)
        parsed_data = json.loads(out)
        active_mds = [
            mds
            for mds in parsed_data.get("mdsmap", [])
            if mds.get("rank", -1) in [0, 1] and mds.get("state") == "active"
        ]
        if len(active_mds) == 2:
            return 0  # Two active MDS found
        else:
            time.sleep(retry_interval)  # Retry after the specified interval

    return 1


def run(ceph_cluster, **kw):
    """
    CEPH-83597462 - CG quiesce IO freeze, MDS failover caps, and client eviction.

    Test Steps:
    1. CG quiesce: set reaches QUIESCED and writer completions pause
       (aggregate num_caps logged only; write-only revoke, drop not required)
    2. MDS failover: client session num_caps decreases after failover
    3. Evict: client session id is removed from MDS client ls

    """
    mnt_info = {}
    sv_list = []
    fs_name = "cephfs"
    group_name = None
    client = None
    fs_util_v1 = None
    cephfs_common_utils = None

    try:
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        cg_snap_util = CG_Snap_Utils(ceph_cluster)
        cephfs_common_utils = CephFSCommonUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        config = kw.get("config")
        osp_cred = config.get("osp_cred")
        build = config.get("build", config.get("rhbuild"))

        if config.get("cloud-type") == "openstack":
            os_cred = osp_cred.get("globals").get("openstack-credentials")
            params = {}
            params["username"] = os_cred["username"]
            params["password"] = os_cred["password"]
            params["auth_url"] = os_cred["auth-url"]
            params["auth_version"] = os_cred["auth-version"]
            params["tenant_name"] = os_cred["tenant-name"]
            params["service_region"] = os_cred["service-region"]
            params["domain_name"] = os_cred["domain"]
            params["tenant_domain_id"] = os_cred["tenant-domain-id"]
            params["cloud_type"] = "openstack"
        elif config.get("cloud-type") == "ibmc":
            pass
        else:
            pass

        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)
        client = clients[0]

        mon_node_ip = fs_util_v1.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)

        crash_status_before = fs_util_v1.get_crash_ls_new(client)
        log.info("Crash status before Test: %s", crash_status_before)

        log.info("Setup test configuration")
        setup_params = test_setup(fs_util_v1, ceph_cluster, client)
        fs_name = setup_params["default_fs"]
        sv_list = setup_params["sv_list"]
        group_name = setup_params["subvolumegroup"]["group_name"]
        qs_set = []
        for sv in sv_list:
            if sv.get("group_name"):
                qs_member = f"{sv['group_name']}/{sv['subvol_name']}"
            else:
                qs_member = f"{sv['subvol_name']}"
            qs_set.append(qs_member)
        mnt_params = {
            "client": clients[1],
            "fs_util": fs_util_v1,
            "fs_name": fs_name,
            "export_created": 0,
        }
        nfs_params = {
            "nfs_name": setup_params["nfs_name"],
            "nfs_export_name": setup_params["nfs_export_name"],
            "nfs_server": setup_params["nfs_server"],
        }
        sv_path_dict = {}
        sv_name_list = []
        for sv in sv_list:
            mnt_type = random.choice(["fuse", "kernel"])
            if mnt_type == "nfs":
                mnt_params.update(nfs_params)
            cmd = f"ceph fs subvolume getpath {fs_name} {sv['subvol_name']}"
            if sv.get("group_name"):
                cmd += f" {sv['group_name']}"
            out, rc = client.exec_command(
                sudo=True,
                cmd=cmd,
            )
            sv_path = out.strip()
            sv_path_dict.update({sv["subvol_name"]: sv_path})
            sv_name_list.append(sv["subvol_name"])
            mnt_client = random.choice(clients)
            mnt_params.update({"mnt_path": sv_path, "client": mnt_client})
            path, _ = fs_util_v1.mount_ceph(mnt_type, mnt_params)

            mnt_info.update({sv["subvol_name"]: {"path": path, "client": mnt_client}})

        test_fail = 0

        log.info("Test1: CG quiesce reaches QUIESCED and writer completions pause")
        (
            io_threads,
            io_error_slots,
            io_progress,
            io_lock,
            io_stop,
        ) = start_quiesce_io(mnt_info, sv_list)
        time.sleep(5)
        log_io_progress("after 5s warmup, before Test1 quiesce", io_progress, io_lock)

        sv_name = random.choice(sv_name_list)
        subvol_path = sv_path_dict[sv_name]
        mnt_client = mnt_info[sv_name]["client"]
        if (
            client_caps_quiesce_test(
                client,
                subvol_path,
                fs_name,
                qs_set,
                cg_snap_util,
                fs_util_v1,
                mnt_client,
                io_threads,
                io_progress,
                io_lock,
            )
            == 1
        ):
            test_fail += 1

        log_io_progress("before stop_event (post release)", io_progress, io_lock)
        io_stop.set()
        log.info("[quiesce-io] stop_event set")
        # After release, blocked creates should finish; then threads exit.
        if join_io_threads(io_threads, io_error_slots, timeout=300) == 1:
            log.error("Test1 background IO threads failed or did not finish")
            test_fail += 1
        log_io_progress("after Test1 IO join", io_progress, io_lock)

        run_io_local(fs_util_v1, mnt_info, sv_list)

        log.info("Test2:Check if client caps are recalled when MDS failover happens")
        sv_name = random.choice(sv_name_list)
        subvol_path = sv_path_dict[sv_name]
        mnt_client = mnt_info[sv_name]["client"]
        if (
            client_caps_mds_failover_test(
                client,
                fs_name,
                cephfs_common_utils,
                fs_util_v1,
                subvol_path,
                mnt_client,
            )
            == 1
        ):
            test_fail += 1

        run_io_local(fs_util_v1, mnt_info, sv_list)

        log.info(
            "Test3:Check if client caps are recalled when client session is evicted"
        )
        sv_name = random.choice(sv_name_list)
        subvol_path = sv_path_dict[sv_name]
        mnt_pt = mnt_info[sv_name]["path"]
        mnt_client = mnt_info[sv_name]["client"]
        if (
            client_caps_evict_test(
                client,
                subvol_path,
                mnt_client,
                mnt_pt,
                fs_name,
                fs_util_v1,
                mnt_info,
                sv_name,
            )
            == 1
        ):
            test_fail += 1

        if test_fail > 0:
            log.error("FAIL:Client Caps Validation Test Failed")
            return 1
        mount_summary = {name: info["path"] for name, info in mnt_info.items()}
        log.info(
            "PASS: quiesce IO freeze + MDS failover caps + evict session gone | mounts=%s",
            mount_summary,
        )
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up the system")
        if client is None or fs_util_v1 is None or cephfs_common_utils is None:
            log.error("Skipping cleanup; setup did not complete")
        else:
            crash_status_after = fs_util_v1.get_crash_ls_new(client)
            log.info("Crash status after Test: %s", crash_status_after)
            wait_time_secs = 300
            if cephfs_common_utils.wait_for_healthy_ceph(client, wait_time_secs):
                log.error("Cluster health is not OK even after waiting for sometime")
            else:
                log.info("Cluster Health is OK")
            for sv in mnt_info:
                mnt_pt = mnt_info[sv]["path"]
                client_tmp = mnt_info[sv]["client"]
                try:
                    out, rc = client_tmp.exec_command(
                        sudo=True,
                        cmd=f"umount -l {mnt_pt}",
                    )
                except BaseException as ex:
                    log.info(ex)
                    if "not mounted" not in str(ex):
                        log.error("Unmount failed with unexpected reason")
            for sv in sv_list:
                subvol_name = sv["subvol_name"]
                fs_util_v1.remove_subvolume(
                    client,
                    fs_name,
                    subvol_name,
                    validate=True,
                    group_name=sv.get("group_name", None),
                )
            if group_name:
                fs_util_v1.remove_subvolumegroup(
                    client, fs_name, group_name, validate=True
                )
