import json
import random
import string
import time
import traceback
from threading import Thread

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
            log.info(f"max_mds before test {max_mds_val}")
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


def run_io_local(fs_util, mnt_info, sv_list, wait=True):
    """
    This methods runs IOs in parallel on given mountpoints.
    mnt_info : dict type input, with below format,
    {
    sv_name:{'path' : path,'client':client}
    }
    If wait=False, returns the list of running threads without joining.
    Returns: (write_procs, io_targets) where io_targets is a list of
    {'client': client, 'path': io_path} for progress checks.
    """
    write_procs = []
    io_targets = []
    rand_str = "".join(random.choice(string.digits) for i in range(3))
    for sv in sv_list:
        sv_name = sv["subvol_name"]
        mnt_path = mnt_info[sv_name]["path"]
        mnt_client = mnt_info[sv_name]["client"]
        io_tools = ["smallfile"]
        io_path = f"{mnt_path}/run_ios_{rand_str}"
        mnt_client.exec_command(sudo=True, cmd=f"mkdir -p {io_path}")
        io_targets.append({"client": mnt_client, "path": io_path})
        p = Thread(target=fs_util.run_ios, args=(mnt_client, io_path, io_tools))
        p.start()
        write_procs.append(p)
    if wait:
        for p in write_procs:
            p.join()
    return write_procs, io_targets


def get_io_file_count(mnt_client, io_path):
    """Return number of regular files under io_path."""
    out, _ = mnt_client.exec_command(
        sudo=True, cmd=f"find {io_path} -type f 2>/dev/null | wc -l"
    )
    return int(out.strip())


def verify_io_paused(io_targets, duration=20, interval=5):
    """
    While quiesced, file counts under IO dirs must not increase.
    Returns True if paused, False if progress detected.
    """
    baselines = []
    for target in io_targets:
        count = get_io_file_count(target["client"], target["path"])
        baselines.append(count)
        log.info(f"IO baseline file count for {target['path']}: {count}")

    elapsed = 0
    while elapsed < duration:
        time.sleep(interval)
        elapsed += interval
        for idx, target in enumerate(io_targets):
            current = get_io_file_count(target["client"], target["path"])
            log.info(
                f"IO file count after {elapsed}s for {target['path']}: "
                f"{current} (baseline {baselines[idx]})"
            )
            if current > baselines[idx]:
                log.error(
                    f"IO progressed during quiesce on {target['path']}: "
                    f"{baselines[idx]} -> {current}"
                )
                return False
    log.info("IO made no progress during quiesce settle window")
    return True


def find_client_session(client, fs_util, fs_name, subvol_path, mnt_client):
    """
    Find MDS client session by exact subvol root path + mount client hostname.
    Searches all active MDS. Returns dict with id, num_caps, mds or None.
    Returns the string 'ambiguous' if more than one distinct client id matches.

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
                    f"Matched client session id={entry['id']} mds={mds_name} "
                    f"root={root} hostname={hostname} num_caps={entry['num_caps']}"
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
            f"No client session found for root={expect_root} hostname={expect_host}"
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
            f"Ambiguous client sessions for root={expect_root} "
            f"hostname={expect_host}: {unique}"
        )
        return "ambiguous"

    chosen = unique[0]
    if len(matches) > 1:
        log.info(
            f"Session id={chosen['id']} seen on {len(matches)} MDS entries; "
            f"using mds={chosen['mds']} num_caps={chosen['num_caps']}"
        )
    return chosen


def client_session_exists(client, fs_util, fs_name, client_id):
    """Return True if client_id is present on any active MDS."""
    mds_list = fs_util.get_active_mdss(client, fs_name=fs_name)
    for mds_name in mds_list:
        out, _ = client.exec_command(
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
    io_targets,
    io_threads,
):
    """
    This method is for Test1: Client caps validation during CG quiesce
    """
    session = find_client_session(client, fs_util, fs_name, subvol_path, mnt_client)
    if session is None or session == "ambiguous":
        log.error("Failed to locate unique client session before quiesce")
        return 1

    caps_before_quiesce = session["num_caps"]
    client_id = session["id"]
    log.info(f"Before quiesce: client_id={client_id}, num_caps={caps_before_quiesce}")

    log.info(f"CG quiesce on members:{qs_set}")
    rand_str = "".join(random.choice(string.digits) for i in range(3))
    qs_id_val = f"caps_test_{rand_str}"
    qs_out = cg_snap_util.cg_quiesce(
        client, qs_set, qs_id=qs_id_val, fs_name=fs_name, timeout=600, expiration=600
    )
    if qs_out == 1:
        log.error("CG quiesce command failed validation")
        return 1

    qs_query = cg_snap_util.get_qs_query(client, qs_id_val, fs_name=fs_name)
    qs_state = qs_query["sets"][qs_id_val]["state"]["name"]
    log.info(f"Quiesce set {qs_id_val} state after await: {qs_state}")
    if qs_state != "QUIESCED":
        log.error(f"Expected QUIESCED, got {qs_state}")
        cg_snap_util.cg_quiesce_release(
            client, qs_id_val, fs_name=fs_name, if_await=True
        )
        return 1

    if not any(t.is_alive() for t in io_threads):
        log.error("IO threads finished before quiesce validation; cannot verify pause")
        cg_snap_util.cg_quiesce_release(
            client, qs_id_val, fs_name=fs_name, if_await=True
        )
        return 1

    if not verify_io_paused(io_targets, duration=20, interval=5):
        log.error("IO did not pause while quiesce set was QUIESCED")
        cg_snap_util.cg_quiesce_release(
            client, qs_id_val, fs_name=fs_name, if_await=True
        )
        return 1

    caps_after_quiesce = caps_before_quiesce
    retry_cnt = 12
    while retry_cnt > 0:
        session_after = find_client_session(
            client, fs_util, fs_name, subvol_path, mnt_client
        )
        if session_after is None or session_after == "ambiguous":
            log.error("Failed to locate client session after quiesce")
            cg_snap_util.cg_quiesce_release(
                client, qs_id_val, fs_name=fs_name, if_await=True
            )
            return 1
        caps_after_quiesce = session_after["num_caps"]
        log.info(
            f"Client Caps Before quiesce : {caps_before_quiesce},"
            f"Clients Caps After quiesce : {caps_after_quiesce}"
        )
        if caps_after_quiesce >= caps_before_quiesce:
            retry_cnt -= 1
            time.sleep(5)
        else:
            break

    cg_snap_util.cg_quiesce_release(client, qs_id_val, fs_name=fs_name, if_await=True)
    if caps_after_quiesce >= caps_before_quiesce:
        log.error("Clients caps are not revoked during CG quiesce")
        return 1
    return 0


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
    if session_before is None or session_before == "ambiguous":
        log.error("Failed to locate unique client session before MDS failover")
        return 1
    caps_before_mds_fail = session_before["num_caps"]
    log.info(
        f"Before MDS fail: client_id={session_before['id']}, "
        f"num_caps={caps_before_mds_fail}"
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
        if session_after and session_after != "ambiguous":
            break
        log.info(f"Client session not found after failover, retry {attempt + 1}/3")
        time.sleep(30)

    if session_after is None or session_after == "ambiguous":
        log.error("Failed to locate client session after MDS failover")
        return 1

    caps_after_mds_fail = session_after["num_caps"]
    log.info(
        f"Client Caps Before MDS fail:{caps_before_mds_fail},"
        f"Clients Caps After MDS fail:{caps_after_mds_fail} "
        f"(client_id after={session_after['id']})"
    )
    if caps_after_mds_fail >= caps_before_mds_fail:
        log.error("Clients caps are not updated during MDS failover")
        return 1
    return 0


def client_caps_evict_test(client, subvol_path, mnt_client, mnt_pt, fs_name, fs_util):
    """
    This is method for Test3: Client caps validation when client evicted
    """
    session = find_client_session(client, fs_util, fs_name, subvol_path, mnt_client)
    if session is None or session == "ambiguous":
        log.error("Failed to locate unique client session before eviction")
        return 1

    client_id = session["id"]
    mds_name = session["mds"]
    caps_before_evict = session["num_caps"]
    log.info(
        f"Evicting Client with ID : {client_id} on mds.{mds_name} "
        f"(num_caps={caps_before_evict})"
    )
    cmd = f"ceph tell mds.{mds_name} client evict id={client_id}"
    client.exec_command(sudo=True, cmd=cmd)
    time.sleep(2)

    if client_session_exists(client, fs_util, fs_name, client_id):
        log.error(f"Client session id={client_id} still present after eviction")
        return 1
    log.info(
        f"Client session id={client_id} removed after eviction "
        f"(caps before evict were {caps_before_evict})"
    )

    log.info(f"Unmount and Remount {subvol_path} as Client session was evicted")
    mnt_client.exec_command(sudo=True, cmd=f"umount -l {mnt_pt}")
    mnt_params = {
        "mnt_path": subvol_path,
        "client": mnt_client,
        "fs_util": fs_util,
        "fs_name": fs_name,
    }
    mnt_params.update({"export_created": 0})
    fs_util.mount_ceph("fuse", mnt_params)
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
    log.info(f"FS status before MDS failover:{output}")
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
    CEPH-83597462 - Verify Clients caps update during evict, IO quiesce and MDS failover.

    Test Steps:
    1. Validate Client Caps updated when CG quiesce is run on subvolumes
    2. Validate Client Caps are updated after MDS failover.
        3. Check if client caps are updated when client session is evicted

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
        log.info(f"Crash status before Test: {crash_status_before}")

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

        log.info(
            "Test1:Validate Client Caps recalled when CG quiesce is run on subvolumes"
        )
        io_threads, io_targets = run_io_local(fs_util_v1, mnt_info, sv_list, wait=False)
        time.sleep(5)

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
                io_targets,
                io_threads,
            )
            == 1
        ):
            test_fail += 1

        for t in io_threads:
            t.join(timeout=300)

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
                client, subvol_path, mnt_client, mnt_pt, fs_name, fs_util_v1
            )
            == 1
        ):
            test_fail += 1

        if test_fail > 0:
            log.error("FAIL:Client Caps Validation Test Failed")
            return 1
        log.info("PASS:Client Caps Validation Test Passed")
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
            log.info(f"Crash status after Test: {crash_status_after}")
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
