"""
Post-upgrade CephX key rotation for CephFS upgrade suites.

Polarion: CEPH-xxxxx

Shared module (release-agnostic). Suites under ``suites/<release>/cephfs/``
(reef, squid, tentacle, …) call this module after the final upgrade hop.
Release coupling is suite YAML only (placement + ``config``); this file has
no runtime version gate.

---------------------------------------------------------------------------
What this module does (phases)
---------------------------------------------------------------------------
1. Client RPMs (when ``install_client_rpms``):
   ``dnf install -y --nogpgcheck`` of the configured package set on every
   client-role node of the cluster under test.

2. ``client.admin`` (when ``rotate_admin``):
   - ``cephadm shell -- ceph auth get-or-create client.admin-backup``
   - ``cephadm shell -- ceph auth rotate client.admin --key-type <key_type>``
   - prefer clean ``[client.admin]`` block from rotate stdout; else
     ``ceph auth get`` via ``cephadm shell --name mon.*`` (host keyring stale)
   - rewrite ``/etc/ceph/ceph.client.admin.keyring`` on installer + all clients
   - also rewrite ``/var/lib/ceph/<fsid>/config/ceph.client.admin.keyring``
     on the installer (path ``cephadm shell`` bind-mounts as ``/etc/ceph/ceph.keyring``)
   - verify with default ``cephadm shell``; ``auth rm`` backup via mon identity

3. Mount clients (kernel/fuse recipes from JSON or mirror pickle):
   For each distinct ``client.<hostname>``:
   - ``ceph auth rotate client.<hostname> --key-type <key_type>``
   - deploy keyring + ``.secret`` from cephadm onto that client
   - umount those CephFS mounts only (never NFS paths / nfs|nfs4 fstype)
   - remount (JSON path: subvol enrich via getpath; mirror: FS-root)
   - probe: printf content → cat → rm ``cephx_post_upgrade_probe``

4. NFS live rotate-key (opt-in: suite sets ``rotate_nfs: true`` and pre_req
   JSON has ``NFS``):
   - Pass config export name into PR #6108 ``_run_rotate_key_live_mount``
   - ``ceph nfs cluster rotate-key`` while the export stays mounted
   - No NFS umount/remount; missing ``rotate-key`` CLI → fail

Does not rotate service daemon keys (mon/mgr/osd/mds). Missing
``ceph auth rotate`` fails the test. Suites should set ``abort-on-fail: true``.
"""

import json
import os
import pickle
import re
import traceback
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

from cli.exceptions import OperationFailedError
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.nfs.test_nfs_cephx_key_rotation import (
    KEY_TYPE as DEFAULT_KEY_TYPE,
    _rotate_key_available,
    _run_rotate_key_live_mount,
)
from utility.log import Log

log = Log(__name__)

# Defaults shared across releases; override via suite ``config`` when needed.
DEFAULT_CLIENT_RPM_PACKAGES = (
    "ceph-common librados2 librbd1 python3-rados python3-rbd ceph-fuse"
)
DEFAULT_UPGRADE_CONFIG_PATH = "/home/cephuser/cephfs_upgrade_config.json"
ADMIN_BACKUP_ENTITY = "client.admin-backup"
ADMIN_KEYRING_PATH = "/etc/ceph/ceph.client.admin.keyring"
# cephadm shell bind-mounts this host path as /etc/ceph/ceph.keyring (IBM #18).
CEPHADM_ADMIN_KEYRING_TMPL = (
    "/var/lib/ceph/{fsid}/config/ceph.client.admin.keyring"
)
KEYRING_PATTERN = re.compile(r"^\s*key\s*=\s*(\S+)\s*$", re.MULTILINE)
PROBE_BASENAME = "cephx_post_upgrade_probe"
PROBE_CONTENT = "cephx-post-upgrade-probe"


@dataclass
class RotateOptions:
    """
    Suite-tunable knobs resolved from YAML ``config``.

    Same module for every release stream. Enable ``rotate_nfs`` only in suites
    whose pre_req created NFS.
    """

    mirror_pickle: bool = False
    upgrade_config_path: str = DEFAULT_UPGRADE_CONFIG_PATH
    client_packages: str = DEFAULT_CLIENT_RPM_PACKAGES
    key_type: str = DEFAULT_KEY_TYPE
    rotate_admin: bool = True
    # Default off: suites that create NFS in pre_req must set rotate_nfs: true.
    rotate_nfs: bool = False
    install_client_rpms: bool = True


def _options_from_config(config: dict) -> RotateOptions:
    """Build RotateOptions from suite ``config`` (unknown keys ignored)."""
    packages = config.get("client_packages", DEFAULT_CLIENT_RPM_PACKAGES)
    if isinstance(packages, (list, tuple)):
        packages = " ".join(packages)
    return RotateOptions(
        mirror_pickle=bool(config.get("mirror_pickle", False)),
        upgrade_config_path=config.get(
            "upgrade_config_path", DEFAULT_UPGRADE_CONFIG_PATH
        ),
        client_packages=packages,
        key_type=config.get("key_type", DEFAULT_KEY_TYPE),
        rotate_admin=bool(config.get("rotate_admin", True)),
        rotate_nfs=bool(config.get("rotate_nfs", False)),
        install_client_rpms=bool(config.get("install_client_rpms", True)),
    )


@dataclass
class CephfsMountRecipe:
    """
    One kernel or fuse CephFS mount to rotate credentials for, then
    umount, remount, and probe.
    """

    mnt_pt: str
    client_hostname: str
    mount_kind: str  # "kernel" or "fuse"
    fs_name: str
    vol_name: str
    subvol_name: str
    group_name: str
    fuse_extra_params: str = ""
    kernel_sub_dir: str = "/"
    kernel_extra_params: str = ""


def _cmd_detail(out, err) -> str:
    """Merge stdout/stderr for failure messages (either stream may hold the error)."""
    parts = [p for p in ((out or "").strip(), (err or "").strip()) if p]
    return " | ".join(parts) if parts else "(no output)"


def _extract_client_admin_keyring(text: str) -> Optional[str]:
    """
    Return a clean ``[client.admin]`` keyring block from command output.

    ``cephadm shell`` / ``auth rotate`` often prefix noise (e.g. ``Inferring
    fsid ...``). Writing that noise into ``/etc/ceph`` breaks later default
    ``cephadm shell`` auth (rc=13).
    """
    if not text:
        return None
    marker = "[client.admin]"
    idx = text.find(marker)
    if idx < 0:
        return None
    lines = text[idx:].splitlines()
    kept = [lines[0]]
    for line in lines[1:]:
        if line.startswith("[") and not line.startswith(marker):
            break
        kept.append(line)
    blob = "\n".join(kept).strip()
    if not KEYRING_PATTERN.search(blob):
        return None
    return blob


def _cephadm_shell(installer, subcmd: str) -> str:
    """
    Run ``cephadm shell -- <subcmd>`` on the installer.

    Auth uses the keyring cephadm bind-mounts from
    ``/var/lib/ceph/<fsid>/config/ceph.client.admin.keyring`` (as
    ``/etc/ceph/ceph.keyring`` inside the container), not necessarily
    host ``/etc/ceph/ceph.client.admin.keyring``. After ``client.admin``
    rotate and before that cephadm config keyring is rewritten, use
    ``_cephadm_shell_as_mon`` instead.
    """
    out, err = installer.exec_command(
        sudo=True,
        cmd=f"cephadm shell -- {subcmd}",
        check_ec=False,
    )
    if installer.exit_status != 0:
        raise OperationFailedError(
            f"cephadm shell failed (rc={installer.exit_status}): "
            f"{_cmd_detail(out, err)}"
        )
    return (out or err or "").strip()


def _cephadm_ls_daemons(installer) -> list:
    """Return parsed ``cephadm ls`` JSON list from the installer."""
    out, err = installer.exec_command(
        sudo=True,
        # Plain `cephadm ls` already emits JSON; `--format json` is rejected as an
        # unrecognized global arg on some host cephadm builds.
        cmd="cephadm ls",
        check_ec=False,
    )
    if installer.exit_status != 0:
        raise OperationFailedError(
            f"cephadm ls failed (rc={installer.exit_status}): "
            f"{_cmd_detail(out, err)}"
        )
    try:
        daemons = json.loads(out or "[]")
    except json.JSONDecodeError as exc:
        raise OperationFailedError(f"cephadm ls JSON parse failed: {exc}") from exc
    if not isinstance(daemons, list):
        raise OperationFailedError("cephadm ls did not return a JSON list")
    return daemons


def _cluster_fsid(installer) -> str:
    """Return cluster fsid from ``cephadm ls`` on the installer."""
    for daemon in _cephadm_ls_daemons(installer):
        fsid = daemon.get("fsid") or ""
        if fsid:
            return fsid
    raise OperationFailedError(
        "No fsid in cephadm ls; needed to refresh cephadm admin keyring path"
    )


def _cephadm_admin_keyring_path(installer) -> str:
    """Host path cephadm shell bind-mounts as the admin keyring."""
    return CEPHADM_ADMIN_KEYRING_TMPL.format(fsid=_cluster_fsid(installer))


def _local_mon_daemon_name(installer) -> str:
    """Return a local ``mon.*`` daemon name from ``cephadm ls`` on the installer."""
    for daemon in _cephadm_ls_daemons(installer):
        name = daemon.get("name") or ""
        if name.startswith("mon."):
            return name
    raise OperationFailedError(
        "No local mon.* daemon on installer; needed to auth after "
        "client.admin rotate while /etc/ceph admin keyring is stale"
    )


def _cephadm_shell_as_mon(installer, subcmd: str) -> str:
    """
    Run ``cephadm shell --name mon.* -- <subcmd>``.

    Mon daemon identity is unaffected by ``client.admin`` rotate, so this
    works while host ``/etc/ceph/ceph.client.admin.keyring`` is still stale.
    """
    mon_name = _local_mon_daemon_name(installer)
    out, err = installer.exec_command(
        sudo=True,
        cmd=f"cephadm shell --name {mon_name} -- {subcmd}",
        check_ec=False,
    )
    if installer.exit_status != 0:
        raise OperationFailedError(
            f"cephadm shell --name {mon_name} failed "
            f"(rc={installer.exit_status}): {_cmd_detail(out, err)}"
        )
    return (out or err or "").strip()


def _auth_rotate_available(installer) -> bool:
    """Return True if ``ceph auth --help`` lists a rotate subcommand."""
    help_text = _cephadm_shell(installer, "ceph auth --help")
    return "rotate" in help_text


def _mask_key(key: str) -> str:
    """Return a half-masked key for logs (first 4 ... last 4)."""
    if not key or len(key) < 8:
        return "****"
    return f"{key[:4]}...{key[-4:]}"


def _get_entity_key(installer, entity: str) -> str:
    """Parse the secret key value from ``ceph auth get <entity>`` via cephadm."""
    raw = _cephadm_shell(installer, f"ceph auth get {entity}")
    match = KEYRING_PATTERN.search(raw)
    if not match:
        raise OperationFailedError(f"Could not parse key for {entity}")
    return match.group(1)


def _write_remote_text(host, path: str, content: str, mode: str = "644") -> None:
    """Write ``content`` to ``path`` on a remote Node or Ceph object; chmod."""
    text = content if content.endswith("\n") else content + "\n"
    remote = host.remote_file(sudo=True, file_name=path, file_mode="w")
    remote.write(text)
    remote.flush()
    remote.close()
    host.exec_command(sudo=True, cmd=f"chmod {mode} {path}", check_ec=False)


def _install_client_rpms(clients, packages: str) -> None:
    """
    ``dnf install -y --nogpgcheck <packages>`` on each client; log ceph --version.

    Does not gate on RHCS/IBM version strings.
    """
    cmd = f"dnf install -y --nogpgcheck {packages}"
    for client in clients:
        log.info("Installing/updating client packages on %s", client.node.hostname)
        client.exec_command(sudo=True, cmd=cmd, long_running=True)
        ver, _ = client.exec_command(sudo=True, cmd="ceph --version", check_ec=False)
        log.info("ceph --version on %s: %s", client.node.hostname, (ver or "").strip())


def _admin_keyring_nodes(ceph_cluster, clients) -> List:
    """
    Nodes that must receive a rewritten admin keyring after client.admin rotate:
    installer + every client-role node on the cluster under test.
    """
    nodes = list(ceph_cluster.get_nodes("installer"))
    seen = {n.hostname for n in nodes}
    for client in clients:
        node = client.node
        if node.hostname not in seen:
            nodes.append(node)
            seen.add(node.hostname)
    return nodes


def _rotate_client_admin(installer, key_nodes, key_type: str) -> None:
    """
    Rotate ``client.admin`` and rewrite admin keyrings on ``key_nodes``.

    Steps:
      1. Snapshot key (masked log); create ``client.admin-backup``.
      2. ``ceph auth rotate client.admin --key-type <key_type>`` via cephadm.
      3. Prefer a clean ``[client.admin]`` block from rotate stdout; else
         ``ceph auth get`` via mon identity (host keyring stale until step 4).
      4. Fail if key unchanged; write new keyring on each key_node
         (``/etc/ceph/...``) and on the installer's cephadm config path
         (``/var/lib/ceph/<fsid>/config/...`` — what default shell mounts).
      5. Verify get with default cephadm shell (proves cephadm keyring rewrite);
         ``auth rm`` backup via mon identity.
    """
    log.info("=== rotate client.admin (with backup entity) ===")
    # Use mon identity for baseline read: the host-side admin keyring may already
    # be stale (e.g. after a prior upgrade hop) and cephadm shell would get rc=13.
    raw_before = _cephadm_shell_as_mon(installer, "ceph auth get client.admin")
    match_before = KEYRING_PATTERN.search(raw_before)
    if not match_before:
        raise OperationFailedError(
            "Could not parse client.admin key before rotate (mon-identity get)"
        )
    before = match_before.group(1)
    log.info("client.admin baseline key %s", _mask_key(before))

    _cephadm_shell(
        installer,
        f'ceph auth get-or-create {ADMIN_BACKUP_ENTITY} mon "allow *"',
    )
    _cephadm_shell(installer, f"ceph auth get {ADMIN_BACKUP_ENTITY}")
    rotate_out = _cephadm_shell(
        installer, f"ceph auth rotate client.admin --key-type {key_type}"
    )

    # Prefer a cleaned rotate keyring for rewrite; never write shell noise
    # (e.g. "Inferring fsid ...") into keyring files — that yields rc=13 later.
    keyring_blob = _extract_client_admin_keyring(rotate_out or "")
    if keyring_blob:
        log.info("Using cleaned client.admin keyring from auth rotate stdout")
        match = KEYRING_PATTERN.search(keyring_blob)
    else:
        log.info(
            "auth rotate stdout had no clean [client.admin] block; "
            "fetching client.admin via mon identity for rewrite"
        )
        keyring_blob = _cephadm_shell_as_mon(
            installer, "ceph auth get client.admin"
        )
        keyring_blob = (
            _extract_client_admin_keyring(keyring_blob) or keyring_blob
        )
        match = KEYRING_PATTERN.search(keyring_blob or "")
    if not match:
        raise OperationFailedError(
            "Could not parse client.admin key after rotate "
            "(rotate stdout and mon-identity get)"
        )
    after = match.group(1)
    if before == after:
        raise OperationFailedError("client.admin key unchanged after auth rotate")
    log.info("client.admin rotated key %s", _mask_key(after))

    for node in key_nodes:
        log.info("Refreshing %s on %s", ADMIN_KEYRING_PATH, node.hostname)
        _write_remote_text(node, ADMIN_KEYRING_PATH, keyring_blob, mode="644")

    # Default cephadm shell mounts this path as /etc/ceph/ceph.keyring — not
    # host /etc/ceph/ceph.client.admin.keyring (Jenkins #18 / cephadm.log).
    cephadm_keyring = _cephadm_admin_keyring_path(installer)
    log.info(
        "Refreshing cephadm-shell keyring %s on %s",
        cephadm_keyring,
        installer.hostname,
    )
    installer.exec_command(
        sudo=True,
        cmd=f"mkdir -p {os.path.dirname(cephadm_keyring)}",
        check_ec=False,
    )
    _write_remote_text(installer, cephadm_keyring, keyring_blob, mode="600")

    # Default shell must work after cephadm config keyring rewrite.
    _cephadm_shell(installer, "ceph auth get client.admin")
    # Backup rm via mon: does not depend on host admin keyring.
    _cephadm_shell_as_mon(installer, f"ceph auth rm {ADMIN_BACKUP_ENTITY}")
    log.info("client.admin rotated and keyring refreshed on admin nodes")


def _collect_nfs_mount_paths(config: dict) -> Set[str]:
    """
    Collect NFS mount paths from pre_req JSON ``NFS`` section.

    Used only to exclude those paths from CephFS umount. Empty when pre_req
    did not create NFS.
    """
    paths: Set[str] = set()
    nfs_root = config.get("NFS") or {}
    for _nfs_name, exports in nfs_root.items():
        if not isinstance(exports, dict):
            continue
        for _export_key, export_data in exports.items():
            if isinstance(export_data, dict):
                mnt = export_data.get("nfs_mnt_pt")
                if mnt:
                    paths.add(mnt.rstrip("/"))
    return paths


def _collect_cephfs_mount_recipes(config: dict) -> List[CephfsMountRecipe]:
    """
    Build kernel/fuse mount recipes from ``upgrade_pre_req`` JSON ``CephFS``.

    Classifies mounts by path substring ``cephfs_kernel`` / ``cephfs_fuse``.
    Skips unrecognized paths (does not treat NFS mounts as CephFS recipes).
    """
    recipes: List[CephfsMountRecipe] = []
    cephfs = config.get("CephFS") or {}
    for vol_name, groups in cephfs.items():
        if not isinstance(groups, dict):
            continue
        for group_name, subvols in groups.items():
            if not isinstance(subvols, dict):
                continue
            for subvol_name, data in subvols.items():
                if not isinstance(data, dict) or "mnt_pt" not in data:
                    continue
                mnt_pt = data["mnt_pt"]
                client_hostname = data["mnt_client"]
                if "cephfs_fuse" in mnt_pt:
                    kind = "fuse"
                elif "cephfs_kernel" in mnt_pt:
                    kind = "kernel"
                else:
                    raise OperationFailedError(
                        f"Unrecognized CephFS mount path {mnt_pt!r}; "
                        "expected cephfs_kernel or cephfs_fuse in mnt_pt"
                    )
                recipes.append(
                    CephfsMountRecipe(
                        mnt_pt=mnt_pt,
                        client_hostname=client_hostname,
                        mount_kind=kind,
                        fs_name=vol_name,
                        vol_name=vol_name,
                        subvol_name=subvol_name,
                        group_name=group_name,
                    )
                )
    return recipes


def _enrich_recipe_from_cluster(client, recipe: CephfsMountRecipe) -> CephfsMountRecipe:
    """
    Fill remount params for standard pre_req subvol mounts.

    Runs ``ceph fs subvolume getpath`` and sets kernel ``sub_dir`` + ``fs=``
    or fuse ``-r <path> --client_fs``.
    """
    cmd = (
        f"ceph fs subvolume getpath {recipe.vol_name} "
        f"{recipe.subvol_name} {recipe.group_name}"
    )
    subvol_path, _ = client.exec_command(sudo=True, cmd=cmd)
    subvol_path = (subvol_path or "").strip()
    if recipe.mount_kind == "kernel":
        return CephfsMountRecipe(
            mnt_pt=recipe.mnt_pt,
            client_hostname=recipe.client_hostname,
            mount_kind=recipe.mount_kind,
            fs_name=recipe.fs_name,
            vol_name=recipe.vol_name,
            subvol_name=recipe.subvol_name,
            group_name=recipe.group_name,
            kernel_sub_dir=subvol_path,
            kernel_extra_params=f",fs={recipe.fs_name}",
        )
    fuse_extra = f"-r {subvol_path} --client_fs {recipe.fs_name}"
    return CephfsMountRecipe(
        mnt_pt=recipe.mnt_pt,
        client_hostname=recipe.client_hostname,
        mount_kind=recipe.mount_kind,
        fs_name=recipe.fs_name,
        vol_name=recipe.vol_name,
        subvol_name=recipe.subvol_name,
        group_name=recipe.group_name,
        fuse_extra_params=fuse_extra,
    )


def _collect_mirror_mount_recipes(
    log_dir: str, client_hostname: str
) -> List[CephfsMountRecipe]:
    """
    Build FS-root kernel+fuse recipes from mirror ``variables.pkl``.

    Same pickle layout as ``configure_cephfs_mirroring`` /
    ``post_upgrade_validate``. Remount uses ``fs=`` / ``--client_fs`` at the
    pickle mount dirs (no subvol getpath enrich) so later mirror validate
    still finds mounts at those paths.
    """
    pkl_path = os.path.join(log_dir, "variables.pkl")
    if not os.path.isfile(pkl_path):
        raise OperationFailedError(
            f"Mirror mount state not found at {pkl_path}; cannot remount after rotate"
        )
    with open(pkl_path, "rb") as file:
        (
            source_fs,
            _target_fs,
            _target_user,
            _target_site_name,
            kernel_mounting_dir_1,
            fuse_mounting_dir_1,
            _subvol1_path,
            _subvol2_path,
            _fs_util_ceph1,
            _fs_util_ceph2,
            _subvolumegroup_list,
            _subvolume_list,
        ) = pickle.load(file)
    return [
        CephfsMountRecipe(
            mnt_pt=kernel_mounting_dir_1,
            client_hostname=client_hostname,
            mount_kind="kernel",
            fs_name=source_fs,
            vol_name=source_fs,
            subvol_name="subvol_1",
            group_name="subvolgroup_1",
            kernel_sub_dir="/",
            kernel_extra_params=f",fs={source_fs}",
        ),
        CephfsMountRecipe(
            mnt_pt=fuse_mounting_dir_1,
            client_hostname=client_hostname,
            mount_kind="fuse",
            fs_name=source_fs,
            vol_name=source_fs,
            subvol_name="subvol_2",
            group_name="subvolgroup_1",
            fuse_extra_params=f"--client_fs {source_fs}",
        ),
    ]


def _client_by_hostname(clients, hostname: str):
    """Return the Ceph client object whose node hostname matches ``hostname``."""
    for client in clients:
        if client.node.hostname == hostname:
            return client
    raise OperationFailedError(f"No client object for hostname {hostname}")


def _deploy_client_credentials(installer, client, entity: str) -> None:
    """
    Deploy rotated credentials for ``entity`` onto ``client``.

    Fetches keyring and key via cephadm on the installer, then writes
    ``/etc/ceph/ceph.<entity>.keyring`` and ``/etc/ceph/<hostname>.secret``.
    """
    hostname = entity.replace("client.", "", 1)
    keyring_blob = _cephadm_shell(installer, f"ceph auth get {entity}")
    secret = _cephadm_shell(installer, f"ceph auth get-key {entity}")
    _write_remote_text(
        client, f"/etc/ceph/ceph.{entity}.keyring", keyring_blob, mode="644"
    )
    _write_remote_text(client, f"/etc/ceph/{hostname}.secret", secret, mode="600")


def _umount_cephfs_only(client, mnt_pt: str, nfs_paths: Set[str]) -> None:
    """
    Umount a CephFS mount point; never umount NFS.

    Skips when ``mnt_pt`` is in ``nfs_paths`` or ``findmnt`` reports nfs/nfs4.
    Falls back to ``umount -l`` if plain umount fails.
    """
    normalized = mnt_pt.rstrip("/")
    for nfs in nfs_paths:
        if normalized == nfs or normalized.startswith(nfs + "/"):
            log.info("Skipping umount of NFS path %s", mnt_pt)
            return
    out, _ = client.exec_command(
        sudo=True,
        cmd=f"findmnt -n -o FSTYPE --target {mnt_pt}",
        check_ec=False,
    )
    fstype = (out or "").strip()
    if fstype in ("nfs", "nfs4"):
        log.info("Skipping umount of NFS fstype at %s", mnt_pt)
        return
    client.exec_command(
        sudo=True, cmd=f"umount {mnt_pt}", timeout=120, check_ec=False
    )
    if client.exit_status != 0:
        log.info(
            "Doing lazy umount (-l) for %s (plain umount rc=%s)",
            mnt_pt,
            client.exit_status,
        )
        client.exec_command(
            sudo=True, cmd=f"umount -l {mnt_pt}", timeout=120, check_ec=False
        )


def _remount_recipe(fs_util: FsUtils, client, recipe: CephfsMountRecipe) -> None:
    """Remount one kernel or fuse recipe via FsUtils at the original mnt_pt."""
    mon_ips = ",".join(fs_util.get_mon_node_ips())
    if recipe.mount_kind == "kernel":
        fs_util.kernel_mount(
            [client],
            recipe.mnt_pt,
            mon_ips,
            sub_dir=recipe.kernel_sub_dir,
            extra_params=recipe.kernel_extra_params,
        )
    else:
        fs_util.fuse_mount(
            [client],
            recipe.mnt_pt,
            extra_params=recipe.fuse_extra_params,
        )


def _probe_io(client, mnt_pt: str) -> None:
    """
    Smoke-check the remounted path: printf a small file, cat it back.

    File: ``<mnt_pt>/cephx_post_upgrade_probe``. Caller removes it.
    """
    path = f"{mnt_pt.rstrip('/')}/{PROBE_BASENAME}"
    client.exec_command(
        sudo=True,
        cmd=f"printf '%s\\n' '{PROBE_CONTENT}' > {path}",
    )
    client.exec_command(sudo=True, cmd=f"cat {path} > /dev/null")


def _cleanup_probes(client, mnt_pt: str) -> None:
    """Best-effort ``rm -f`` of the probe file under ``mnt_pt``."""
    path = f"{mnt_pt.rstrip('/')}/{PROBE_BASENAME}"
    client.exec_command(sudo=True, cmd=f"rm -f {path}", check_ec=False)


def _rotate_mount_clients(
    installer,
    fs_util: FsUtils,
    clients,
    recipes: List[CephfsMountRecipe],
    nfs_paths: Set[str],
    key_type: str,
    enrich_from_cluster: bool = True,
) -> None:
    """
    Per distinct mount-client hostname: rotate key, deploy creds, umount
    CephFS only, remount, probe, cleanup probe.

    ``enrich_from_cluster=True`` → standard JSON/subvol remount.
    ``enrich_from_cluster=False`` → mirror FS-root recipes as-is.
    """
    by_host: Dict[str, List[CephfsMountRecipe]] = {}
    for recipe in recipes:
        by_host.setdefault(recipe.client_hostname, []).append(recipe)

    for hostname, host_recipes in by_host.items():
        entity = f"client.{hostname}"
        before = _get_entity_key(installer, entity)
        log.info("Rotating %s (baseline key %s)", entity, _mask_key(before))
        _cephadm_shell(
            installer, f"ceph auth rotate {entity} --key-type {key_type}"
        )
        after = _get_entity_key(installer, entity)
        if before == after:
            raise OperationFailedError(f"Key unchanged after rotate for {entity}")
        client = _client_by_hostname(clients, hostname)
        _deploy_client_credentials(installer, client, entity)

        enriched = []
        for recipe in host_recipes:
            if enrich_from_cluster:
                enriched.append(_enrich_recipe_from_cluster(client, recipe))
            else:
                enriched.append(recipe)

        for recipe in enriched:
            _umount_cephfs_only(client, recipe.mnt_pt, nfs_paths)

        for recipe in enriched:
            _remount_recipe(fs_util, client, recipe)
            try:
                _probe_io(client, recipe.mnt_pt)
            finally:
                _cleanup_probes(client, recipe.mnt_pt)


def _nfs_section_from_config(config: dict) -> Optional[Tuple[str, str, str, str]]:
    """
    Return the first NFS export tuple from pre_req JSON, or None.

    Tuple: ``(nfs_cluster_name, export_name, nfs_mnt_pt, nfs_mnt_client)``.
    Export name is taken from the JSON key (e.g. ``/export1``).
    """
    nfs_root = config.get("NFS")
    if not nfs_root:
        return None
    for nfs_name, exports in nfs_root.items():
        if not isinstance(exports, dict):
            continue
        for export_name, export_data in exports.items():
            if not isinstance(export_data, dict):
                continue
            mnt_client = export_data.get("nfs_mnt_client")
            if not mnt_client:
                raise OperationFailedError(
                    f"NFS export {export_name!r} missing nfs_mnt_client in config"
                )
            return (
                nfs_name,
                export_name,
                export_data.get("nfs_mnt_pt", "/mnt/nfs/"),
                mnt_client,
            )
    return None


def _run_nfs_live_rotate_key(
    ceph_cluster,
    installer,
    clients,
    config: dict,
) -> None:
    """
    Run PR #6108 live-mount NFS CephX rotate-key for the export in ``config``.

    Requires an ``NFS`` section in pre_req JSON. Calls
    ``_run_rotate_key_live_mount`` with the config export name (no umount).
    Fails if ``ceph nfs cluster rotate-key`` is unavailable.
    """
    nfs_info = _nfs_section_from_config(config)
    if not nfs_info:
        raise OperationFailedError(
            "rotate_nfs=true but pre_req JSON has no NFS section"
        )
    if not _rotate_key_available(installer):
        raise OperationFailedError(
            "NFS section present but ceph nfs cluster rotate-key is unavailable"
        )
    nfs_name, export_name, nfs_mnt, mnt_client_host = nfs_info
    nfs_client = _client_by_hostname(clients, mnt_client_host)
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    mount_point = nfs_mnt.rstrip("/") if nfs_mnt else nfs_mnt
    log.info(
        "NFS rotate-key for cluster %s export %s (live mount %s, no umount)",
        nfs_name,
        export_name,
        mount_point,
    )
    _run_rotate_key_live_mount(
        installer,
        nfs_nodes,
        [nfs_client],
        nfs_name,
        export_name,
        mount_point,
    )


def run(ceph_cluster, **kw):
    """
    Entry point: post-upgrade manual CephX rotation for CephFS upgrade suites.

    Always (defaults): install client RPMs; rotate client.admin + keyring
    rewrite; rotate mount-client keys; umount/remount kernel+fuse; probe.

    NFS live rotate-key runs only when ``config.rotate_nfs`` is true **and**
    pre_req JSON has ``NFS``.

    Config keys:
        mirror_pickle (bool): load mounts from run_config.log_dir/variables.pkl
        upgrade_config_path (str): pre_req JSON path
        client_packages (str|list): dnf package set
        key_type (str): CephX key type (default aes256k)
        rotate_admin (bool): default true
        rotate_nfs (bool): default false — set true only when pre_req created NFS
        install_client_rpms (bool): default true

    Mirror: scope YAML with ``clusters: <upgraded>`` (e.g. ceph1 only).

    Returns:
        0 on success, 1 on failure.
    """
    try:
        config_kw = kw.get("config") or {}
        opts = _options_from_config(config_kw)
        clients = ceph_cluster.get_ceph_objects("client")
        if not clients:
            raise OperationFailedError("No client nodes in cluster")
        installer = ceph_cluster.get_nodes("installer")[0]
        fs_util = FsUtils(ceph_cluster)

        if not _auth_rotate_available(installer):
            raise OperationFailedError(
                "ceph auth rotate is not available on this build"
            )

        log.info(
            "CephX post-upgrade rotate options: mirror_pickle=%s key_type=%s "
            "rotate_admin=%s rotate_nfs=%s install_client_rpms=%s",
            opts.mirror_pickle,
            opts.key_type,
            opts.rotate_admin,
            opts.rotate_nfs,
            opts.install_client_rpms,
        )

        admin_refreshed = False
        if opts.install_client_rpms:
            _install_client_rpms(clients, opts.client_packages)
        if opts.rotate_admin:
            _rotate_client_admin(
                installer,
                _admin_keyring_nodes(ceph_cluster, clients),
                opts.key_type,
            )
            admin_refreshed = True

        pre_upgrade_config = None
        nfs_paths: Set[str] = set()
        recipes: List[CephfsMountRecipe] = []

        if opts.mirror_pickle:
            run_config = kw.get("run_config") or {}
            log_dir = run_config.get("log_dir")
            if not log_dir:
                raise OperationFailedError(
                    "mirror_pickle set but run_config.log_dir missing"
                )
            hostname = clients[0].node.hostname
            recipes = _collect_mirror_mount_recipes(log_dir, hostname)
        else:
            remote = clients[0].remote_file(
                sudo=True,
                file_name=opts.upgrade_config_path,
                file_mode="r",
            )
            pre_upgrade_config = json.load(remote)
            remote.close()
            nfs_paths = _collect_nfs_mount_paths(pre_upgrade_config)
            recipes = _collect_cephfs_mount_recipes(pre_upgrade_config)

        if not recipes:
            raise OperationFailedError(
                "No kernel/fuse CephFS mount recipes found for post-upgrade rotate"
            )

        log.info(
            "Rotating %d CephFS mount(s); NFS paths excluded from umount: %s",
            len(recipes),
            nfs_paths,
        )
        _rotate_mount_clients(
            installer,
            fs_util,
            clients,
            recipes,
            nfs_paths,
            opts.key_type,
            enrich_from_cluster=not opts.mirror_pickle,
        )

        if opts.rotate_nfs:
            if not pre_upgrade_config:
                raise OperationFailedError(
                    "rotate_nfs=true requires pre_req JSON (not used with mirror_pickle)"
                )
            _run_nfs_live_rotate_key(
                ceph_cluster, installer, clients, pre_upgrade_config
            )
        else:
            log.info("rotate_nfs=false; NFS rotate-key phase not enabled")

        log.info(
            "TEST PASSED - Post-upgrade CephX key rotation verified for %d "
            "mount(s)%s",
            len(recipes),
            ", admin keyring refreshed" if admin_refreshed else "",
        )
        return 0
    except Exception as exc:
        log.error("Post-upgrade CephX key rotation failed: %s", exc)
        log.error(traceback.format_exc())
        return 1
