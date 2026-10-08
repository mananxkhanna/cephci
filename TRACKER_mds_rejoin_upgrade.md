# Tracker: MDS rejoin hang during 7x→9x cephadm upgrade

**Status:** Internal RCA locked · no external broadcast yet · handoff for next chat  
**Updated:** 2026-10-08  
**Owner:** Manan Khanna  

---

## One-liner

Intermittent product wedge: after OSDs, cephadm sets `max_mds=1`; an MDS fail during that drain leaves rank 1 stuck in `rejoin` (`rejoin_joint_start`, no `rejoin_done`) → scale-down never finishes → upgrade hangs ~22/50. Same suite/build can pass or fail (timing).

---

## Suite / builds

| Item | Value |
|---|---|
| Suite | `suites/tentacle/cephfs/tier-0_cephfs_upgrade_7x_to_9x.yaml` |
| Conf | `conf/tentacle/cephfs/tier-2_cephfs_upgrade.yaml` |
| Stage | `Upgrade along with IOs` (`tests/parallel/test_parallel.py`) |
| Build | 20.2.2-349 · rhel-9 (Reef 7.1 → Tentacle 9.2) |
| Pass | upgrade-ci **143, 144** (IBM) |
| Fail | **145–147** (IBM), **148** (RH) |
| Best cluster logs | **146** (145 `ceph_logs` 404) |
| MTE | https://149.81.216.83/job/tentacle-test-executor/3279/ (soft build pin) |

### Parallel sub-tests
1. `cephfs_io.py` — continuous IO  
2. `cephfs_mds_failover.py` — `ceph mds fail` ~every 2 min; wait ranks 0+1 `active` (600s)  
3. `test_cephadm_upgrade.py` — mgr → mon → osd → mds → rest  

**Failover N** = Nth `ceph mds fail` in that loop. Fails 1–11 OK; **fail #12** hits during scale-down on bad runs.

---

## Timing vs product

| Layer | What |
|---|---|
| **Timing** | Fail must land after cephadm `fs set max_mds 1` (fail #12: ~22s later on 146, ~76s on 148). Explains pass 143/144 vs fail 145–148. |
| **Product** | Rank 1 stays in `rejoin` (0 caps), never `rejoin_done` → drain to 1 MDS never completes → upgrade stuck. |
| **Not** | Loose script / wait too short (600s Fail_MDS + ~50 min scale-down wait, zero progress). |

### Note: Fail_MDS error text
`"2 Active MDS did not start…"` is the **test helper** (always waits for ranks 0+1). After `max_mds=1`, steady state should be **one** active MDS — check is stale once scale-down starts. Real signal = rank 1 wedged in rejoin blocking drain.

---

## Evidence (locked from logs)

### max_mds / cephadm (146 & 148)
1. Suite sets `max_mds=5`  
2. After OSDs: `Scaling down filesystem cephfs` → mgr `fs set max_mds 1`  
3. Loop: `Waiting for fs cephfs to scale down to reach 1 MDS` (~191–192×, ~50 min)  
4. Never reaches MDS daemon upgrade / scale-up  

| | 146 (IBM) | 148 (RH) |
|---|---|---|
| `max_mds=1` | 2026-10-06 16:15:44Z | 2026-10-08 05:51:52Z |
| Wait starts | 16:15:52Z | 05:52:00Z |
| Fail #12 | 16:16:06Z | 05:53:08Z |
| Fail_MDS timeout | 16:26:18Z | 06:03:20Z |
| End mdsmap | r0 active, r1 rejoin, r2/r3 still present | r0 active, r1 rejoin only |

### MDS (146 `…node5.ztspay`)
- `reconnect_done` → `rejoin_start` / `rejoin_joint_start` @ 16:16:59Z  
- **`rejoin_done` count = 0**; log ends there  
- Client evicts during reconnect (~48s) before rejoin  

### Log bases
```
http://9.11.121.102/logs/IBM/9.2/rhel-9/upgrade/20.2.2-349/cephfs/146/logs/Test_Cephfs_Upgrade_7x_to_9x/
http://9.11.121.102/logs//RH/9.2/rhel-9/upgrade/20.2.2-349/cephfs/148/logs/Test_Cephfs_Upgrade_7x_to_9x/
```

External Bob TFA (weaker on scale-down; prefer our RCA):  
`/Users/manankhanna/Desktop/fix_tracker/cephfs_upgrade_7x_to_9x_tfa_report.md`

---

## Related bugs

| Key | Notes |
|---|---|
| **[IBMCEPH-18199](https://ibm-ceph.atlassian.net/browse/IBMCEPH-18199)** | Same suite/error (9.1). Closed Insufficient Data. Harish / Vivek. **Primary reopen candidate.** |
| **[IBMCEPH-10216](https://ibm-ceph.atlassian.net/browse/IBMCEPH-10216)** / [BZ#2302855](https://bugzilla.redhat.com/show_bug.cgi?id=2302855) | Same pattern 6.1→7.1. Closed insufficient data. Venky / Suma. |
| Amarnath (BZ `amk`) | **No** exact match filed |

---

## Dev log requirements (from both JIRAs)

Collected 2026-10-08 from comments on IBMCEPH-18199 (Vivek) and IBMCEPH-10216 (Venky).  
**Core ask is the same:** reproduce under elevated MDS debug — default CI logs show *that* it stuck, not *why*.

### Per ticket

| | **IBMCEPH-18199** (Vivek) | **IBMCEPH-10216** / BZ#2302855 (Venky) |
|---|---|---|
| **Config** | `ceph config set mds debug_mds 20` | Prefer `debug_mds=20` (warns: slower + large logs) |
| **What they need** | Why stuck in rejoin — default MDS logs insufficient | Logs from entry into **`up:rejoin`**, especially **peer MDS messaging** during rejoin |
| **When** | Repro with debug enabled before upgrade/failover | Same; cover from rejoin entry onward |
| **Extra** | Shareable logs (Box OK; asked about `10.64.24.74`) | Repeated failovers under IO; Suma hit: fail **rank 0 while rank 1 is `stopping`** (~21/27); optional live cluster left stuck |
| **Prior attempt** | Harish enabled debug once — run **did not hit the bug** (SSH fail, upgrade never started) → closed Cannot Reproduce | Suma later hit with debug; Venky still closed for insufficient data later |

### Unified checklist (bring this when reopening)

1. Enable **`debug_mds=20` before** upgrade + failover (not after the wedge).
2. Hit the **same** rejoin hang (not a different failure).
3. Collect MDS logs covering **`reconnect → rejoin_start / rejoin_joint_start`** with **no `rejoin_done`** — stuck rank **and** peer active MDS.
4. Include cephadm / suite timeline: `max_mds=1` / scale-down vs the failing `ceph mds fail`.
5. Optional while stuck: `ceph fs status`, `ceph health detail`, `ceph tell mds.<stuck> ops` / `status`.
6. Deliver via Box / shareable path (not only internal QE URLs).

---

## Decisions so far

- [x] Internal analysis before Neha/Slack/BZ broadcast  
- [x] Product + timing (not loose script)  
- [x] Prefer 146 over Bob’s 145-centric TFA  
- [ ] Reopen/comment IBMCEPH-18199 with 9.2 evidence  
- [ ] `debug_mds=20` hit (MTE or dedicated)  
- [ ] Team draft / Neha ping (hold until asked)  

---

## Next chat — start here

User will say what to do. Likely options:
1. Reopen/update **IBMCEPH-18199** with scale-down / `max_mds=1` RCA + 145–148 links  
2. Plan/run **debug_mds=20** reproduce  
3. Finalize team Slack/email from drafts in prior chat  
4. Align Fail_MDS wait with post-`max_mds=1` topology (script hygiene — secondary)

---

## Prior chat context

Search conversations: “CephFS upgrade MDS rejoin hang”, “Cephfs upgrade failure analysis”.  
Workspace note: this dir also contains a cephci checkout; tracker file is this `.md` only.
