"""Test BUFFER watcher drain latency with a controlled scenario.

Setup:
  - Schedule created PAUSED (no catch-up fires, no live cron fires)
  - Fire 1 triggered via backfill, unpaused to process it
  - Schedule paused again while fire 1 is running
  - Fire 2 queued via backfill (stored in PendingBackfills, not yet processed)
  - Schedule unpaused → fire 2 is processed while fire 1 is running → BUFFERED
  - Watcher activity starts watching fire 1's workflow
  - Fire 1 completes (25s sleep), watcher returns, drain fires fire 2
  - Measure: fire1_start → fire2_start should be ~25-35s (watcher poll ≤5s overhead)

Usage:
    uv run python -m schedule_samples.test_buffer_watcher [--cleanup]
"""

import argparse
import asyncio
import time

from datetime import datetime, timedelta, timezone

from google.protobuf.duration import from_timedelta
from google.protobuf.timestamp_pb2 import Timestamp

from cadence.api.v1 import common_pb2, schedule_pb2, tasklist_pb2
from cadence.client import Client

from schedule_samples.workflow import TASK_LIST, SLOW_WORKFLOW_TYPE


def _ts(dt: datetime) -> Timestamp:
    t = Timestamp()
    t.FromDatetime(dt)
    return t


async def _delete_if_exists(client: Client, schedule_id: str) -> None:
    try:
        await client.delete_schedule(schedule_id)
        print(f"  deleted existing schedule {schedule_id!r}")
    except Exception:
        pass


async def main(args: argparse.Namespace) -> None:
    # Use a timestamp-based unique ID to avoid requestID collisions across runs.
    # generateRequestID in Go hashes (scheduleID, scheduledTime, triggerSource) into a UUID.
    # If the same scheduleID + fire time is reused across runs, Cadence returns the
    # original (already-closed) RunID, making isWorkflowRunning return false immediately.
    schedule_id = f"watcher-drain-test-{int(time.time())}"

    async with Client(domain=args.domain, target=args.target) as client:
        if args.cleanup:
            # --cleanup deletes ALL watcher-drain-test-* schedules (list not available,
            # so just try the most recent). Users can manually delete old ones.
            print("Pass specific --schedule-id, or delete manually via the Cadence CLI.")
            return

        print("=== BUFFER watcher drain latency test ===\n")

        # Use cron boundaries safely in the past. spec.end_time blocks live cron fires
        # from interfering; the unique schedule ID prevents requestID collisions across runs.
        now_utc = datetime.now(timezone.utc)
        floor5 = now_utc.replace(minute=(now_utc.minute // 5) * 5, second=0, microsecond=0)
        fire1_time = floor5 - timedelta(minutes=10)   # 10 min ago
        fire2_time = fire1_time + timedelta(minutes=5)  # 5 min ago

        # End time must be in the future so the scheduler can compute a next-run timer
        # (if end_time is past, the scheduler sees "no more runs" and exits immediately).
        # 10 minutes in the future is well beyond the 35s test window but prevents
        # any live cron fires that would otherwise interfere (next cron is within ~5 min).
        spec_end = now_utc + timedelta(minutes=10)

        print(f"schedule_id={schedule_id!r}")
        print(f"fire1_time={fire1_time:%H:%M} UTC  fire2_time={fire2_time:%H:%M} UTC")
        print(f"spec.end_time=now+10min  (blocks live cron timer from interfering)\n")

        await client.create_schedule(
            schedule_id,
            spec=schedule_pb2.ScheduleSpec(
                cron_expression="*/5 * * * *",
                start_time=_ts(fire1_time),
                end_time=_ts(spec_end),   # no cron fires after spec_end
            ),
            action=schedule_pb2.ScheduleAction(
                start_workflow=schedule_pb2.ScheduleAction.StartWorkflowAction(
                    workflow_type=common_pb2.WorkflowType(name=SLOW_WORKFLOW_TYPE),
                    task_list=tasklist_pb2.TaskList(name=TASK_LIST),
                    workflow_id_prefix=f"{schedule_id}-",
                    execution_start_to_close_timeout=from_timedelta(timedelta(minutes=10)),
                    task_start_to_close_timeout=from_timedelta(timedelta(seconds=10)),
                )
            ),
            policies=schedule_pb2.SchedulePolicies(
                overlap_policy=schedule_pb2.SCHEDULE_OVERLAP_POLICY_BUFFER,
                catch_up_policy=schedule_pb2.SCHEDULE_CATCH_UP_POLICY_SKIP,
            ),
        )
        await client.pause_schedule(schedule_id, reason="test setup")
        print(f"Created and paused schedule {schedule_id!r}")

        # Trigger fire 1 via backfill.
        await client.backfill_schedule(
            schedule_id,
            start_time=fire1_time - timedelta(seconds=1),
            end_time=fire1_time + timedelta(seconds=1),
            overlap_policy=schedule_pb2.SCHEDULE_OVERLAP_POLICY_BUFFER,
        )
        await client.unpause_schedule(schedule_id, reason="start fire 1")
        fire1_wall_start = time.monotonic()
        print("Queued fire 1 + unpaused → waiting for fire 1 to start...")

        poll_start = time.monotonic()
        while True:
            await asyncio.sleep(2)
            desc = await client.describe_schedule(schedule_id)
            if desc.info.total_runs >= 1:
                print(f"  fire1 started at +{time.monotonic() - poll_start:.0f}s")
                break
            if time.monotonic() - poll_start > 20:
                print("ERROR: fire 1 never started")
                await _delete_if_exists(client, schedule_id)
                return

        # Wait until fire1's workflow has been running for at least 15s so it's well
        # into its 25s sleep when we queue fire2. This ensures isWorkflowRunning=true.
        elapsed = time.monotonic() - fire1_wall_start
        wait_more = max(0, 15 - elapsed)
        if wait_more > 0:
            print(f"  Sleeping {wait_more:.0f}s more (want fire1 running ≥15s before pausing)")
            await asyncio.sleep(wait_more)

        elapsed = time.monotonic() - fire1_wall_start
        print(f"  Pausing at +{elapsed:.0f}s (~{25 - elapsed:.0f}s left of fire1's 25s sleep)")
        await client.pause_schedule(schedule_id, reason="pause while fire 1 running")

        await client.backfill_schedule(
            schedule_id,
            start_time=fire2_time - timedelta(seconds=1),
            end_time=fire2_time + timedelta(seconds=1),
            overlap_policy=schedule_pb2.SCHEDULE_OVERLAP_POLICY_BUFFER,
        )
        print(f"  Queued fire 2 backfill ({fire2_time:%H:%M} UTC)")

        await asyncio.sleep(1)
        await client.unpause_schedule(schedule_id, reason="trigger fire 2 processing")
        print("  Unpaused → fire 2 should BUFFER (fire1 still running), watcher starts")

        time_until_fire1_done = 25 - (time.monotonic() - fire1_wall_start)
        print(f"\nFire1 should complete in ~{time_until_fire1_done:.0f}s. "
              f"Watcher polls every 5s. Expected fire2 start: ~{time_until_fire1_done + 5:.0f}s from now.\n")

        prev_runs = 1
        fire2_started_at: float | None = None
        while True:
            await asyncio.sleep(2)
            desc = await client.describe_schedule(schedule_id)
            total = desc.info.total_runs
            elapsed = time.monotonic() - fire1_wall_start
            print(f"  elapsed={elapsed:.0f}s  total_runs={total}")
            if total > prev_runs:
                fire2_started_at = time.monotonic()
                break
            if elapsed > 90:
                print("ERROR: fire 2 never started within 90s")
                break

        if fire2_started_at is not None:
            latency = fire2_started_at - fire1_wall_start
            print(f"\nFire 2 started {latency:.0f}s after fire 1 wall-start.")
            if latency < 25:
                print(f"BUFFER BROKEN: fire2 started only {latency:.0f}s after fire1 "
                      f"(fire1 sleeps 25s, buffer check bypassed)")
            elif latency <= 35:
                print(f"PASS: watcher drain working (~{latency:.0f}s = 25s sleep + ≤5s watcher + overhead)")
            else:
                print(f"SLOW: {latency:.0f}s, watcher not triggering drain promptly")

        await client.pause_schedule(schedule_id, reason="test complete")
        print(f"\nSchedule paused: {schedule_id!r}")
        print(f"Run: cadence --do {args.domain} schedule delete --sid {schedule_id}  (to clean up)")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Test BUFFER watcher drain latency")
    p.add_argument("--target", default="localhost:7833")
    p.add_argument("--domain", default="default")
    p.add_argument("--cleanup", action="store_true")
    return p


if __name__ == "__main__":
    try:
        asyncio.run(main(build_parser().parse_args()))
    except KeyboardInterrupt:
        pass
