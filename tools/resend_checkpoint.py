#!/usr/bin/env python3
"""
Force-resend a checkpoint Pushover notification.

Run from the repo root:
    python tools/resend_checkpoint.py              # resends checkpoint_600
    python tools/resend_checkpoint.py checkpoint_350

What it does:
  1. Looks up the existing accuracy_report record for the given rtype.
  2. If the PDF file still exists on disk, re-uploads it to filebin and resends Pushover.
  3. If the file is gone, regenerates the PDF from the last 120 days of signals,
     uploads, notifies, and updates the DB record.
  4. If the checkpoint was never saved to the DB at all, runs a full
     check_and_run_checkpoints() which will generate and notify normally.
"""
import os, sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _update_record(rtype: str, filename: str, url: str, verdict: str) -> None:
    from db.database import _is_postgres, _get_pg_conn, _put_pg_conn, _get_sqlite_conn
    try:
        if _is_postgres():
            conn = _get_pg_conn(); cur = conn.cursor()
            cur.execute(
                "UPDATE accuracy_reports SET download_url=%s, filename=%s, status_label=%s "
                "WHERE report_type=%s",
                (url, filename, verdict, rtype),
            )
            conn.commit(); cur.close(); _put_pg_conn(conn)
        else:
            conn = _get_sqlite_conn()
            conn.execute(
                "UPDATE accuracy_reports SET download_url=?, filename=?, status_label=? "
                "WHERE report_type=?",
                (url, filename, verdict, rtype),
            )
            conn.commit(); conn.close()
        print(f"  [resend] DB record updated for {rtype}")
    except Exception as e:
        print(f"  [resend] DB update failed: {e}")


def resend(rtype: str = "checkpoint_600") -> None:
    from db.database import get_accuracy_reports, count_total_signals, get_signal_log
    from reports.checkpoint_reports import (
        _upload_and_notify,
        generate_checkpoint_15,
        generate_checkpoint_30,
        generate_checkpoint_60,
        check_and_run_checkpoints,
    )

    total_logged = count_total_signals()
    print(f"  [resend] {total_logged} total signals all-time")

    TITLES = {
        "checkpoint_150": "Axiom Terminal — Sanity Check (150 Signals)",
        "checkpoint_350": "Axiom Terminal — Preliminary Assessment (350 Signals)",
        "checkpoint_600": "Axiom Terminal — Final Verdict (600 Signals)",
    }
    GENERATORS = {
        "checkpoint_150": lambda df: generate_checkpoint_15(df),
        "checkpoint_350": lambda df: generate_checkpoint_30(df),
        "checkpoint_600": lambda df: generate_checkpoint_60(df, total_logged=total_logged),
    }

    title = TITLES.get(rtype, rtype)
    reports = get_accuracy_reports() or []
    record = next((r for r in reports if r["report_type"] == rtype), None)

    if record:
        filename = record.get("filename", "")
        verdict  = record.get("status_label", "")
        if filename and os.path.exists(filename):
            print(f"  [resend] Re-uploading existing PDF: {filename}")
            url = _upload_and_notify(
                filename, title,
                f"Verdict: {verdict}\n{total_logged} signals total. Tap to download PDF.",
            )
            _update_record(rtype, filename, url or "", verdict)
            print(f"  [resend] Done. URL: {url}")
            return

        print(f"  [resend] PDF not on disk — regenerating from last 120 days of signals...")
    else:
        print(f"  [resend] {rtype} not in accuracy_reports — will generate fresh")

    df = get_signal_log(days=120)
    if df.empty:
        print("  [resend] No signals in 120-day window — cannot generate report")
        return

    gen_fn = GENERATORS.get(rtype)
    if not gen_fn:
        print(f"  [resend] Unknown rtype: {rtype}")
        return

    result = gen_fn(df)
    if not result:
        print("  [resend] Report generation returned None")
        return

    filename, verdict = result
    url = _upload_and_notify(
        filename, title,
        f"Verdict: {verdict}\n{total_logged} signals total. Tap to download PDF.",
    )

    if record:
        _update_record(rtype, filename, url or "", verdict)
    else:
        from db.database import save_accuracy_report
        THRESHOLDS = {"checkpoint_150": 150, "checkpoint_350": 350, "checkpoint_600": 600}
        save_accuracy_report(rtype, THRESHOLDS.get(rtype, 0), filename, url or "", verdict)

    print(f"  [resend] Done. verdict={verdict}  URL={url}")


if __name__ == "__main__":
    rtype = sys.argv[1] if len(sys.argv) > 1 else "checkpoint_600"
    resend(rtype)
