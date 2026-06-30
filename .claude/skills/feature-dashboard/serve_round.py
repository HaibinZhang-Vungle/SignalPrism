#!/usr/bin/env python3
"""
Run the Feature Workbench as a ROUND in the ML pipeline.

This turns the static Capability Map into an interactive, file-in / file-out step:

  1. Read the previous round's feature_set from the shared folder (the INPUT).
  2. Serve the dashboard on loopback with that selection PRE-TICKED; the operator
     adds / removes features this round.
  3. On "Save new feature list", validate the selection against the live schema
     catalog, compute the added/removed delta vs the baseline, and write the new
     feature_set (TRD §7.3.4 shape + resolved `features[]`) back to the shared
     folder (the OUTPUT): an atomically-replaced rolling pointer
     `current_feature_set.json`, an immutable `round_<NNN>.json` snapshot, and a
     `CHANGELOG.md` entry.

The page is served from this server, so the save POST is same-origin — no CORS,
no auth, and the server binds 127.0.0.1 only. Opening the generated HTML directly
via file:// still works as the plain, export-only standalone tool (the round
globals this server injects are simply absent there).

Stdlib only. Usage:
    python3 serve_round.py [--shared-dir DIR] [--schemas-dir DIR] [--port N]
                           [--id-stem STEM] [--owner WHO] [--purpose TEXT]
                           [--from-file HTML] [--force] [--no-exit] [--open]
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
import tempfile
import threading
import webbrowser
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

# Same repo-root convention as build_dashboard.py; import it as a sibling module.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_dashboard  # noqa: E402

REPO_ROOT = build_dashboard.REPO_ROOT
DEFAULT_SHARED = REPO_ROOT / "components" / "Data" / "feature_sets"
DEFAULT_SCHEMAS = build_dashboard.DEFAULT_SCHEMAS

CURRENT_FILE = "current_feature_set.json"
CHANGELOG_FILE = "CHANGELOG.md"

# Shared runtime context, populated by main() before the server starts.
CTX: dict = {}


# ---------------------------------------------------------------------------
# Catalog indexing & feature resolution
# ---------------------------------------------------------------------------

def resolved_feature(row: dict) -> dict:
    """Canonical feature dict (mirrors the dashboard's exportPayload fields)."""
    return {
        "column": row["name"],
        "schema": row["schema_id"],
        "group": row["group"],
        "type": row["type"],
        "semantic_type": row["semantic_type"],
        "suitability": row["suitability"],
        "null_semantics": row["null"],
        "source": row["source"],
    }


def index_catalog(catalog: dict) -> dict:
    by_id, by_name, order = {}, {}, {}
    for i, r in enumerate(catalog["rows"]):
        by_id[r["schema_id"] + "::" + r["name"]] = r
        by_name.setdefault(r["name"], r)
        order.setdefault(r["name"], i)
    return {"by_id": by_id, "by_name": by_name, "order": order}


# ---------------------------------------------------------------------------
# Writing the feature_set
# ---------------------------------------------------------------------------

def atomic_write(path: Path, text: str) -> None:
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp_", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def append_changelog(shared: Path, fs: dict) -> None:
    log = shared / CHANGELOG_FILE
    lines = []
    if not log.exists():
        lines.append("# Feature Set Rounds — Change Log\n")
    lines.append(f"\n## Round {fs['round']} — {fs['feature_set_id']}  ({fs['saved_at']})\n")
    lines.append(f"- base: `{fs['base_feature_set'] or '(none)'}`  ·  owner: {fs['owner']}"
                 f"  ·  purpose: {fs['purpose']}  ·  total: {fs['count']}\n")
    lines.append(f"- added ({len(fs['added_features'])}): "
                 f"{', '.join(fs['added_features']) or '—'}\n")
    lines.append(f"- removed ({len(fs['removed_features'])}): "
                 f"{', '.join(fs['removed_features']) or '—'}\n")
    with open(log, "a", encoding="utf-8") as f:
        f.write("".join(lines))


def build_feature_set(submitted_rows: list[dict]) -> dict:
    """Assemble the output feature_set object and compute the delta vs baseline."""
    baseline_cols = CTX["baseline_cols"]            # list, baseline order
    baseline_set = set(baseline_cols)
    submitted_cols = [r["column"] for r in submitted_rows]
    submitted_set = set(submitted_cols)

    added = [c for c in submitted_cols if c not in baseline_set]
    removed = [c for c in baseline_cols if c not in submitted_set]
    n = CTX["round"]
    return {
        "feature_set_id": f"{CTX['id_stem']}_round_{n:03d}",
        "base_feature_set": CTX["base_id"],
        "added_features": added,
        "removed_features": removed,
        "owner": CTX["owner"],
        "purpose": CTX["purpose"],
        "round": n,
        "generated_from": "Signal Prism schemas",
        "schema_snapshot": CTX["snapshot"],
        "saved_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(submitted_rows),
        "features": submitted_rows,
    }


def save_round(payload: dict) -> tuple[int, dict]:
    """Validate + persist. Returns (http_status, json_response)."""
    feats = payload.get("features")
    if not isinstance(feats, list):
        return 400, {"ok": False, "error": "payload must contain a 'features' array"}

    idx = CTX["index"]
    resolved, unknown = [], []
    for f in feats:
        col = (f or {}).get("column")
        if not col:
            continue
        schema = (f or {}).get("schema")
        if schema and (schema + "::" + col) in idx["by_id"]:
            resolved.append(resolved_feature(idx["by_id"][schema + "::" + col]))
        elif col in idx["by_name"]:
            resolved.append(resolved_feature(idx["by_name"][col]))
        else:
            unknown.append(col)
    if unknown:
        return 400, {"ok": False, "error": "unknown column(s) not in the schema catalog",
                     "details": unknown}
    if not resolved:
        return 400, {"ok": False, "error": "no valid features submitted"}

    fs = build_feature_set(resolved)
    shared: Path = CTX["shared"]
    snapshot = shared / f"round_{fs['round']:03d}_feature_set.json"
    if snapshot.exists() and not CTX["force"]:
        return 400, {"ok": False,
                     "error": f"snapshot {snapshot.name} already exists; re-run with --force to overwrite"}

    text = json.dumps(fs, ensure_ascii=False, indent=2) + "\n"
    atomic_write(snapshot, text)
    atomic_write(shared / CURRENT_FILE, text)
    append_changelog(shared, fs)

    CTX["saved"] = True
    written = [str(snapshot), str(shared / CURRENT_FILE), str(shared / CHANGELOG_FILE)]
    return 200, {"ok": True, "feature_set_id": fs["feature_set_id"], "round": fs["round"],
                 "written": written, "added": fs["added_features"],
                 "removed": fs["removed_features"], "count": fs["count"]}


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    def _send(self, status: int, body: bytes, ctype: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        if self.path.split("?")[0] not in ("/", "/index.html"):
            self._send(404, b"not found", "text/plain; charset=utf-8")
            return
        self._send(200, CTX["html"].encode("utf-8"), "text/html; charset=utf-8")

    def do_POST(self):  # noqa: N802
        if self.path.split("?")[0] != "/save":
            self._send(404, b'{"ok":false,"error":"not found"}', "application/json")
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            payload = json.loads(self.rfile.read(length) or b"{}")
        except (ValueError, json.JSONDecodeError) as e:
            self._send(400, json.dumps({"ok": False, "error": f"bad request body: {e}"}).encode(),
                       "application/json")
            return
        status, resp = save_round(payload)
        self._send(status, json.dumps(resp).encode("utf-8"), "application/json")
        # Exit-after-save so the pipeline can proceed (unless --no-exit).
        if status == 200 and not CTX["no_exit"]:
            threading.Thread(target=CTX["server"].shutdown, daemon=True).start()

    def log_message(self, fmt, *args):  # concise one-line stdout logs
        sys.stdout.write("  %s - %s\n" % (self.address_string(), fmt % args))


# ---------------------------------------------------------------------------
# Setup & main
# ---------------------------------------------------------------------------

def load_baseline(shared: Path) -> dict | None:
    cur = shared / CURRENT_FILE
    if not cur.exists():
        return None
    try:
        return json.loads(cur.read_text(encoding="utf-8"))
    except (ValueError, json.JSONDecodeError) as e:
        sys.exit(f"error: {cur} is not valid JSON ({e}); fix or remove it before running a round")


def render_round_html(base_html: str, round_meta: dict, preselected: list) -> str:
    inject = ("<script>window.__ROUND__=" + json.dumps(round_meta)
              + ";window.__PRESELECTED__=" + json.dumps(preselected, ensure_ascii=False)
              + ";</script>")
    return base_html.replace("<body>", "<body>\n" + inject, 1)


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Serve the Feature Workbench as a pipeline round.")
    ap.add_argument("--shared-dir", default=str(DEFAULT_SHARED))
    ap.add_argument("--schemas-dir", default=str(DEFAULT_SCHEMAS))
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--id-stem", default="feature_set_candidate")
    ap.add_argument("--owner", default=getpass.getuser())
    ap.add_argument("--purpose", default="offline_floor_simulation")
    ap.add_argument("--from-file", default=None,
                    help="serve a prebuilt HTML instead of rendering from schemas (stale-catalog fallback)")
    ap.add_argument("--force", action="store_true", help="allow overwriting an existing round_<N>.json snapshot")
    ap.add_argument("--no-exit", action="store_true", help="keep serving after a save (multi-save session)")
    ap.add_argument("--open", action="store_true", help="open the page in a browser on startup")
    args = ap.parse_args(argv)

    shared = Path(args.shared_dir).resolve()
    schemas_dir = Path(args.schemas_dir).resolve()
    shared.mkdir(parents=True, exist_ok=True)

    catalog = build_dashboard.build_catalog(schemas_dir)
    snapshot_label = "schemas: " + ", ".join(catalog["files"])
    if args.from_file:
        base_html = Path(args.from_file).resolve().read_text(encoding="utf-8")
    else:
        base_html = build_dashboard.render_html(catalog, snapshot_label)

    base = load_baseline(shared)
    if base:
        base_round = int(base.get("round", -1))
        this_round = base_round + 1          # a -1 "seed" current → first round is 0
        base_id = base.get("feature_set_id")
        baseline_cols = [f.get("column") for f in base.get("features", []) if f.get("column")]
        preselected = base.get("features", [])
        label = "seed" if base_round < 0 else f"round {base_round}"
        print(f"Loaded baseline {base_id} ({label}) with {len(baseline_cols)} features "
              f"from {shared/CURRENT_FILE}")
    else:
        this_round, base_id, baseline_cols, preselected = 0, None, [], []
        print(f"No existing feature_set found in {shared} — starting Round 0 from an empty baseline.")

    round_meta = {"round": this_round, "base_feature_set_id": base_id,
                  "shared_dir": str(shared), "save_url": "/save"}
    html_out = render_round_html(base_html, round_meta, preselected)

    CTX.update({
        "html": html_out, "index": index_catalog(catalog), "snapshot": snapshot_label,
        "shared": shared, "round": this_round, "base_id": base_id,
        "baseline_cols": baseline_cols, "id_stem": args.id_stem, "owner": args.owner,
        "purpose": args.purpose, "force": args.force, "no_exit": args.no_exit, "saved": False,
    })

    # Line-buffer stdout so startup notices + access logs reach a redirected
    # pipeline log promptly rather than being held in a block buffer.
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except (AttributeError, ValueError):
        pass

    server = HTTPServer(("127.0.0.1", args.port), Handler)
    CTX["server"] = server
    url = f"http://127.0.0.1:{args.port}/"
    print(f"Round {this_round} · base: {base_id or '(none)'} · serving {url}")
    print("Open it, add/remove features, then click 'Save new feature list'."
          + ("" if args.no_exit else "  The server exits after the first save."))
    if args.open:
        webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        server.server_close()

    if CTX["saved"]:
        return 0
    if args.no_exit:
        return 0  # stopping a multi-save session by hand is normal
    print("No feature_set was saved this round.", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
