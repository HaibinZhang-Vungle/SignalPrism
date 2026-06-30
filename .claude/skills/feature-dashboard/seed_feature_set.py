#!/usr/bin/env python3
"""
Seed the shared feature_sets folder for the FIRST round.

Because every feature comes from ./schemas, this writes a sensible starting point
so the round UI does not open blank:

  - round_000.json          — the EMPTY genesis baseline (round 0, no features).
  - current_feature_set.json — the "normal features" seed the UI pre-loads.
                               Marked round -1 so the first interactive Save lands
                               as round_000_feature_set.json (see serve_round.py).

"Normal features" = columns the schemas tag as directly usable (suitability ==
`feature`), across all schema files by default. Adjust with --suitability.

Stdlib only. Usage:
    python3 seed_feature_set.py [--shared-dir DIR] [--schemas-dir DIR]
                                [--suitability feature[,feature_after_encode]]
                                [--id-stem STEM] [--owner WHO] [--purpose TEXT] [--force]
"""

from __future__ import annotations

import argparse
import getpass
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_dashboard  # noqa: E402
import serve_round      # noqa: E402  (reuse resolved_feature + path defaults)

DEFAULT_SHARED = serve_round.DEFAULT_SHARED
DEFAULT_SCHEMAS = build_dashboard.DEFAULT_SCHEMAS


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_json(path: Path, obj: dict) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Seed current_feature_set.json + round_000.json from schemas.")
    ap.add_argument("--shared-dir", default=str(DEFAULT_SHARED))
    ap.add_argument("--schemas-dir", default=str(DEFAULT_SCHEMAS))
    ap.add_argument("--suitability", default="feature",
                    help="comma-separated suitability values to include (default: feature)")
    ap.add_argument("--id-stem", default="feature_set_candidate")
    ap.add_argument("--owner", default=getpass.getuser())
    ap.add_argument("--purpose", default="offline_floor_simulation")
    ap.add_argument("--force", action="store_true", help="overwrite existing seed files")
    args = ap.parse_args(argv)

    shared = Path(args.shared_dir).resolve()
    schemas_dir = Path(args.schemas_dir).resolve()
    shared.mkdir(parents=True, exist_ok=True)

    wanted = {s.strip() for s in args.suitability.split(",") if s.strip()}
    catalog = build_dashboard.build_catalog(schemas_dir)
    snapshot = "schemas: " + ", ".join(catalog["files"])

    normal = [serve_round.resolved_feature(r) for r in catalog["rows"] if r["suitability"] in wanted]
    if not normal:
        sys.exit(f"error: no columns matched suitability {sorted(wanted)} in {schemas_dir}")

    current = shared / serve_round.CURRENT_FILE
    genesis = shared / "round_000.json"
    if (current.exists() or genesis.exists()) and not args.force:
        sys.exit(f"error: {current.name} / round_000.json already exist in {shared}; "
                 f"re-run with --force to overwrite")

    ts = now_iso()
    empty_fs = {
        "feature_set_id": f"{args.id_stem}_round_000_empty",
        "base_feature_set": None,
        "added_features": [], "removed_features": [],
        "owner": args.owner, "purpose": args.purpose,
        "round": 0,
        "generated_from": "Signal Prism schemas",
        "schema_snapshot": snapshot,
        "saved_at": ts, "count": 0, "features": [],
        "note": "Empty genesis baseline for round 0.",
    }
    seed_fs = {
        "feature_set_id": f"{args.id_stem}_seed",
        "base_feature_set": None,
        "added_features": [f["column"] for f in normal],
        "removed_features": [],
        "owner": args.owner, "purpose": args.purpose,
        "round": -1,
        "generated_from": "Signal Prism schemas",
        "schema_snapshot": snapshot,
        "saved_at": ts, "count": len(normal), "features": normal,
        "note": ("Seed of 'normal' (directly-usable) features the UI pre-loads. "
                 "round=-1 so the first saved round is round_000_feature_set.json."),
    }

    write_json(genesis, empty_fs)
    write_json(current, seed_fs)

    print(f"Seeded {shared}")
    print(f"  round_000.json            empty genesis (round 0, 0 features)")
    print(f"  current_feature_set.json  {len(normal)} normal features "
          f"(suitability {sorted(wanted)})")
    by_schema: dict[str, int] = {}
    for f in normal:
        by_schema[f["schema"]] = by_schema.get(f["schema"], 0) + 1
    for sname, n in sorted(by_schema.items()):
        print(f"    {sname}: {n}")
    print("Next: python3 .claude/skills/feature-dashboard/serve_round.py")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
