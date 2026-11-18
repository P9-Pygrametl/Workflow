"""Tune each implementation with a coordinate-descent search.

Every pass evaluates all levels of one knob while the other knobs are
fixed. The current level is included in each sweep, so the comparison is
made against a fresh measurement rather than an old baseline.

Example:
    .venv/bin/python -m run_all.tune --pages 100 --repeats 3
"""

import argparse
import json
import os
import random
import sqlite3
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datagenerator.datagenerator_db import generate
from run_all.config import (
    DW_CONFIG,
    MODES,
    ROOT,
    SCHEMA_FILE,
    SOURCE_CONFIG,
    TUNED_FILE,
)
from run_all.measure import run_child
from run_all.tuning import (
    LEVELS,
    apply_params,
    build_environment,
    default_params,
    knobs_for,
)
from run_all.validation import validate, warehouse_state


TUNING_DB = ROOT / "data" / "tuning.db"


@dataclass
class Trial:
    wall_seconds: float | None
    cpu_seconds: float | None
    peak_pss_mb: float | None
    peak_rss_mb: float | None
    valid: bool
    error: str | None


class TrialStore:
    """SQLite log of every tuning run, usable for resuming."""

    def __init__(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS trials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                mode TEXT NOT NULL,
                pages INTEGER NOT NULL,
                pass_index INTEGER NOT NULL,
                knob TEXT NOT NULL,
                params TEXT NOT NULL,
                repeat INTEGER NOT NULL,
                wall_seconds REAL,
                cpu_seconds REAL,
                peak_pss_mb REAL,
                peak_rss_mb REAL,
                valid INTEGER NOT NULL,
                error TEXT
            )
            """
        )
        self.connection.commit()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.connection.close()

    def find(self, mode, pages, pass_index, knob, params, repeat):
        row = self.connection.execute(
            """
            SELECT wall_seconds, cpu_seconds, peak_pss_mb, peak_rss_mb,
                   valid, error
            FROM trials
            WHERE mode = ? AND pages = ? AND pass_index = ?
              AND knob = ? AND params = ? AND repeat = ?
            LIMIT 1
            """,
            (mode, pages, pass_index, knob, params, repeat),
        ).fetchone()
        if row is None:
            return None
        return Trial(*row)

    def save(self, mode, pages, pass_index, knob, params, repeat, trial):
        self.connection.execute(
            """
            INSERT INTO trials (
                timestamp, mode, pages, pass_index, knob, params, repeat,
                wall_seconds, cpu_seconds, peak_pss_mb, peak_rss_mb,
                valid, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                time.asctime(),
                mode,
                pages,
                pass_index,
                knob,
                params,
                repeat,
                trial.wall_seconds,
                trial.cpu_seconds,
                trial.peak_pss_mb,
                trial.peak_rss_mb,
                int(trial.valid),
                trial.error,
            ),
        )
        self.connection.commit()


def reset_warehouse():
    with psycopg.connect(**DW_CONFIG) as connection:
        connection.execute(SCHEMA_FILE.read_text())


def expected_fact_rows():
    with psycopg.connect(**SOURCE_CONFIG) as connection:
        return connection.execute("SELECT count(*) FROM testresults").fetchone()[0]


def child_environment(mode):
    return build_environment(os.environ, mode.env)


def run_trial(
    mode,
    params,
    pages,
    pass_index,
    knob,
    repeat,
    store,
    expected_facts,
):
    params_json = json.dumps(params, sort_keys=True)
    existing = store.find(
        mode.key, pages, pass_index, knob, params_json, repeat
    )
    if existing is not None:
        print("      reused", end="", flush=True)
        return existing

    tuned = apply_params(mode, params)
    environment = child_environment(tuned)
    last_error = None

    for attempt in range(2):
        reset_warehouse()
        measurement = run_child(tuned.command, environment)
        state = warehouse_state(deep_validate=False, config=DW_CONFIG)
        errors = validate(measurement, state, deep_validate=False)

        if state["testresults"] != expected_facts:
            errors.append(
                f"expected {expected_facts} fact rows, "
                f"found {state['testresults']}"
            )

        if not errors:
            trial = Trial(
                wall_seconds=measurement.wall_seconds,
                cpu_seconds=measurement.cpu_seconds,
                peak_pss_mb=measurement.peak_pss_mb,
                peak_rss_mb=measurement.peak_rss_mb,
                valid=True,
                error=None,
            )
            store.save(
                mode.key,
                pages,
                pass_index,
                knob,
                params_json,
                repeat,
                trial,
            )
            return trial

        last_error = "; ".join(errors)
        print(f"      attempt {attempt + 1} invalid: {last_error}")

    trial = Trial(
        wall_seconds=measurement.wall_seconds,
        cpu_seconds=measurement.cpu_seconds,
        peak_pss_mb=measurement.peak_pss_mb,
        peak_rss_mb=measurement.peak_rss_mb,
        valid=False,
        error=last_error,
    )
    store.save(
        mode.key,
        pages,
        pass_index,
        knob,
        params_json,
        repeat,
        trial,
    )
    return trial


def evaluate_levels(
    mode,
    params,
    pages,
    pass_index,
    knob,
    repeats,
    store,
    expected_facts,
):
    """Evaluate every level of one knob, interleaving levels by repeat."""
    levels = list(LEVELS[knob])

    if params[knob] not in levels:
        levels.insert(0, params[knob])

    trials = {level: [] for level in levels}

    for repeat in range(repeats):
        for level in levels:
            trial_params = dict(params)
            trial_params[knob] = level
            print(
                f"    {knob}={level} repeat {repeat + 1}/{repeats}",
                end="",
                flush=True,
            )
            trial = run_trial(
                mode,
                trial_params,
                pages,
                pass_index,
                knob,
                repeat,
                store,
                expected_facts,
            )
            trials[level].append(trial)
            wall = trial.wall_seconds
            print(
                f"  wall={wall:.2f}s"
                if wall is not None
                else f"  INVALID {trial.error or ''}"
            )

    medians = {}
    for level, results in trials.items():
        valid_walls = [
            result.wall_seconds
            for result in results
            if result.valid and result.wall_seconds is not None
        ]
        medians[level] = (
            statistics.median(valid_walls)
            if len(valid_walls) == repeats
            else None
        )

    candidates = {
        level: median
        for level, median in medians.items()
        if median is not None
    }

    if not candidates:
        return params[knob], medians

    best = min(candidates, key=candidates.get)
    return best, medians


def confirm_params(
    mode,
    params,
    pages,
    pass_index,
    repeats,
    store,
    expected_facts,
):
    """Run the winning configuration again and return its median wall time."""
    values = []
    for repeat in range(repeats):
        trial = run_trial(
            mode,
            params,
            pages,
            pass_index,
            "final",
            repeat,
            store,
            expected_facts,
        )
        if trial.valid and trial.wall_seconds is not None:
            values.append(trial.wall_seconds)

    return statistics.median(values) if len(values) == repeats else None


def tune_mode(mode, pages, repeats, passes, seed, store, expected_facts):
    params = default_params(mode)
    knobs = knobs_for(mode)
    completed_passes = 0

    print(f"\n=== {mode.key} ===")
    print(f"knobs: {', '.join(knobs)}")
    print(f"start: {json.dumps(params, sort_keys=True)}")

    for pass_index in range(1, passes + 1):
        order = list(knobs)
        random.Random(seed + pass_index).shuffle(order)
        changed = False

        print(f"\n  pass {pass_index}/{passes}: {', '.join(order)}")

        for knob in order:
            current = params[knob]
            print(f"  {knob}: current={current}")

            best, medians = evaluate_levels(
                mode,
                params,
                pages,
                pass_index,
                knob,
                repeats,
                store,
                expected_facts,
            )

            shown = ", ".join(
                f"{level}={median:.2f}s" if median is not None
                else f"{level}=invalid"
                for level, median in sorted(
                    medians.items(),
                    key=lambda item: str(item[0]),
                )
            )
            print(f"    medians: {shown}")

            if best != current:
                params[knob] = best
                changed = True
                print(f"    winner: {knob}={best}")
            else:
                print(f"    keep: {knob}={current}")

        completed_passes = pass_index
        print(f"  after pass {pass_index}: {json.dumps(params, sort_keys=True)}")

        if not changed:
            print("  converged")
            break

    final_median = confirm_params(
        mode,
        params,
        pages,
        passes + 1,
        repeats,
        store,
        expected_facts,
    )

    print(f"  final: {json.dumps(params, sort_keys=True)}")
    if final_median is not None:
        print(f"  final median: {final_median:.2f}s")
    else:
        print("  final confirmation failed")

    return params, final_median, completed_passes


def update_profile(path, mode, params, pages, repeats, passes, median):
    if path.exists():
        profile = json.loads(path.read_text())
    else:
        profile = {}

    profile[mode.key] = {
        "implementation": mode.implementation,
        "runtime": mode.runtime,
        "params": params,
        "pages": pages,
        "repeats": repeats,
        "passes": passes,
        "median_wall_seconds": median,
        "timestamp": time.asctime(),
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(profile, indent=2, sort_keys=True) + "\n")


def select_modes(names):
    if not names:
        return list(MODES)

    selected = [
        mode
        for mode in MODES
        if mode.key in names
        or mode.label in names
        or mode.implementation in names
    ]

    if not selected:
        raise ValueError(f"No modes matched {names!r}")

    return selected


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Tune each implementation with coordinate descent. "
            "The warehouse is reset before every trial."
        )
    )
    parser.add_argument("--pages", type=int, default=100)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--passes", type=int, default=3)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument(
        "--output",
        type=Path,
        default=TUNED_FILE,
        help=f"Tuned profile (default: {TUNED_FILE}).",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=TUNING_DB,
        help=f"Trial database (default: {TUNING_DB}).",
    )
    parser.add_argument(
        "--modes",
        nargs="*",
        help="Optional mode keys, labels, or implementation names.",
    )
    parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="Use the source data already in the database.",
    )
    parser.add_argument(
        "--probe",
        action="store_true",
        help="Run every mode's default once and stop, for timing estimates.",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Delete the trial database before starting.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the search plan without running anything.",
    )
    args = parser.parse_args()

    if args.pages < 1:
        parser.error("--pages must be positive")
    if args.repeats < 1:
        parser.error("--repeats must be at least 1")
    if args.passes < 1:
        parser.error("--passes must be at least 1")

    return args


def main():
    args = parse_args()

    try:
        modes = select_modes(args.modes)
    except ValueError as error:
        print(error)
        return 1

    if args.dry_run:
        total = 0
        for mode in modes:
            knobs = knobs_for(mode)
            evaluations = sum(len(LEVELS[knob]) for knob in knobs)
            runs = evaluations * args.repeats * args.passes
            total += runs
            print(
                f"{mode.key}: {', '.join(knobs)} "
                f"({evaluations} evaluations/pass, {runs} runs max)"
            )
        print(f"\nTotal runs: {total} (plus {len(modes) * args.repeats} confirmations)")
        return 0

    if args.fresh and args.db.exists():
        print(f"Removing {args.db}")
        args.db.unlink()

    if not args.skip_generate:
        print(f"Generating source data for pages={args.pages}...")
        generate(args.pages)

    expected_facts = expected_fact_rows()
    print(f"Expected fact rows: {expected_facts}")

    args.output.parent.mkdir(parents=True, exist_ok=True)

    if args.probe:
        with TrialStore(args.db) as store:
            for mode in modes:
                params = default_params(mode)
                print(f"\n=== probe {mode.key} ===")
                print(f"  {json.dumps(params, sort_keys=True)}")
                for repeat in range(args.repeats):
                    trial = run_trial(
                        mode,
                        params,
                        args.pages,
                        0,
                        "probe",
                        repeat,
                        store,
                        expected_facts,
                    )
                    if trial.wall_seconds is not None:
                        print(f"  wall={trial.wall_seconds:.2f}s")
                    else:
                        print(f"  INVALID {trial.error or ''}")
        print(f"\nTrial log: {args.db.resolve()}")
        return 0

    with TrialStore(args.db) as store:
        for mode in modes:
            params, median, completed = tune_mode(
                mode,
                args.pages,
                args.repeats,
                args.passes,
                args.seed,
                store,
                expected_facts,
            )
            update_profile(
                args.output,
                mode,
                params,
                args.pages,
                args.repeats,
                completed,
                median,
            )
            print(f"  wrote {mode.key} to {args.output}")

    print(f"\nTrial log: {args.db.resolve()}")
    print(f"Tuned profile: {args.output.resolve()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
