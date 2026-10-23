"""Warehouse validation for the unified benchmark runner."""

import json

import psycopg


STRUCTURAL_QUERY = """
    SELECT
        (SELECT count(*) FROM pygrametlexa.page),
        (SELECT count(*) FROM pygrametlexa.test),
        (SELECT count(*) FROM pygrametlexa.date),
        (SELECT count(*) FROM pygrametlexa.testresults),
        (SELECT count(*) FROM pygrametlexa.testresults f
         WHERE NOT EXISTS (
             SELECT 1 FROM pygrametlexa.page p
             WHERE p.pageid = f.pageid
         )),
        (SELECT count(*) FROM pygrametlexa.testresults f
         WHERE NOT EXISTS (
             SELECT 1 FROM pygrametlexa.test t
             WHERE t.testid = f.testid
         )),
        (SELECT count(*) FROM pygrametlexa.testresults f
         WHERE NOT EXISTS (
             SELECT 1 FROM pygrametlexa.date d
             WHERE d.dateid = f.dateid
         ))
"""

FINGERPRINT_QUERY = """
    WITH rows AS (
        SELECT to_jsonb(p) - 'pageid' AS doc
        FROM pygrametlexa.page p

        UNION ALL

        SELECT to_jsonb(t) - 'testid'
        FROM pygrametlexa.test t

        UNION ALL

        SELECT to_jsonb(d) - 'dateid'
        FROM pygrametlexa.date d

        UNION ALL

        SELECT jsonb_build_object(
            'url', p.url,
            'version', p.version,
            'validfrom', p.validfrom,
            'validto', p.validto,
            'test', t.testname,
            'date', d.date,
            'errors', f.errors
        )
        FROM pygrametlexa.testresults f
        JOIN pygrametlexa.page p ON p.pageid = f.pageid
        JOIN pygrametlexa.test t ON t.testid = f.testid
        JOIN pygrametlexa.date d ON d.dateid = f.dateid
    )
    SELECT count(*),
           coalesce(sum(hashtextextended(doc::text, 0)::numeric), 0)
    FROM rows
"""

STATE_KEYS = (
    "page",
    "test",
    "date",
    "testresults",
    "orphan_page",
    "orphan_test",
    "orphan_date",
)


def warehouse_state(deep_validate, config):
    """Return row counts, orphan counts, and an optional fingerprint.

    The fingerprint hashes every dimension row without its surrogate
    key and every fact row denormalized to business keys. One combined
    checksum keeps the validation cheap and the implementation small.
    """
    state = {key: None for key in STATE_KEYS}
    state["signature"] = None

    try:
        connection = psycopg.connect(**config)
    except psycopg.Error:
        return state

    try:
        with connection.cursor() as cursor:
            cursor.execute(STRUCTURAL_QUERY)

            for key, value in zip(STATE_KEYS, cursor.fetchone()):
                state[key] = value

            if deep_validate:
                cursor.execute(FINGERPRINT_QUERY)
                row_count, checksum = cursor.fetchone()
                state["signature"] = json.dumps(
                    {
                        "rows": row_count,
                        "checksum": str(checksum),
                    },
                    sort_keys=True,
                )
    except psycopg.Error:
        pass
    finally:
        connection.close()

    return state


def validate(measurement, state, deep_validate):
    """Return a list of correctness problems for one run."""
    errors = []

    if measurement.returncode is None:
        errors.append("implementation could not be launched")
    elif measurement.returncode != 0:
        errors.append(f"implementation exited with code {measurement.returncode}")

    if state["testresults"] in (None, 0):
        errors.append("fact table is empty or unreadable")

    for key, dimension in (
        ("orphan_page", "page"),
        ("orphan_test", "test"),
        ("orphan_date", "date"),
    ):
        orphaned = state[key]

        if orphaned:
            errors.append(f"{orphaned} fact rows reference missing {dimension} rows")

    if deep_validate and state["signature"] is None:
        errors.append("warehouse fingerprint could not be computed")

    return errors
