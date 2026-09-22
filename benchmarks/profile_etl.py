import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent

# ETL scripts use relative paths for generated source files.
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from profile_runtime import profile  # noqa: E402


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "implementation",
        choices=["database"],
        help="ETL implementation to profile",
    )
    args = parser.parse_args()

    result = profile(args.implementation)
    print()
    print("PROFILE_RESULT=" + json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()