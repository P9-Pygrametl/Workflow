"""
Usage:

    from phase_helper import Phase
    # Example 1
    with Phase("row_transform") as p:
        ...do the row transform...
        p.rows = 9000000
    
    # Example 2
    with Phase("dimension_lookup") as p:
        ...
        p.rows = 9000000

    # Example 3
    with Phase("total") as p:
        with Phase("row_transform") as rt:
            ...
        with Phase("dimension_lookup") as dl:
            ...
        p.rows = 9000000
"""

import sys

class Phase:
    def __init__(self, name):
        self.name = name
        self.rows = None

    def __enter__(self):
        print(f"PHASE_START,{self.name}", flush=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        rows = "" if self.rows is None else self.rows
        print(f"PHASE_END,{self.name},{rows}", flush=True)
        return False