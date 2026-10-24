"""Land batches from the command line.

    python -m generator --out ./landing              # next batch not yet landed
    python -m generator --out ./landing --batches 0-3
    python -m generator --out ./landing --batches all
"""
from __future__ import annotations

import argparse
import json

from . import Config, build_world, landed_batches, plan_batches, write_batch


def select_batches(spec: str, landed: list[str], total: int) -> list[int]:
    if spec == "next":
        done = {int(b.split("_")[1]) for b in landed}
        nxt = next((i for i in range(total) if i not in done), None)
        return [] if nxt is None else [nxt]
    if spec == "all":
        return list(range(total))
    if "-" in spec:
        lo, hi = (int(x) for x in spec.split("-"))
        return list(range(lo, hi + 1))
    return [int(x) for x in spec.split(",")]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", required=True, help="landing root, e.g. /Volumes/retail/raw/landing")
    parser.add_argument("--batches", default="next", help="next | all | 3 | 0-5 | 1,2,3")
    args = parser.parse_args()

    cfg = Config()
    batches = plan_batches(cfg)
    wanted = select_batches(args.batches, landed_batches(args.out), len(batches))
    if not wanted:
        print("All batches are already landed.")
        return
    world = build_world(cfg)
    for i in wanted:
        manifest = write_batch(world, batches[i], args.out)
        print(json.dumps({k: manifest[k] for k in ("batch_id", "first_day", "last_day", "orders", "returns")}))


if __name__ == "__main__":
    main()
