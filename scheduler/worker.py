"""Start one scheduler node:   python -m scheduler.worker --node n1"""
import argparse
import logging
import signal
import sys

from .config import load
from .node import SchedulerNode
from .store import Store


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)   # JSON lines

    settings = load()
    store = Store(settings.db_path, settings.partitions)
    store.init_schema()
    node = SchedulerNode(args.node, store, settings)
    signal.signal(signal.SIGTERM, lambda *_: node.stop_event.set())
    node.run_forever()


if __name__ == "__main__":
    main()
