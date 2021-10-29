"""A Harmony CLI wrapper around Concise"""

from argparse import ArgumentParser
import harmony
from podaac.merger.harmony.service import ConciseService


def main(config=None):
    """Main Harmony CLI entrypoint"""

    parser = ArgumentParser()
    harmony.setup_cli(parser)

    args = parser.parse_args()
    if harmony.is_harmony_cli(args):
        harmony.run_cli(parser, args, ConciseService, cfg=config)
    else:
        parser.error("Only --harmony CLIs are supported")


if __name__ == "__main__":
    main()
