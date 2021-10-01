"""A simple CLI wrapper around the main merge function"""

from argparse import ArgumentParser
import logging
from pathlib import Path

from podaac.merger.merge import merge_netcdf_files


def main():
    """Main CLI entrypoint"""

    parser = ArgumentParser(
        prog='merge',
        description='Simple CLI wrapper around the granule merge module.')
    parser.add_argument(
        'data_dir',
        help='The directory containing the files to be merged.')
    parser.add_argument(
        'output_path',
        help='The output filename for the merged output.')
    parser.add_argument(
        '-v', '--verbose',
        help='Enable verbose output to stdout; useful for debugging',
        action='store_true'
    )
    parser.add_argument(
        '-c', '--cores',
        help='Override the number of cores to be utilized during multitreaded/multiprocess operations. Defaults to cpu_count',
        type=int,
        default=None
    )

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    input_files = list(Path(args.data_dir).resolve().iterdir())
    merge_netcdf_files(input_files, args.output_path, process_count=args.cores)


if __name__ == '__main__':
    main()
