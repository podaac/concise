from argparse import ArgumentParser
import csv
from os import environ, remove
from pathlib import Path
from shutil import copyfileobj
from tempfile import mkstemp

import requests

from podaac.merger.merge import merge_netcdf_files


def main():
    parser = ArgumentParser(
        prog='benchmark',
        description='A benchmarking script for the merge module'
    )
    parser.add_argument(
        'output_path',
        help='An output path for the benchmark report'
    )
    parser.add_argument(
        '-c', '--cores',
        help='Override the number of cores to be utilized during multitreaded/multiprocess operations. Defaults to cpu_count',
        type=int,
        default=None
    )
    parser.add_argument(
        '-l', '--granule-list',
        help='A text file containing URLs of granules to use during benchmarking session (default: internal list in module)',
        default=None
    )
    parser.add_argument(
        '-d', '--granule-dir',
        help='The directory to store granules for benchmarking (default: `granules` folder in module)',
        default=None
    )
    parser.add_argument(
        '-r', '--runs',
        help='Number of runs to perform during benchmarking (default: 5)',
        type=int,
        default=5
    )

    args = parser.parse_args()
    current_dir = Path(__file__).parent

    if args.granule_list is None:
        list_path = current_dir.joinpath('granule_list.txt')
    else:
        list_path = Path(args.granule_list)

    if args.granule_dir is None:
        granules_dir = current_dir.joinpath('granules')
    else:
        granules_dir = Path(args.granule_dir)

    granule_list = open(list_path, mode='r').read().splitlines()
    runs = args.runs

    # -- Precheck --
    print('Checking granule set...')
    download_granule_set(granules_dir, granule_list)

    # -- Benchmarking run --
    session_stats = list()
    granule_set = list(granules_dir.iterdir())
    temp_merge_path = mkstemp()[1]

    for i in range(1, runs + 1):
        print('Running benchmark {}/{}'.format(i, runs))

        run_stats = dict()
        merge_netcdf_files(granule_set, temp_merge_path, perf_stats=run_stats, process_count=args.cores)
        session_stats.append(run_stats)

    remove(temp_merge_path)

    # -- Report output --
    with open(args.output_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Run #', 'Preprocessing', 'Merging', 'Metadata', 'Total (Seconds)', 'Total (Minutes)'])

        for i, run_stats in enumerate(session_stats):
            sum_secs = run_stats['preprocess'] + run_stats['merge'] + run_stats['metadata']
            sum_mins = sum_secs / 60
            writer.writerow([i + 1, run_stats['preprocess'], run_stats['merge'], run_stats['metadata'], sum_secs, sum_mins])

        averages = calc_averages(session_stats)
        sum_secs = averages['preprocess'] + averages['merge'] + averages['metadata']
        sum_mins = sum_secs / 60
        writer.writerow(['Average', averages['preprocess'], averages['merge'], averages['metadata'], sum_secs, sum_mins])

    print('Report written to {}'.format(args.output_path))


def calc_averages(session_stats):
    averages = dict()
    runs = len(session_stats)

    for run_stats in session_stats:
        for key in run_stats:
            if key not in averages:
                averages[key] = run_stats[key]
            else:
                averages[key] = averages[key] + run_stats[key]

    for key in averages:
        averages[key] = averages[key] / runs

    return averages


def download_granule_set(granules_dir, granule_list):
    session = requests.Session()

    granules_dir.mkdir(exist_ok=True)
    session.headers.update({'Authorization': 'Bearer {}'.format(environ.get('EARTHDATA_TOKEN'))})

    for url in granule_list:
        filename = url.rsplit('/', 1)[1]
        file_path = granules_dir.joinpath(filename)

        if not file_path.exists():
            print('Downloading {}'.format(filename))

            with session.get(url, stream=True) as res, open(file_path, mode='xb') as file:
                res.raise_for_status()
                copyfileobj(res.raw, file)


if __name__ == '__main__':
    main()
