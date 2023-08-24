import os
import matplotlib.pyplot as plt
import netCDF4 as nc
import xarray as xr
from harmony import BBox, Client, Collection, Request, Environment
import argparse

from os import path

from utils import FileHandler
from utils.enums import Venue
import itertools


def parse_args():
    """
    Parses the program arguments
    Returns
    -------
    args
    """

    parser = argparse.ArgumentParser(
        description='Update CMR with latest profile',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('-e', '--env',
                        help='CMR environment used to pull results from.',
                        required=True,
                        choices=["uat", "ops", "ngap_uat", "ngap_ops"],
                        metavar='uat or ops')

    parser.add_argument('-n', '--notebook',
                        help='Notebook to run',
                        required=False,
                        metavar='')

    parser.add_argument('-i', '--input_file',
                        help='File of json collections',
                        required=True,
                        metavar='')

    parser.add_argument('-o', '--output_path',
                        help='output path for success and fails',
                        required=False,
                        metavar='')

    args = parser.parse_args()
    return args


def get_username_and_password(venue):
    if venue.lower() == "uat":
        return os.environ.get("UAT_USERNAME"), os.environ.get("UAT_PASSWORD")
    elif venue.lower() == "ops":
        return os.environ.get('OPS_USERNAME'), os.environ.get('OPS_PASSWORD')
    else:
        raise ValueError("Invalid venue")


def get_x_y_variables(variables):
    x_var_candidates = ["lon", "longitude", "beam_clon", "sp_lon", "cellon"]
    y_var_candidates = ["lat", "latitude", "beam_clat", "sp_lat", "cellat"]

    x_var, y_var = None, None
    for var in variables:
        if x_var is None and var in x_var_candidates:
            x_var = var
        if y_var is None and var in y_var_candidates:
            y_var = var
        if x_var and y_var:
            break

    return x_var, y_var


def test(collection_id, venue):

    max_results = 2

    username, password = get_username_and_password(venue)
    environment = Environment.UAT if venue == "UAT" else Environment.PROD
    harmony_client = Client(auth=(username, password), env=environment)

    collection = Collection(id=collection_id)

    request = Request(
        collection=collection,
        concatenate=True,
        max_results=max_results,
        skip_preview=True,
        format="application/x-netcdf4",
    )

    request.is_valid()

    print(harmony_client.request_as_curl(request))

    job1_id = harmony_client.submit(request)

    print(f'\n{job1_id}')

    print(harmony_client.status(job1_id))

    print('\nWaiting for the job to finish')

    results = harmony_client.result_json(job1_id)

    print('\nDownloading results:')

    futures = harmony_client.download_all(job1_id)
    file_names = [f.result() for f in futures]
    print('\nDone downloading.')

    filename = file_names[0]
    # Handle time dimension and variables dropping
    dt = nc.Dataset(filename, 'r')
    groups = list(dt.groups)
    dt.close()

    drop_variables = [
        'time',
        'sample',
        'meas_ind',
        'wvf_ind',
        'ddm',
        'averaged_l1'
    ]
    if not groups:
        groups = [None]

    for group in groups:

        ds = xr.open_dataset(filename, group=group, decode_times=False, drop_variables=drop_variables)

        assert len(ds.coords['subset_index']) == max_results
        variables = list(ds.variables)
        x_var, y_var = get_x_y_variables(variables)

        for v in variables:
            if v not in ['subset_files', 'lat', 'lon', 'latitude', 'longitude', 'beam_clat', 'beam_clon']:
                variable = v
                break

        if x_var is not None and y_var is not None:
            break

        ds.close()

    if x_var is None or y_var is None:
        raise Exception("Lon and Lat variables are not found")

    for index in range(0, max_results):
        ax = ds.isel(subset_index=index).plot.scatter(
            y=y_var,
            x=x_var,
            hue=variable,
            s=1,
            levels=9,
            cmap="jet",
            aspect=2.5,
            size=9
        )
        plt.xlim(0., 360.)
        plt.ylim(-90., 90.)
        #plt.show(block=False)
        plt.clf()
        plt.close(ax.figure)

def run():
    """
    Run from command line.

    Returns
    -------
    """

    _args = parse_args()
    environment = _args.env
    notebook = _args.notebook
    inputFile = _args.input_file
    output_location = _args.output_path if _args.output_path else '.'

    success = []
    fails = []

    if path.exists(inputFile):
        venue = Venue.from_str(environment)
        collections = FileHandler.get_file_content_list_per_line(inputFile)
        print(collections)
        # limit number of collections tested to 1
        for collection in itertools.islice(collections, 1):
            if "POCLOUD" not in collection and venue == "uat":
                continue

            try:
                print(collection)
                test(collection, venue.name)
                success.append(collection)
            except Exception as ex:
                print(ex)
                fails.append(collection)

        # Create output files
        #if output_location:
        #    success_outfile = path.realpath(f'{output_location}/{_args.env}_success.txt')
        #    fail_outfile = path.realpath(f'{output_location}/{_args.env}_fail.txt')

        #    if success:
        #        with open(success_outfile, 'w') as the_file:
        #            the_file.writelines(x + '\n' for x in success)

        #    if fails:
        #        with open(fail_outfile, 'w') as the_file:
        #            the_file.writelines(x + '\n' for x in fails)


if __name__ == '__main__':
    print("Start running test .......")
    run()
