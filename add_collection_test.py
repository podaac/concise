import os
from os import path
from urllib.parse import urlparse
import itertools
import unittest
import numpy as np
import netCDF4 as nc
import requests
from harmony import BBox, Client, Collection, Request, Environment
import argparse
from utils import FileHandler
from utils.enums import Venue


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


def verify_dims(merged_group, origin_group, both_merged):
    for dim in origin_group.dimensions:
        if both_merged:
            unittest.TestCase().assertEqual(merged_group.dimensions[dim].size, origin_group.dimensions[dim].size)
        else:
            unittest.TestCase().assertGreaterEqual(merged_group.dimensions[dim].size, origin_group.dimensions[dim].size)


def verify_attrs(merged_obj, origin_obj, both_merged):
    ignore_attributes = [
        'request-bounding-box', 'request-bounding-box-description', 'PODAAC-dataset-shortname',
        'PODAAC-persistent-ID', 'time_coverage_end', 'time_coverage_start'
    ]

    merged_attrs = merged_obj.ncattrs()
    origin_attrs = origin_obj.ncattrs()

    for attr in origin_attrs:
        if attr in ignore_attributes:
            # Skip attributes which are present in the Java implementation,
            # but not (currently) present in the Python implementation
            continue

        if not both_merged and attr not in merged_attrs:
            # Skip attributes which are not present in both merged and origin.
            # This is normal operation as some attributes may be omited b/c
            # they're inconsistent between granules
            continue

        merged_attr = merged_obj.getncattr(attr)
        if both_merged and isinstance(merged_attr, int):
            # Skip integer values - the Java implementation seems to omit
            # these values due to its internal handling of all values as
            # Strings
            continue

        origin_attr = origin_obj.getncattr(attr)
        if isinstance(origin_attr, np.ndarray):
            unittest.TestCase().assertTrue(np.array_equal(merged_attr, origin_attr))
        else:
            if attr != "history_json":
                unittest.TestCase().assertEqual(merged_attr, origin_attr)


def verify_variables(merged_group, origin_group, subset_index, both_merged):
    for var in origin_group.variables:
        merged_var = merged_group.variables[var]
        origin_var = origin_group.variables[var]

        verify_attrs(merged_var, origin_var, both_merged)

        if both_merged:
            # both groups require subset indexes
            merged_data = merged_var[subset_index[0]]
            origin_data = origin_var[subset_index[1]]
        else:
            # merged group requires a subset index
            merged_data = np.resize(merged_var[subset_index], origin_var.shape)
            origin_data = origin_var

        # verify variable data
        if isinstance(origin_data, str):
            unittest.TestCase().assertEqual(merged_data, origin_data)
        else:
            unittest.TestCase().assertTrue(np.array_equal(merged_data, origin_data, equal_nan=True))


def verify_groups(merged_group, origin_group, subset_index, both_merged=False):
    verify_dims(merged_group, origin_group, both_merged)
    verify_attrs(merged_group, origin_group, both_merged)
    verify_variables(merged_group, origin_group, subset_index, both_merged)

    for child_group in origin_group.groups:
        merged_subgroup = merged_group[child_group]
        origin_subgroup = origin_group[child_group]
        verify_groups(merged_subgroup, origin_subgroup, subset_index, both_merged)


# GET TOKEN FROM CMR
def get_token(cmr_root, username, password):
    token_api = "https://{}/api/users/tokens".format(cmr_root)
    response = requests.get(token_api, auth=(username, password))
    content = response.json()
    if len(content) > 0:
        return content[0].get('access_token')
    else:
        create_token_api = "https://{}/api/users/token".format(cmr_root)
        response = requests.post(create_token_api, auth=(username, password))
        content = response.json()
        return content.get('access_token')


def download_file(url, local_path, headers):
    response = requests.get(url, stream=True, headers=headers)
    if response.status_code == 200:
        with open(local_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print("Original File downloaded successfully.")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")


def test(collection_id, venue):

    max_results = 2

    username, password = get_username_and_password(venue)
    environment = Environment.UAT if venue.lower() == "uat" else Environment.PROD
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
    merge_dataset = nc.Dataset(filename, 'r')

    cmr_base_url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json?readable_granule_name="
    edl_root = 'urs.earthdata.nasa.gov'

    if venue.lower() == 'uat':
        cmr_base_url = "https://cmr.uat.earthdata.nasa.gov/search/granules.umm_json?readable_granule_name="
        edl_root = 'uat.urs.earthdata.nasa.gov'

    token = get_token(edl_root, username, password)
    headers = {
        "Authorization": f"Bearer {token}"
    }

    original_files = merge_dataset.variables['subset_files']
    assert len(original_files) == max_results

    for file in original_files:

        file_name = file.rsplit(".", 1)[0]
        print(file_name)
        cmr_query = f"{cmr_base_url}{file_name}&collection_concept_id={collection_id}"
        print(cmr_query)

        response = requests.get(cmr_query, headers=headers)

        result = response.json()
        links = result.get('items')[0].get('umm').get('RelatedUrls')
        for link in links:
            if link.get('Type') == 'GET DATA':
                data_url = link.get('URL')
                parsed_url = urlparse(data_url)
                local_file_name = os.path.basename(parsed_url.path)
                download_file(data_url, local_file_name, headers)

    for i, file in enumerate(original_files):
        origin_dataset = nc.Dataset(file)
        verify_groups(merge_dataset, origin_dataset, i)


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
        if output_location:
            success_outfile = path.realpath(f'{output_location}/{_args.env}_success.txt')
            fail_outfile = path.realpath(f'{output_location}/{_args.env}_fail.txt')

            if success:
                with open(success_outfile, 'w') as the_file:
                    the_file.writelines(x + '\n' for x in success)

            if fails:
                with open(fail_outfile, 'w') as the_file:
                    the_file.writelines(x + '\n' for x in fails)


if __name__ == '__main__':
    print("Start running test .......")
    run()
