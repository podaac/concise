import papermill as pm
import json
import os
import argparse

from tests.jupyter.cumulus_api import CumulusAPI
from tests.jupyter.enums import Venue


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
                        required=True,
                        metavar='')

    parser.add_argument('-t', '--token',
                        help='Launchpad token json file for collections',
                        required=True,
                        metavar='')

    parser.add_argument('-o', '--output_path',
                        help='output path for success and fails',
                        required=False,
                        metavar='')

    args = parser.parse_args()
    return args

def run():
    """
    Run from command line.

    Returns
    -------
    """

    _args = parse_args()
    environment = _args.env
    notebook = _args.notebook
    tokenFile = _args.token
    output_location = _args.output_path if _args.output_path  else '.'

    notebook = "./tests/jupyter/notebooks/harmony_concise_api_test.ipynb"
    notebook_path = os.path.dirname(notebook)
    notebook_name = os.path.basename(notebook)

    jsonContent = json.loads(tokenFile)
    if "token" in jsonContent.keys():
        token = jsonContent['token']
    else:
        raise Exception(f"Missing field 'token'!\r\nCurrent fields: {jsonContent.keys()}")

    success = []
    fails = []

    venue = Venue.from_str(environment)
    collections = Collections.GetCollectionAsList(token, venue)
    for collection in collections:

        try:
            print(collection)
            pm.execute_notebook(
               notebook,
               f"{notebook_path}/output/{collection}_{environment}_output_{notebook_name}",
               parameters=dict(collection=collection, venue=venue)
            )
            success.append(collection)
        except Exception as ex:
            print(ex)
            fails.append(collection)

    # Create output files
    if output_location:
        success_outfile = f'{output_location}/{_args.env}_success.txt'
        fail_outfile = f'{output_location}/{_args.env}_fail.txt'

        if success:
            with open(success_outfile, 'w') as the_file:
                the_file.writelines(x + '\n' for x in success)

        if fails:
            with open(fail_outfile, 'w') as the_file:
                the_file.writelines(x + '\n' for x in fails)

if __name__ == '__main__':
    run()