import cmr
import papermill as pm
import json
import os
import argparse

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

    parser.add_argument('-c', '--collections',
                        help='List of collection test',
                        required=False,
                        metavar='',
                        type=str)

    parser.add_argument('-e', '--env',
                        help='CMR environment used to pull results from.',
                        required=True,
                        choices=["uat", "ops"],
                        metavar='uat or ops')

    parser.add_argument('-n', '--notebook',
                        help='Notebook to run',
                        required=True,
                        metavar='')

    parser.add_argument('-i', '--input_file',
                        help='File of json collections',
                        required=False,
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

    collection_json = _args.collections
    environment = _args.env
    notebook = _args.notebook
    input_file = _args.input_file

    if collection_json:
        collections = json.loads(collection_json)
    if input_file:
        with open(_args.input_file) as json_data:
            try:
                collections = json.load(json_data)
            except ValueError:
                collections = []
                json_data.seek(0)
                lines = json_data.readlines()
                for line in lines:
                    collections.append(line.strip())


    notebook = "./notebooks/harmony_concise_api_test.ipynb"
    notebook_path = os.path.dirname(notebook)
    notebook_name = os.path.basename(notebook)

    success = []
    fails = []

    venue = "prod"
    if environment == "uat":
        venue = "uat"

    for collection in collections:

        try:
            print(collection)
            pm.execute_notebook(
               notebook,
               "{}/output/{}_{}_output_{}".format(notebook_path, collection, environment, notebook_name),
               parameters=dict(collection=collection, venue=venue)
            )
            success.append(collection)
        except Exception as ex:
            print(ex)
            fails.append(collection)

    if _args.output_path:
        success_outfile = f'{_args.output_path}/{_args.env}_success.txt'
        fail_outfile = f'{_args.output_path}/{_args.env}_fail.txt'

        if success:
            with open(success_outfile, 'w') as the_file:
                the_file.writelines(x + '\n' for x in success)

        if fails:
            with open(fail_outfile, 'w') as the_file:
                the_file.writelines(x + '\n' for x in fails)

if __name__ == '__main__':
    run()
