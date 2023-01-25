import argparse
import cmr

from podaac.umms_updater.util import create_assoc
from podaac.umms_updater.util import svc_update

def parse_args():
    """
    Parses the program arguments
    Returns
    -------
    args
    """

    parser = argparse.ArgumentParser(
        description='Remove collection association from CMR',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('-e', '--env',
                        help='CMR environment used to pull results from.',
                        required=False,
                        choices=["uat", "ops", "sit"],
                        metavar='uat or ops')

    parser.add_argument('-i', '--input_file',
                        help='File of json collections to remove',
                        required=False,
                        metavar='')

    parser.add_argument('-t', '--token',
                        help='Launchpad token string',
                        default=None,
                        required=True,
                        metavar='Launchpad token')

    parser.add_argument('-p', '--provider',
                        help='A provider ID identifies a provider and is'
                             'composed of a combination of upper case'
                             ' letters, digits, and underscores.'
                             'The maximum length is 10 characters.'
                             'Concept ID is provided if UMM-S record already'
                             'exists and needs to be updated.',
                        required=False,
                        default='POCLOUD',
                        metavar='POCLOUD')

    parser.add_argument('-n', '--umm_name',
                        help='Name of the umm tool or service',
                        required=True,
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

    collection_json = _args.input_file
    environment = _args.env
    service_name = _args.umm_name

    header = {
        'Content-type': "application/json",
        'Authorization': str(_args.token),
    }
    url_prefix = svc_update.cmr_environment_url(_args.env)

    mode = cmr.queries.CMR_OPS
    if environment == 'uat':
        mode = cmr.queries.CMR_UAT

    service_query = cmr.queries.ServiceQuery(mode=mode).provider(_args.provider).name(service_name).get()
    service_concept_id = service_query[0].get('concept_id')

    with open(collection_json) as afile:
        assoc_concept_ids = afile.readlines()
    for i, assoc_concept_id in enumerate(assoc_concept_ids):
        collection_concept_id = assoc_concept_id.strip('\n')
        print(f"Removing {collection_concept_id} from CMR association")
        resp = create_assoc.remove_association(url_prefix, service_concept_id, collection_concept_id, header)
        print(f'Response: {resp}')

if __name__ == '__main__':
    run()