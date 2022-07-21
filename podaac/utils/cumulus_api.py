"""Module for Cumulus API calls"""

import requests

from podaac.utils.enums import Venue

class CumulusAPI():
    """
    Class for Cumulus API calls
    """

    def get_collections(token:str, venue:Venue, provider:str="POCUMULUS") -> list:
        """
        Function to get the collections from cumulus
        """

        print("\r\nGetting Collections from Cumulus...\r\n")

        if venue == Venue.OPS:
            cumulus_baseurl = "cmr.earthdata.nasa.gov"
        elif venue == Venue.UAT:
            cumulus_baseurl = "cmr.uat.earthdata.nasa.gov"
        elif venue == Venue.SIT:
            cumulus_baseurl = "cmr.uat.earthdata.nasa.gov"

        url = f"{cumulus_baseurl}/search/collections.umm_json?page_size=2000&provider={provider}"

        custom_header = {
            "Cmr-pretty": "true",
            "Authorization": f"{token}" }

        response = requests.get(
            url = url,
            headers = custom_header)

        print(f"Response: {response.status_code}")
        if response.status_code != 200:
            print(f"Response text:\r\n{response.text}\r\n")
            print(f"Request url:\r\n{response.request.url}\r\n")

        return response


    def get_collection_as_list(token:str, venue:Venue, provider:str="POCUMULUS") -> list:
        """
        Function to get the collections from cumulusvand convert it into a list
        """

        collections = []
        response = CumulusAPI.get_collections(token, venue, provider)
        data_dict = response.json()

        # Confirm if access is successful
        assert response.status_code == 200, f"Expected Status code: 200, actual status code: {response.status_code}"
        assert data_dict["hits"] > 0, f'There are no results! "{data_dict["hits"]}" item found in the response!'

        # Extract data from response
        for item in data_dict["items"]:
            collections.append(item["umm"]["ShortName"])

        return collections
