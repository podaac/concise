import json
from shutil import rmtree
import sys
from tempfile import mkdtemp
from unittest import TestCase
from unittest.mock import patch
from os import environ
from pathlib import Path
from urllib.parse import urlsplit

from netCDF4 import Dataset
import pytest

import podaac.merger.harmony.cli


@pytest.mark.usefixtures("pass_options")
class TestMerge(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.__test_path = Path(__file__).parent.resolve()
        cls.__data_path = cls.__test_path.joinpath('data')
        cls.__harmony_path = cls.__data_path.joinpath('harmony')
        cls.__output_path = Path(mkdtemp(prefix='tmp-', dir=cls.__data_path))

    @classmethod
    def tearDownClass(cls):
        if not cls.KEEP_TMP:  # pylint: disable=no-member
            rmtree(cls.__output_path)

    def test_service_invoke(self):
        in_message_path = self.__harmony_path.joinpath('message.json')
        in_message_data = in_message_path.read_text()
        in_message = json.loads(in_message_data)

        # test with both paged catalogs and un-paged catalogs
        for in_catalog_name in ['catalog.json', 'catalog0.json']:

            in_catalog_path = self.__harmony_path.joinpath('source', in_catalog_name)

            test_args = [
                podaac.merger.harmony.cli.__file__,
                '--harmony-action', 'invoke',
                '--harmony-input', in_message_data,
                '--harmony-source', str(in_catalog_path),
                '--harmony-metadata-dir', str(self.__output_path),
                '--harmony-data-location', self.__output_path.as_uri()
            ]

            test_env = {
                'ENV': 'dev',
                'OAUTH_CLIENT_ID': '',
                'OAUTH_UID': '',
                'OAUTH_PASSWORD': '',
                'OAUTH_REDIRECT_URI': '',
                'STAGING_PATH': '',
                'STAGING_BUCKET': ''
            }

            with patch.object(sys, 'argv', test_args), patch.dict(environ, test_env):
                podaac.merger.harmony.cli.main()

            out_catalog_path = self.__output_path.joinpath('catalog.json')
            out_catalog = json.loads(out_catalog_path.read_text())

            item_meta = next(item for item in out_catalog['links'] if item['rel'] == 'item')
            item_href = item_meta['href']
            item_path = self.__output_path.joinpath(item_href).resolve()

            # -- Item Verification --
            item = json.loads(item_path.read_text())
            properties = item['properties']

            # Accumulation method checks
            self.assertEqual(item['bbox'], [-4, -3, 4, 3])
            self.assertEqual(properties['start_datetime'], '2020-01-01T00:00:00+00:00')
            self.assertEqual(properties['end_datetime'], '2020-01-05T23:59:59+00:00')

            # -- Asset Verification --
            data = item['assets']['data']
            collection_name = in_message['sources'][0]['collection']

            # Sanity checks on metadata
            print(f"item_path === f{item_path}")
            print(f"properties['end_datetime'] === f{properties['end_datetime']}")
            print(f"href === f{data['href']}")
            print(f"title === f{data['href']}")
            self.assertTrue(data['href'].endswith(f"{properties['end_datetime']}_{collection_name}_merged.nc4"))
            self.assertEqual(data['title'], f"{properties['end_datetime']}_{collection_name}_merged.nc4")
            self.assertEqual(data['type'], 'application/x-netcdf4')
            self.assertEqual(data['roles'], ['data'])

            # -- subset_files Verification --
            file_list = [
                '2020_01_01_7f00ff_global.nc',
                '2020_01_02_3200ff_global.nc',
                '2020_01_03_0019ff_global.nc',
                '2020_01_04_0065ff_global.nc',
                '2020_01_05_00b2ff_global.nc'
            ]

            path = urlsplit(data['href']).path
            dataset = Dataset(path)
            subset_files = dataset['subset_files'][:].tolist()
            subset_files.sort()

            self.assertEqual(file_list, subset_files)
