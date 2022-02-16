from tempfile import mkdtemp
from unittest import TestCase
from pathlib import Path
from shutil import rmtree

import netCDF4 as nc
import numpy as np
import pytest
import json
import os
import importlib_metadata

from podaac.merger import merge


def is_file_empty(parent_group):
    """
    Function to test if a all variable size in a dataset is 0
    """

    for var in parent_group.variables.values():
        if var.size != 0:
            return False
    for child_group in parent_group.groups.values():
        return is_file_empty(child_group)      
    return True

@pytest.mark.usefixtures("pass_options")
class TestMerge(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.__test_path = Path(__file__).parent.resolve()
        cls.__test_data_path = cls.__test_path.joinpath('data')
        cls.__output_path = Path(mkdtemp(prefix='tmp-', dir=cls.__test_data_path))

    @classmethod
    def tearDownClass(cls):
        if not cls.KEEP_TMP:  # pylint: disable=no-member
            rmtree(cls.__output_path)

    def verify_dims(self, merged_group, origin_group, both_merged):
        for dim in origin_group.dimensions:
            if both_merged:
                self.assertEqual(merged_group.dimensions[dim].size, origin_group.dimensions[dim].size)
            else:
                self.assertGreaterEqual(merged_group.dimensions[dim].size, origin_group.dimensions[dim].size)

    def verify_attrs(self, merged_obj, origin_obj, both_merged):
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
                self.assertTrue(np.array_equal(merged_attr, origin_attr))
            else:
                if attr != "history_json":
                    self.assertEqual(merged_attr, origin_attr)

    def verify_variables(self, merged_group, origin_group, subset_index, both_merged):
        for var in origin_group.variables:
            merged_var = merged_group.variables[var]
            origin_var = origin_group.variables[var]

            self.verify_attrs(merged_var, origin_var, both_merged)

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
                self.assertEqual(merged_data, origin_data)
            else:
                self.assertTrue(np.array_equal(merged_data, origin_data, equal_nan=True))

    def verify_files(self, dataset, input_files, data_dir):
        data_path = self.__test_data_path.joinpath(data_dir)

        subset_files = list(dataset.variables['subset_files'])
        output_files = []
        for file in input_files:
            with nc.Dataset(data_path.joinpath(file)) as dataset:
                res = is_file_empty(dataset)
                if res is False:
                    self.assertIn(file.name, subset_files)
                    output_files.append(file)
        return output_files
        # return subset_files

    def verify_groups(self, merged_group, origin_group, subset_index, both_merged=False):
        self.verify_dims(merged_group, origin_group, both_merged)
        self.verify_attrs(merged_group, origin_group, both_merged)
        self.verify_variables(merged_group, origin_group, subset_index, both_merged)

        for child_group in origin_group.groups:
            merged_subgroup = merged_group[child_group]
            origin_subgroup = origin_group[child_group]
            self.verify_groups(merged_subgroup, origin_subgroup, subset_index, both_merged)

    def run_verification(self, data_dir, output_name, process_count=None):
        output_path = self.__output_path.joinpath(output_name)
        data_path = self.__test_data_path.joinpath(data_dir)
        input_files = list(data_path.iterdir())

        merge.merge_netcdf_files(input_files, output_path, process_count=process_count)

        merged_dataset = nc.Dataset(output_path)
        file_map = self.verify_files(merged_dataset, input_files, data_dir)

        for i, file in enumerate(file_map):
            origin_dataset = nc.Dataset(data_path.joinpath(file))
            self.verify_groups(merged_dataset, origin_dataset, i)

    def run_java_verification(self, output_name, process_count=None):
        data_path = self.__test_data_path.joinpath('no_groups')
        input_files = list(data_path.iterdir())

        java_path = self.__test_data_path.joinpath('java_results', 'merged-ASCATA-L2-25km-Lat-90.0_90.0-Lon-180.0_180.0.subset.nc')
        python_path = self.__output_path.joinpath(output_name)

        merge.merge_netcdf_files(input_files, python_path, process_count=process_count)

        java_dataset = nc.Dataset(java_path)
        python_dataset = nc.Dataset(python_path)

        file_map = self.build_file_mapping(python_dataset, java_dataset)

        for subset_index in file_map:
            self.verify_groups(python_dataset, java_dataset, subset_index, True)

    def build_file_mapping(self, merged_dataset, origin_dataset):  # pylint: disable=no-self-use
        merged_files = list(merged_dataset.variables['subset_files'])
        origin_files = list(origin_dataset.variables['subset_files'])
        file_map = list()

        for merged_index, file in enumerate(merged_files):
            origin_index = origin_files.index(file)
            file_map.append((merged_index, origin_index))

        return file_map

    def test_simple_merge_single(self):
        self.run_verification('no_groups', 'simple_merge_single.nc', 1)

    def test_group_merge_single(self):
        self.run_verification('groups', 'group_merge_single.nc', 1)

    def test_subgroup_merge_single(self):
        self.run_verification('subgroups', 'subgroup_merge_single.nc', 1)

    def test_simple_merge_multi(self):
        self.run_verification('no_groups', 'simple_merge_multi.nc')

    def test_group_merge_multi(self):
        self.run_verification('groups', 'group_merge_multi.nc')

    def test_subgroup_merge_multi(self):
        self.run_verification('subgroups', 'subgroup_merge_multi.nc')

    def test_empty_data_merge_multi(self):
        self.run_verification('empty_data', 'empty_data_merge_multi.nc')

    def test_compare_java_single(self):
        self.run_java_verification('python_merge_single.nc', 1)

    def test_compare_java_multi(self):
        self.run_java_verification('python_merge_multi.nc')

    def test_history(self):
        data_dir = 'no_groups'
        output_name_single = 'test_history_single.nc'
        output_name_multi = 'test_history_multi.nc'
        data_path = self.__test_data_path.joinpath(data_dir)
        input_files = list(data_path.iterdir())

        def assert_valid_history(merged_dataset, input_files):
            input_files = [os.path.basename(file_name) for file_name in input_files]
            history_json = json.loads(merged_dataset.getncattr('history_json'))[-1]
            assert 'date_time' in history_json
            assert history_json.get('program') == 'concise'
            assert history_json.get('derived_from') == input_files
            assert history_json.get('version') == importlib_metadata.distribution('podaac-concise').version
            assert 'input_files=' in history_json.get('parameters')
            assert history_json.get('program_ref') == 'https://cmr.earthdata.nasa.gov:443/search/concepts/S2153799015-POCLOUD'
            assert history_json.get('$schema') == 'https://harmony.earthdata.nasa.gov/schemas/history/0.1.0/history-v0.1.0.json'

        # Single core mode
        merge.merge_netcdf_files(
            original_input_files=input_files,
            output_file=self.__output_path.joinpath(output_name_single),
            process_count=1
        )
        merged_dataset = nc.Dataset(self.__output_path.joinpath(output_name_single))
        assert_valid_history(merged_dataset, input_files)

        merged_dataset.close()

        # Multi core mode
        merge.merge_netcdf_files(
            original_input_files=input_files,
            output_file=self.__output_path.joinpath(output_name_multi),
            process_count=2
        )
        merged_dataset = nc.Dataset(self.__output_path.joinpath(output_name_multi))
        assert_valid_history(merged_dataset, input_files)

        merged_dataset.close()

        # Run again, but use l2ss-py output which contains existing
        # history_json. Concise history should contain new entry plus
        # all entries from input files
        data_path = self.__test_data_path.joinpath('l2ss_py_output')
        input_files = list(data_path.iterdir())
        merge.merge_netcdf_files(
            original_input_files=input_files,
            output_file=self.__output_path.joinpath(output_name_single),
            process_count=1
        )
        merged_dataset = nc.Dataset(self.__output_path.joinpath(output_name_single))
        assert_valid_history(merged_dataset, input_files)
        history_json = json.loads(merged_dataset.getncattr('history_json'))
        assert len(history_json) == 3
        assert history_json[0]['program'] == 'l2ss-py'
        assert history_json[1]['program'] == 'l2ss-py'
        assert history_json[2]['program'] == 'concise'
