from unittest import TestCase
from unittest.mock import Mock
from netCDF4 import Dataset, Group

import podaac.merger.path_utils as path_utils


class PathUtilsTest(TestCase):
    def test_group_path(self):
        mock_1 = Mock(
            spec=Group,
            path='/'
        )

        mock_2 = Mock(
            spec=Group,
            path='/group'
        )

        path_1 = path_utils.get_group_path(mock_1, 'resource1')
        path_2 = path_utils.get_group_path(mock_2, 'resource2')

        self.assertEqual(path_1, '/resource1')
        self.assertEqual(path_2, '/group/resource2')

    def test_group_resolution(self):
        with Dataset('test.nc', mode='w', diskless=True) as dataset:
            group_0 = dataset.createGroup('/group_0')
            group_1 = dataset.createGroup('/group_0/group_1')

            result = path_utils.resolve_group(dataset, '/resource_0')
            self.assertEqual(result[0], dataset)
            self.assertEqual(result[1], 'resource_0')

            result = path_utils.resolve_group(dataset, '/group_0/resource_1')
            self.assertEqual(result[0], group_0)
            self.assertEqual(result[1], 'resource_1')

            result = path_utils.resolve_group(dataset, '/group_0/group_1/resource_2')
            self.assertEqual(result[0], group_1)
            self.assertEqual(result[1], 'resource_2')

    def test_dim_resolution(self):
        dims = {
            '/dim_0': 10,
            '/dim_1': 11,
            '/group/dim_0': 12,
            '/group/dim_2': 13,
            '/group/subgroup/dim_0': 14,
        }

        # test dim scoping
        self.assertEqual(path_utils.resolve_dim(dims, '/group/subgroup', 'dim_0'), 14)
        self.assertEqual(path_utils.resolve_dim(dims, '/group', 'dim_0'), 12)
        self.assertEqual(path_utils.resolve_dim(dims, '/', 'dim_0'), 10)

        # test heiarchy resolution
        self.assertEqual(path_utils.resolve_dim(dims, '/group/subgroup', 'dim_2'), 13)  # should resolve to group
        self.assertEqual(path_utils.resolve_dim(dims, '/group/subgroup', 'dim_1'), 11)  # should resolve to root
