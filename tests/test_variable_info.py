from unittest import TestCase
from unittest.mock import Mock
from netCDF4 import Variable
import numpy as np

from podaac.merger.variable_info import VariableInfo


class TestVariableInfo(TestCase):
    def test_immutable(self):
        info = VariableInfo(Mock(spec=Variable))

        attrs = ['name', 'dimensions', 'datatype', 'group_path', 'fill_value']
        for attr in attrs:
            self.assertRaises(AttributeError, setattr, info, attr, None)

    def test_fill_value(self):
        info = VariableInfo(Mock(
            spec=Variable
        ))

        self.assertEqual(info.fill_value, None)

        info = VariableInfo(Mock(
            spec=Variable,
            _FillValue=123
        ))

        self.assertEqual(info.fill_value, 123)

        info = VariableInfo(Mock(
            spec=Variable,
            missing_value=456
        ))

        self.assertEqual(info.fill_value, 456)

    def test_eq(self):
        name = 'Test Var'
        dimensions = ['A', 'B', 'C']
        datatype = np.single
        group_path = '/Group'

        mock_1 = Mock(spec=Variable)
        mock_2 = Mock(spec=Variable)
        mock_3 = Mock(spec=Variable)  # left intentionally unconfigured

        mock_1.configure_mock(
            name=name,
            dimensions=dimensions[:],
            datatype=datatype,
            group=Mock(return_value=Mock(
                path=group_path))
        )

        mock_2.configure_mock(
            name=name,
            dimensions=dimensions[:],
            datatype=datatype,
            group=Mock(return_value=Mock(
                path=group_path))
        )

        self.assertEqual(VariableInfo(mock_1), VariableInfo(mock_2))
        self.assertNotEqual(VariableInfo(mock_1), VariableInfo(mock_3))

    def test_eq_nan(self):
        mock_conf = {
            'name': 'Test Var',
            'dimensions': ['A', 'B', 'C'],
            'datatype': np.single,
            'group': Mock(return_value=Mock(path='/Group')),
            '_FillValue': np.nan
        }

        mock_1 = Mock(spec=Variable)
        mock_2 = Mock(spec=Variable)

        mock_1.configure_mock(**mock_conf)
        mock_2.configure_mock(**mock_conf)

        self.assertEqual(VariableInfo(mock_1), VariableInfo(mock_2))
