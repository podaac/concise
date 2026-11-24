"""Padding of shorter granules must not duplicate real data or use fake zeros."""
from pathlib import Path

import netCDF4 as nc
import numpy as np
import pytest

from podaac.merger import merge


def _make(path, n, with_fill):
    with nc.Dataset(path, 'w') as ds:
        ds.createDimension('mon', n)
        ids = ds.createVariable('ident', str, ('mon',))
        for k in range(n):
            ids[k] = f'id-{path.stem}-{k}'
        kwargs = {'fill_value': -999.0} if with_fill else {}
        val = ds.createVariable('val', 'f8', ('mon',), **kwargs)
        val[:] = np.arange(n) + 1.0
        cnt = ds.createVariable('cnt', 'i8', ('mon',))
        cnt[:] = np.arange(n) + 1


@pytest.mark.parametrize('process_count', [1, 2])
@pytest.mark.parametrize('with_fill', [True, False])
def test_padding_is_not_real_data(tmp_path, process_count, with_fill):
    big, small = tmp_path / 'big.nc', tmp_path / 'small.nc'
    _make(big, 5, with_fill)
    _make(small, 1, with_fill)
    out = tmp_path / 'out.nc'
    merge.merge_netcdf_files([big, small], str(out), ['a', 'b'], process_count=process_count)

    with nc.Dataset(out) as ds:
        ids = ds['ident'][1]
        assert ids[0] == f'id-small-0'
        assert all(i == '' for i in ids[1:]), ids  # not repeated ids

        val = ds['val'][1]  # auto-masked
        assert val[0] == 1.0
        assert np.ma.getmaskarray(val)[1:].all(), val  # padding is fill, not 0

        cnt = ds['cnt'][1]
        assert np.ma.getmaskarray(cnt)[1:].all(), cnt
