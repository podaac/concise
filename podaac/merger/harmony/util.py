"""Misc utility functions"""


def is_netcdf_asset(asset, strict):
    """
    Determine if an asset is netcdf4-python compatible. netcdf4-python currently supports
    HDF5, NetCDF3, and NetCDF4. Determination is currently done through MIME if strict
    mode is enabled. If strict mode is not enabled, determination is done through 'data'
    role.

    Parameters
    ----------
    asset : pystac.Asset
        an asset to check

    Returns
    -------
    bool
        True if netcdf4-python compatible; False otherwise
    """

    if strict:
        accepted_types = ['application/x-hdf5', 'application/x-netcdf', 'application/x-netcdf4']
        return asset.media_type in accepted_types

    return 'data' in asset.roles


def get_granule_url(item, granule_urls, strict=True):
    """
    Processes an item to find a netcdf4-python compatible asset. If no asset is
    found, a RuntimeException is thrown

    Parameters
    ----------
    item : pystac.Item
        an item to process
    granule_urls : list
        list to append the asset's url to
    """

    for asset in item.assets.values():
        if is_netcdf_asset(asset, strict):
            granule_urls.append(asset.href)
            return

    if not strict:
        raise RuntimeError(f'A NetCDF4 asset was not found in this item: {item.id}')

    get_granule_url(item, granule_urls, False)  # Rerun in lax-mode


def get_bbox(item, current_bbox):
    """
    Accumulate bboxes from items to generate a bbox which encompasses all items

    Parameters
    ----------
    item : pystac.Item
        an item to process
    current_bbox : list
        the bbox to accumulate all items to
    """

    if len(current_bbox) == 0:
        if item.bbox is not None:  # Spec allows for null geometry and bbox
            current_bbox[:] = item.bbox
    else:
        # xmin
        if item.bbox[0] < current_bbox[0]:
            current_bbox[0] = item.bbox[0]
        # ymin
        if item.bbox[1] < current_bbox[1]:
            current_bbox[1] = item.bbox[1]
        # xmax
        if item.bbox[2] > current_bbox[2]:
            current_bbox[2] = item.bbox[2]
        # ymax
        if item.bbox[3] > current_bbox[3]:
            current_bbox[3] = item.bbox[3]


def get_datetime(item, datetimes):
    """
    Accumulate datetimes from items to generate a datetime pair that
    encompasses all items

    Parameters
    ----------
    item : pystac.Item
        an item to process
    datetimes : list
        datetime pair to accumulate to; first element is start_datetime,
        second is end_datetime
    """

    if item.datetime is None:
        item_start_dt = item.common_metadata.start_datetime
        item_end_dt = item.common_metadata.end_datetime
    else:
        item_start_dt = item.datetime
        item_end_dt = item.datetime

    if item_start_dt < datetimes[0]:
        datetimes[0] = item_start_dt

    if item_end_dt > datetimes[1]:
        datetimes[1] = item_end_dt
