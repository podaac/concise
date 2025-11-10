"""
Utilities used throughout the merging implementation to simplify group path resolution
and generation
"""
import netCDF4 as nc


def get_group_path(group: nc.Group, resource: str) -> str:
    """
    Generates a Unix-like path from a group and resource to be accessed

    Parameters
    ----------
        group: nc.Group
            NetCDF4 group that contains the resource
        resource: str
            name of the resource being accessed

    Returns
    -------
        str
            Unix-like path to the resource
    """

    if group.path == '/':
        return '/' + resource

    return group.path + '/' + resource


def resolve_group(dataset: nc.Dataset, path: str):
    """
    Resolves a group path into two components: the group and the resource's name

    Parameters
    ----------
        dataset: nc.Dataset
            NetCDF4 Dataset used as the root for all groups
        path: str
            the path to the resource

    Returns
    -------
        tuple
            a tuple of the resolved group and the final path component str respectively
    """

    components = path.rsplit('/', 1)
    group = dataset

    if len(components[0]) > 0:
        group = dataset[components[0]]

    return group, components[1]


def resolve_dim(dims: dict, group_path: str, dim_name: str):
    """
    Attempt to resolve dim name starting from top-most group going down to the root group

    Parameters
    ----------
        dims: dict
            Dictionary of dimensions to be traversed
        group_path: str
            the group path from which to start resolving the specific dimension
        dim_name: str
            the name of the dim to be resolved

    Returns
    -------
        int
            the size of the dimension requested
    """
    group_tree = group_path.split('/')

    for i in range(len(group_tree), 0, -1):
        path = '/'.join(group_tree[:i]) + '/' + dim_name

        if path in dims:
            return dims[path]

    # Attempt to find dim in root node
    return dims[dim_name]


def collapse_dims(dims: dict) -> dict:
    """
    Collapse redundant child-dimension paths when a root dimension already exists.

    If a dimension exists at the root level (e.g., "/mirror_step") and a child
    path also defines the same dimension (e.g., "/product/mirror_step"), the
    child dimension is removed because it is redundant and should inherit from the parent.

    Dimensions that appear only in child groups (i.e., have no parent/root version)
    are preserved.

    Parameters
    ----------
    dims : dict
        Dictionary of {path: size}, where paths are HDF5/NetCDF-style dimension paths.

        Example keys:
            "/mirror_step"
            "/product/mirror_step"
            "/support_data/swt_level"

    Returns
    -------
    dict
        A new dictionary with redundant child dimension declarations removed.
    """
    result = {}

    # Collect root dim names like "/mirror_step"
    root_dims = {p for p in dims if p.count("/") == 1}

    for path, size in dims.items():
        dim = "/" + path.split("/")[-1]

        # If dim has a root version and this is NOT that root path → drop it
        if dim in root_dims and path != dim:
            continue
        result[path] = size

    return result
