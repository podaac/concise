"""
Utilities used throughout the merging implementation to simplify group path resolution
and generation
"""


def get_group_path(group, resource):
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


def resolve_group(dataset, path):
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

    return (group, components[1])


def resolve_dim(dims, group_path, dim_name):
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
