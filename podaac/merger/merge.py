"""Main module containing merge implementation"""

from time import perf_counter
from logging import getLogger
from os import cpu_count
import netCDF4 as nc
import numpy as np

from podaac.merger.merge_worker import run_merge
from podaac.merger.path_utils import get_group_path, resolve_dim, resolve_group
from podaac.merger.preprocess_worker import run_preprocess


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


def merge_netcdf_files(original_input_files, output_file, logger=getLogger(__name__), perf_stats=None, process_count=None):  # pylint: disable=too-many-locals
    """
    Main entrypoint to merge implementation. Merges n >= 2 granules together as a single
    granule. Named in reference to original Java implementation.

    Parameters
    ----------
    input_files: list
        list of string paths to NetCDF4 files to merge
    output_file: str
        output path for merged product
    logger: logger
        logger object
    perf_stats: dict
        dictionary used to store performance stats
    process_count: int
        number of processes to run (expected >= 1)
    """

    if perf_stats is None:
        perf_stats = {}

    if process_count is None:
        process_count = cpu_count()
    elif process_count <= 0:
        raise RuntimeError('process_count should be > 0')

    # -- initial preprocessing --
    logger.info('Preprocessing data...')
    start = perf_counter()

    input_files = []

    # only concatinate files that are not empty
    for file in original_input_files:
        with nc.Dataset(file, 'r') as dataset:
            is_empty = is_file_empty(dataset)
            if is_empty is False:
                input_files.append(file)

    preprocess = run_preprocess(input_files, process_count)
    group_list = preprocess['group_list']
    max_dims = preprocess['max_dims']
    var_info = preprocess['var_info']
    var_metadata = preprocess['var_metadata']
    group_metadata = preprocess['group_metadata']

    perf_stats['preprocess'] = perf_counter() - start
    logger.info('Preprocessing completed: %f', perf_stats['preprocess'])

    merged_dataset = nc.Dataset(output_file, 'w', format='NETCDF4')
    merged_dataset.set_auto_maskandscale(False)
    init_dataset(merged_dataset, group_list, var_info, max_dims, input_files)

    # -- merge datasets --
    logger.info('Merging datasets...')
    start = perf_counter()
    run_merge(merged_dataset, input_files, var_info, max_dims, process_count, logger)

    perf_stats['merge'] = perf_counter() - start
    logger.info('Merging completed: %f', perf_stats['merge'])

    # -- finalize metadata --
    logger.info('Finalizing metadata...')
    start = perf_counter()

    for group_path in group_list:
        group = merged_dataset if group_path == '/' else merged_dataset[group_path]

        group_attrs = group_metadata[group_path]
        clean_metadata(group_attrs)
        group.setncatts(group_attrs)

        for var in group.variables.values():
            if var.name == 'subset_files' and group == merged_dataset:
                continue  # Skip /subset_files for metadata finalization

            var_path = get_group_path(group, var.name)
            var_attrs = var_metadata[var_path]
            clean_metadata(var_attrs)
            var.setncatts(var_attrs)

    perf_stats['metadata'] = perf_counter() - start
    logger.info('Metadata completed: %f', perf_stats['metadata'])

    merged_dataset.close()
    logger.info('Done!')


def clean_metadata(metadata):
    """
    Prepares metadata dictionary for insertion by removing inconsistent entries
    and performing escaping of attribute names

    Parameters
    ----------
    metadata : dict
        dictionary of attribute names and their associated data
    """

    for key in list(metadata):
        val = metadata[key]

        # delete inconsistent items
        if not isinstance(val, np.ndarray) and isinstance(val, bool) and not val:
            del metadata[key]
        elif key == '_FillValue':
            del metadata[key]

        # escape '/' to '_'
        # https://www.unidata.ucar.edu/mailing_lists/archives/netcdfgroup/2012/msg00098.html
        if '/' in key:
            new_key = key.replace('/', '_')
            metadata[new_key] = val
            del metadata[key]


def init_dataset(merged_dataset, groups, var_info, max_dims, input_files):
    """
    Initialize the dataset utilizing data gathered from preprocessing

    Parameters
    ----------
    merged_dataset : nc.Dataset
        the dataset to be initialized
    groups : list
        list of group names
    var_info : dict
        dictionary of variable names and VariableInfo objects
    max_dims : dict
        dictionary of dimension names (including path) and their sizes
    input_files : list
        list of file paths to be merged
    """

    # Create groups
    for group in groups:
        if group == '/':
            continue  # Skip root

        merged_dataset.createGroup(group)

    # Create dims
    merged_dataset.createDimension('subset_index', len(input_files))
    for dim in max_dims.items():
        group = resolve_group(merged_dataset, dim[0])
        group[0].createDimension(group[1], dim[1])

    # Generate filelist
    subset_files = merged_dataset.createVariable('subset_files', np.str_, ['subset_index'])
    subset_files.long_name = 'List of subsetted files used to create this merge product.'
    for i, file in enumerate(input_files):
        subset_files[i] = file.name

    # Recreate variables
    for var in var_info.items():
        dims = ['subset_index'] + list(var[1].dim_order)
        group = resolve_group(merged_dataset, var[0])

        # Holdover from old merging code - not sure if needed, but kept for legacy
        chunk_sizes = [1] + [resolve_dim(max_dims, group[0].path, key) for key in var[1].dim_order]

        group[0].createVariable(
            varname=var[1].name,
            datatype=var[1].datatype,
            dimensions=dims,
            chunksizes=chunk_sizes,
            fill_value=var[1].fill_value,
            zlib=True
        )
