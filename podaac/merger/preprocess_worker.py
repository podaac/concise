"""Preprocessing methods and the utilities to automagically run them in single-thread/multiprocess modes"""

import json
import os
import queue
from copy import deepcopy
from datetime import datetime, timezone
from multiprocessing import Manager, Process

import importlib_metadata
import netCDF4 as nc
import numpy as np

from podaac.merger.path_utils import get_group_path
from podaac.merger.variable_info import VariableInfo


def run_preprocess(file_list, process_count):
    """
    Automagically run preprocessing in an optimized mode determined by the environment

    Parameters
    ----------
    file_list : list
        List of file paths to be processed
    process_count : int
        Number of worker processes to run (expected >= 1)
    """

    if process_count == 1:
        return _run_single_core(file_list)

    return _run_multi_core(file_list, process_count)


def merge_max_dims(merged_max_dims, subset_max_dims):
    """
    Perform aggregation of max_dims. Intended for use in multithreaded
    mode only

    Parameters
    ----------
    merged_max_dims : dict
        Dictionary of the aggregated max_dims
    subset_max_dims : dict
        Dictionary of max_dims from one of the worker processes
    """

    for dim_name, subset_dim_size in subset_max_dims.items():
        if dim_name not in merged_max_dims or subset_dim_size > merged_max_dims[dim_name]:
            merged_max_dims[dim_name] = subset_dim_size


def merge_metadata(merged_metadata, subset_metadata):
    """
    Perform aggregation of metadata. Intended for use in multithreaded
    mode only

    Parameters
    ----------
    merged_metadata : dict
        Dictionary of the aggregated metadata
    subset_max_dims : dict
        Dictionary of metadata from one of the worker processes
    """

    for var_path, subset_attrs in subset_metadata.items():
        if var_path not in merged_metadata:
            merged_metadata[var_path] = {}

        merged_attrs = merged_metadata[var_path]
        for attr_name, subset_attr in subset_attrs.items():
            if attr_name not in merged_attrs:
                merged_attrs[attr_name] = subset_attr
            elif not attr_eq(merged_attrs[attr_name], subset_attr):
                merged_attrs[attr_name] = False  # mark as inconsistent


def construct_history(input_files):
    """
    Construct history JSON entry for this concatenation operation
    https://wiki.earthdata.nasa.gov/display/TRT/In-File+Provenance+Metadata+-+TRT-42

    Parameters
    ----------
    input_files : list
        List of input files

    Returns
    -------
    dict
        History JSON constructed for this concat operation
    """
    base_names = list(map(os.path.basename, input_files))
    history_json = {
        "date_time": datetime.now(tz=timezone.utc).isoformat(),
        "derived_from": base_names,
        "program": 'concise',
        "version": importlib_metadata.distribution('podaac-concise').version,
        "parameters": f'input_files={input_files}',
        "program_ref": "https://cmr.earthdata.nasa.gov:443/search/concepts/S2153799015-POCLOUD",
        "$schema": "https://harmony.earthdata.nasa.gov/schemas/history/0.1.0/history-v0.1.0.json"
    }
    return history_json


def retrieve_history(dataset):
    """
    Retrieve history_json field from NetCDF dataset, if it exists

    Parameters
    ----------
    dataset : netCDF4.Dataset
        NetCDF Dataset representing a single granule

    Returns
    -------
    dict
        history_json field
    """
    if 'history_json' not in dataset.ncattrs():
        return []
    history_json = dataset.getncattr('history_json')
    return json.loads(history_json)


def _run_single_core(file_list):
    """
    Run the granule preprocessing in the current thread/single-core mode

    Parameters
    ----------
    file_list : list
        List of file paths to be processed

    Returns
    -------
    dict
        A dictionary containing the output from the preprocessing process
    """
    group_list = []
    var_info = {}
    max_dims = {}
    var_metadata = {}
    group_metadata = {}
    history_json = []

    for file in file_list:
        with nc.Dataset(file, 'r') as dataset:
            dataset.set_auto_maskandscale(False)
            process_groups(dataset, group_list, max_dims, group_metadata, var_metadata, var_info)
            history_json.extend(retrieve_history(dataset))

    group_list.sort()  # Ensure insertion order doesn't matter between granules

    history_json.append(construct_history(file_list))
    group_metadata[group_list[0]]['history_json'] = json.dumps(
        history_json,
        default=str
    )

    return {
        'group_list': group_list,
        'max_dims': max_dims,
        'var_info': var_info,
        'var_metadata': var_metadata,
        'group_metadata': group_metadata
    }


def _run_multi_core(file_list, process_count):
    """
    Run the granule preprocessing in multi-core mode. This method spins up
    the number of processes defined by process_count which process granules
    in the input queue until empty. When all processes are done, the method
    merges all the preprocessing results together and returns the final
    results

    Parameters
    ----------
    file_list : list
        List of file paths to be processed
    process_count : int
        Number of worker processes to run (expected >= 2)

    Returns
    -------
    dict
        A dictionary containing the output from the preprocessing process
    """
    with Manager() as manager:
        in_queue = manager.Queue(len(file_list))
        results = manager.list()

        for file in file_list:
            in_queue.put(file)

        processes = []
        for _ in range(process_count):
            process = Process(target=_run_worker, args=(in_queue, results))
            processes.append(process)
            process.start()

        # Explicitly check for all processes to successfully exit
        # before attempting to merge results
        for process in processes:
            process.join()

            if process.exitcode != 0:
                raise RuntimeError(f'Preprocessing failed - exit code: {process.exitcode}')

        results = deepcopy(results)  # ensure GC can cleanup multiprocessing

    # -- Merge final results --
    group_list = None
    var_info = None
    max_dims = {}
    var_metadata = {}
    group_metadata = {}
    history_json = []

    for result in results:
        # The following data should be consistent between granules and
        # require no special treatment to merge. Sanity checks added
        # just for verification.
        if group_list is None:
            group_list = result['group_list']
        elif group_list != result['group_list']:
            raise RuntimeError('Groups are inconsistent between granules')

        if var_info is None:
            var_info = result['var_info']
        elif var_info != result['var_info']:
            if set(var_info.keys()).difference(result['var_info']):
                # If not all variables match, only compare variables that intersect
                intersecting_vars = set(var_info).intersection(result['var_info'])
                if list(
                        map(var_info.get, intersecting_vars)
                ) != list(map(result['var_info'].get, intersecting_vars)):
                    raise RuntimeError('Variable schemas are inconsistent between granules')
                var_info.update(result['var_info'])

        # The following data requires accumulation methods
        merge_max_dims(max_dims, result['max_dims'])
        merge_metadata(var_metadata, result['var_metadata'])
        merge_metadata(group_metadata, result['group_metadata'])

        # Merge history_json entries from input files
        history_json.extend(result['history_json'])

    history_json.append(construct_history(file_list))
    group_metadata[group_list[0]]['history_json'] = json.dumps(
        history_json,
        default=str
    )

    return {
        'group_list': group_list,
        'max_dims': max_dims,
        'var_info': var_info,
        'var_metadata': var_metadata,
        'group_metadata': group_metadata
    }


def _run_worker(in_queue, results):
    """
    A method to be executed in a separate process which runs preprocessing on granules
    from the input queue and stores the results internally. When the queue is empty
    (processing is complete), the local results are transfered to the external results
    array to be merged by the main process. If the process never processed any granules
    which is possible if the input queue is underfilled, the process just exits without
    appending to the array

    Parameters
    ----------
    in_queue : Queue
        Input queue of tuples of subset indexes and granule file paths respectively
    results : list
        An array which stores the results of preprocessing from all workers
    """
    empty = True
    group_list = []
    max_dims = {}
    var_info = {}
    var_metadata = {}
    group_metadata = {}
    history_json = []

    while not in_queue.empty():
        try:
            file = in_queue.get_nowait()
        except queue.Empty:
            break

        empty = False
        with nc.Dataset(file, 'r') as dataset:
            dataset.set_auto_maskandscale(False)
            process_groups(dataset, group_list, max_dims, group_metadata, var_metadata, var_info)
            history_json.extend(retrieve_history(dataset))

    group_list.sort()  # Ensure insertion order doesn't matter between granules

    if not empty:
        results.append({
            'group_list': group_list,
            'max_dims': max_dims,
            'var_info': var_info,
            'var_metadata': var_metadata,
            'group_metadata': group_metadata,
            'history_json': history_json
        })


def process_groups(parent_group, group_list, max_dims, group_metadata, var_metadata, var_info):
    """
    Perform preprocessing of a group and recursively process each child group

    Parameters
    ----------
    parent_group: nc.Dataset, nc.Group
        current group to be processed
    group_list: list
        list of group paths
    max_dims: dict
        dictionary which stores dimension paths and associated dimension sizes
    group_metadata: dict
        dictionary which stores group paths and their associated attributes
    var_metadata: dict
        dictionary of dictionaries which stores variable paths and their associated attributes
    var_info: dict
        dictionary of variable paths and associated VariableInfo data
    """

    if parent_group.path not in group_metadata:
        group_metadata[parent_group.path] = {}

    if parent_group.path not in group_list:
        group_list.append(parent_group.path)

    get_max_dims(parent_group, max_dims)
    get_metadata(parent_group, group_metadata[parent_group.path])
    get_variable_data(parent_group, var_info, var_metadata)

    for child_group in parent_group.groups.values():
        process_groups(child_group, group_list, max_dims, group_metadata, var_metadata, var_info)


def get_max_dims(group, max_dims):
    """
    Aggregates dimensions from each group and creates a dictionary
    of the largest dimension sizes for each group

    Parameters
    ----------
    group: nc.Dataset, nc.Group
        group to process dimensions from
    max_dims: dict
        dictionary which stores dimension paths and associated dimension sizes
    """

    for dim in group.dimensions.values():
        dim_path = get_group_path(group, dim.name)

        if dim_path not in max_dims or max_dims[dim_path] < dim.size:
            max_dims[dim_path] = dim.size


def get_metadata(group, metadata):
    """
    Aggregates metadata from various NetCDF4 objects into a dictionary

    Parameters
    ----------
    group : nc.Dataset, nc.Group, nc.Variable
        the NetCDF4 object to aggregate metadata from
    metadata : dict
        a dictionary containing the object name and associated metadata
    """

    for attr_name in group.ncattrs():
        attr = group.getncattr(attr_name)

        if attr_name not in metadata:
            metadata[attr_name] = attr
        elif not attr_eq(metadata[attr_name], attr):
            metadata[attr_name] = False  # mark as inconsistent


def attr_eq(attr_1, attr_2):
    """
    Helper function to check if one attribute value is equal to another
    (no, a simple == was not working)

    Parameters
    ----------
    attr_1 : obj
        An attribute value
    attr_2 : obj
        An attribute value
    """

    if isinstance(attr_1, np.ndarray) or isinstance(attr_2, np.ndarray):
        if not np.array_equal(attr_1, attr_2):
            return False
    elif type(attr_1) != type(attr_2) or attr_1 != attr_2:  # pylint: disable=unidiomatic-typecheck
        return False

    return True


def get_variable_data(group, var_info, var_metadata):
    """
    Aggregate variable metadata and attributes. Primarily utilized in process_groups

    Parameters
    ----------
    group : nc.Dataset, nc.Group
        group associated with this variable
    var_info : dict
        dictionary of variable paths and associated VariableInfo
    var_metadata : dict
        dictionary of variable paths and associated attribute dictionary
    """

    for var in group.variables.values():

        # Generate VariableInfo map
        info = VariableInfo(var)
        var_path = get_group_path(group, var.name)

        if var_path not in var_info:
            var_info[var_path] = info
        elif var_info[var_path] != info:
            # Check to ensure datasets are consistent
            raise RuntimeError('Inconsistent variable schemas')

        # Generate variable attribute map
        if var_path not in var_metadata:
            var_metadata[var_path] = {}

        get_metadata(var, var_metadata[var_path])
