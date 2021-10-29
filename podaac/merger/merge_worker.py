"""Preprocessing methods and the utilities to automagically run them in single-thread/multiprocess modes"""

import multiprocessing
from multiprocessing.shared_memory import SharedMemory
import queue
import netCDF4 as nc
import numpy as np

from podaac.merger.path_utils import resolve_dim, resolve_group


def run_merge(merged_dataset, file_list, var_info, max_dims, process_count):
    """
    Automagically run merging in an optimized mode determined by the environment

    Parameters
    ----------
    merged_dataset : nc.Dataset
        Destination dataset of the merge operation
    file_list : list
        List of file paths to be processed
    var_info : dict
        Dictionary of variable paths and associated VariableInfo
    max_dims : dict
        Dictionary of dimension paths and maximum dimensions found during preprocessing
    process_count : int
        Number of worker processes to run (expected >= 1)
    """

    if process_count == 1:
        _run_single_core(merged_dataset, file_list, var_info, max_dims)
    else:
        # Merging is bottlenecked at the write process which is single threaded
        # so spinning up more than 2 processes for read/write won't scale the
        # optimization
        _run_multi_core(merged_dataset, file_list, var_info, max_dims, 2)


def _run_single_core(merged_dataset, file_list, var_info, max_dims):
    """
    Run the variable merge in the current thread/single-core mode

    Parameters
    ----------
    merged_dataset : nc.Dataset
        Destination dataset of the merge operation
    file_list : list
        List of file paths to be processed
    var_info : dict
        Dictionary of variable paths and associated VariableInfo
    max_dims : dict
        Dictionary of dimension paths and maximum dimensions found during preprocessing
    """
    for i, file in enumerate(file_list):
        with nc.Dataset(file, 'r') as origin_dataset:
            origin_dataset.set_auto_maskandscale(False)

            for item in var_info.items():
                ds_group = resolve_group(origin_dataset, item[0])
                merged_group = resolve_group(merged_dataset, item[0])

                ds_var = ds_group[0].variables[ds_group[1]]
                merged_var = merged_group[0].variables[ds_group[1]]

                resized = resize_var(ds_var, item[1], max_dims)
                merged_var[i] = resized


def _run_multi_core(merged_dataset, file_list, var_info, max_dims, process_count):  # pylint: disable=too-many-locals
    """
    Run the variable merge in multi-core mode. This method creates (process_count - 1)
    read processes which read data from an origin granule, resize it, then queue it
    for the write process to write to disk. The write process is run in the current
    thread

    # of write processes (1) + # of read processes (process_count - 1) = process_count

    Parameters
    ----------
    merged_dataset : nc.Dataset
        Destination dataset of the merge operation
    file_list : list
        List of file paths to be processed
    var_info : dict
        Dictionary of variable paths and associated VariableInfo
    max_dims : dict
        Dictionary of dimension paths and maximum dimensions found during preprocessing
    process_count : int
        Number of worker processes to run (expected >= 2)
    """
    total_variables = len(file_list) * len(var_info)

    # Ensure SharedMemory doesn't get cleaned up before being processed
    context = multiprocessing.get_context('forkserver')

    with context.Manager() as manager:
        in_queue = manager.Queue(len(file_list))
        out_queue = manager.Queue((process_count - 1) * len(var_info))  # Store (process_count - 1) granules in buffer

        for i, file in enumerate(file_list):
            in_queue.put((i, file))

        processes = []
        for _ in range(process_count - 1):
            process = context.Process(target=_run_worker, args=(in_queue, out_queue, max_dims, var_info))
            processes.append(process)
            process.start()

        processed_variables = 0

        while processed_variables < total_variables:
            try:
                i, var_path, shape, memory_name = out_queue.get_nowait()
            except queue.Empty:
                _check_exit(processes)
                continue

            merged_var = merged_dataset[var_path]
            var_meta = var_info[var_path]
            shared_memory = SharedMemory(name=memory_name, create=False)
            resized_arr = np.ndarray(shape, var_meta.datatype, shared_memory.buf)

            merged_var[i] = resized_arr  # The write operation itself

            shared_memory.unlink()
            shared_memory.close()

            processed_variables = processed_variables + 1

        for process in processes:
            # Ensure that child processes properly exit before manager context
            # gets GCed. Solves EOFError
            process.join()


def _run_worker(in_queue, out_queue, max_dims, var_info):
    """
    A method to be executed in a separate process which reads variables from a
    granule, performs resizing, and queues the processed data up for the writer
    process.

    Parameters
    ----------
    in_queue : Queue
        Input queue of tuples of subset indexes and granule file paths respectively
    out_queue : Queue
        Output queue of tuples of subset indexes, variable path, variable shape, and shared memory name
    max_dims : dict
        Dictionary of dimension paths and maximum dimensions found during preprocessing
    var_info : dict
        Dictionary of variable paths and associated VariableInfo
    """
    while not in_queue.empty():
        try:
            i, file = in_queue.get_nowait()
        except queue.Empty:
            break

        with nc.Dataset(file, 'r') as origin_dataset:
            origin_dataset.set_auto_maskandscale(False)

            for var_path, var_meta in var_info.items():
                ds_group, var_name = resolve_group(origin_dataset, var_path)
                ds_var = ds_group.variables[var_name]

                resized_arr = resize_var(ds_var, var_meta, max_dims)

                # Copy resized array to shared memory
                shared_mem = SharedMemory(create=True, size=resized_arr.nbytes)
                shared_arr = np.ndarray(resized_arr.shape, resized_arr.dtype, buffer=shared_mem.buf)
                np.copyto(shared_arr, resized_arr)

                out_queue.put((i, var_path, shared_arr.shape, shared_mem.name))
                shared_mem.close()


def _check_exit(processes):
    """
    Ensure that all processes have exited without error by checking their exitcode
    if they're no longer running. Processes that have exited properly are removed
    from the list

    Parameters
    ----------
    processes : list
        List of processes to check
    """

    for process in processes.copy():
        if not process.is_alive():
            if process.exitcode == 0:
                processes.remove(process)
            else:
                raise RuntimeError(f'Merging failed - exit code: {process.exitcode}')


def resize_var(var, var_info, max_dims):
    """
    Resizes a variable's data to the maximum dimensions found in preprocessing.
    This method will never downscale a variable and only performs bottom and
    left padding as utilized in the original Java implementation

    Parameters
    ----------
    var : nc.Variable
        variable to be resized
    group_path : str
        group path to this variable
    max_dims : dict
        dictionary of maximum dimensions found during preprocessing

    Returns
    -------
    np.ndarray
        An ndarray containing the resized data
    """
    # special case for 0d variables
    if var.ndim == 0:
        return var[:]

    # generate ordered array of new widths
    dims = [resolve_dim(max_dims, var_info.group_path, dim.name) - dim.size for dim in var.get_dims()]
    widths = [[0, dim] for dim in dims]

    # Legacy merger doesn't explicitly define this behavior, but its resizer
    # fills its resized arrays with 0s upon initialization. Sources:
    # https://github.com/Unidata/netcdf-java/blob/87f37eb82b6f862f71e0d5767470500b27af5d1e/cdm-core/src/main/java/ucar/ma2/Array.java#L52
    fill_value = 0 if var_info.fill_value is None else var_info.fill_value

    resized = np.pad(var, widths, mode='constant', constant_values=fill_value)
    return resized
