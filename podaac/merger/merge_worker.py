"""Preprocessing methods and the utilities to automagically run them in single-thread/multiprocess modes"""

import multiprocessing
from multiprocessing.shared_memory import SharedMemory
import queue
import time
import os
import shutil
import netCDF4 as nc
import numpy as np

from podaac.merger.path_utils import resolve_dim, resolve_group


def shared_memory_size():
    """
    try to get the shared memory space size by reading the /dev/shm on linux machines
    """
    try:
        stat = shutil.disk_usage("/dev/shm")
        return stat.total
    except FileNotFoundError:
        # Get memory size via env or default to 60 MB
        default_memory_size = os.getenv("SHARED_MEMORY_SIZE", "60000000")
        return int(default_memory_size)


def run_merge(merged_dataset, file_list, var_info, max_dims, process_count, logger):
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
        _run_multi_core(merged_dataset, file_list, var_info, max_dims, 2, logger)


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

            for var_path, var_meta in var_info.items():
                ds_group, var_name = resolve_group(origin_dataset, var_path)
                merged_group = resolve_group(merged_dataset, var_path)
                ds_var = ds_group.variables.get(var_name)

                merged_var = merged_group[0].variables[var_name]

                if ds_var is None:
                    fill_value = var_meta.fill_value
                    target_shape = tuple(max_dims[f'/{dim}'] for dim in var_meta.dim_order)
                    merged_var[i] = np.full(target_shape, fill_value)
                    continue

                resized = resize_var(ds_var, var_meta, max_dims)
                merged_var[i] = resized


def _run_multi_core(merged_dataset, file_list, var_info, max_dims, process_count, logger):  # pylint: disable=too-many-locals
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

    logger.info("Running multicore ......")
    total_variables = len(file_list) * len(var_info)
    logger.info(f"total variables {total_variables}")

    # Ensure SharedMemory doesn't get cleaned up before being processed
    context = multiprocessing.get_context('forkserver')

    with context.Manager() as manager:
        in_queue = manager.Queue(len(file_list))
        out_queue = manager.Queue((process_count - 1) * len(var_info))  # Store (process_count - 1) granules in buffer
        memory_limit = manager.Value('i', 0)
        lock = manager.Lock()

        logger.info(file_list)
        for i, file in enumerate(file_list):
            in_queue.put((i, file))

        processes = []

        logger.info("creating read processes")
        for _ in range(process_count - 1):
            process = context.Process(target=_run_worker, args=(in_queue, out_queue, max_dims, var_info, memory_limit, lock))
            processes.append(process)
            process.start()

        processed_variables = 0

        logger.info("Start processing variables in main process")
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
            with lock:
                memory_limit.value = memory_limit.value - resized_arr.nbytes
            processed_variables = processed_variables + 1

        for process in processes:
            # Ensure that child processes properly exit before manager context
            # gets GCed. Solves EOFError
            process.join()


def _run_worker(in_queue, out_queue, max_dims, var_info, memory_limit, lock):
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

    # want to use max 95% of the memory size of disk
    max_memory_size = round(shared_memory_size() * .95)

    while not in_queue.empty():

        try:
            i, file = in_queue.get_nowait()
        except queue.Empty:
            break

        with nc.Dataset(file, 'r') as origin_dataset:
            origin_dataset.set_auto_maskandscale(False)

            for var_path, var_meta in var_info.items():

                ds_group, var_name = resolve_group(origin_dataset, var_path)
                ds_var = ds_group.variables.get(var_name)

                if ds_var is None:
                    fill_value = var_meta.fill_value
                    target_shape = tuple(max_dims[f'/{dim}'] for dim in var_meta.dim_order)
                    resized_arr = np.full(target_shape, fill_value)
                else:
                    resized_arr = resize_var(ds_var, var_meta, max_dims)

                if resized_arr.nbytes > max_memory_size:
                    raise RuntimeError(f'Merging failed - MAX MEMORY REACHED: {resized_arr.nbytes}')

                # Limit to how much memory we allocate to max memory size
                while memory_limit.value + resized_arr.nbytes > max_memory_size and not out_queue.empty():
                    time.sleep(.5)

                # Copy resized array to shared memory
                shared_mem = SharedMemory(create=True, size=resized_arr.nbytes)
                shared_arr = np.ndarray(resized_arr.shape, resized_arr.dtype, buffer=shared_mem.buf)
                np.copyto(shared_arr, resized_arr)
                with lock:
                    memory_limit.value = memory_limit.value + resized_arr.nbytes

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
