"""Preprocessing methods and the utilities to automagically run them in single-thread/multiprocess modes"""
import logging
import math
import queue
import time
import os
import shutil
import multiprocessing
from multiprocessing.shared_memory import SharedMemory
from pathlib import Path
import netCDF4 as nc
import numpy as np

from podaac.merger.path_utils import resolve_dim, resolve_group


def shared_memory_size() -> int:
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


def max_var_memory(file_list: list[Path], var_info: dict, max_dims) -> int:
    """Function to get the maximum shared memory that will be used for variables

    Parameters
    ----------
    file_list : list
        List of file paths to be processed
    var_info : dict
        Dictionary of variable paths and associated VariableInfo
    max_dims
    """

    max_var_mem = 0
    for file in file_list:
        with nc.Dataset(file, 'r') as origin_dataset:

            for var_path, var_meta in var_info.items():
                ds_group, var_name = resolve_group(origin_dataset, var_path)
                ds_var = ds_group.variables.get(var_name)

                if ds_var is None:
                    target_shape = tuple(resolve_dim(max_dims, var_meta.group_path, dim) for dim in var_meta.dim_order)
                    var_size = math.prod(target_shape) * var_meta.datatype.itemsize
                    max_var_mem = max(var_size, max_var_mem)
                else:
                    var_size = math.prod(ds_var.shape) * var_meta.datatype.itemsize
                    max_var_mem = max(var_size, max_var_mem)

    return max_var_mem


# pylint: disable=too-many-positional-arguments
def run_merge(merged_dataset: nc.Dataset,
              file_list: list[Path],
              var_info: dict,
              max_dims: dict,
              process_count: int,
              logger: logging.Logger):
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
    logger
    """

    if process_count == 1:
        _run_single_core(merged_dataset, file_list, var_info, max_dims, logger)
    else:
        # Merging is bottlenecked at the write process which is single threaded
        # so spinning up more than 2 processes for read/write won't scale the
        # optimization

        max_var_mem = max_var_memory(file_list, var_info, max_dims)
        max_memory_size = round(shared_memory_size() * .95)

        if max_var_mem < max_memory_size:
            _run_multi_core(merged_dataset, file_list, var_info, max_dims, 2, logger)
        else:
            _run_single_core(merged_dataset, file_list, var_info, max_dims, logger)


def _run_single_core(merged_dataset: nc.Dataset,
                     file_list: list[Path],
                     var_info: dict,
                     max_dims: dict,
                     logger: logging.Logger):
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
    logger
    """

    logger.info("Running single core ......")

    # Pre-allocate a reusable buffer per variable to avoid repeated allocations
    buffers = {}

    for i, file in enumerate(file_list):
        with nc.Dataset(file, 'r') as origin_dataset:
            origin_dataset.set_auto_maskandscale(False)

            for var_path, var_meta in var_info.items():
                ds_group, var_name = resolve_group(origin_dataset, var_path)
                merged_group = resolve_group(merged_dataset, var_path)
                ds_var = ds_group.variables.get(var_name)

                merged_var = merged_group[0].variables[var_name]

                if ds_var is None:
                    if var_meta.fill_value is None:
                        target_shape = tuple(resolve_dim(max_dims, var_meta.group_path, dim) for dim in var_meta.dim_order)
                        merged_var[i] = np.zeros(target_shape, dtype=var_meta.datatype)
                    continue

                # Get or create reusable buffer for this variable
                if var_path not in buffers:
                    target_shape = tuple(resolve_dim(max_dims, var_meta.group_path, dim) for dim in var_meta.dim_order)
                    if target_shape:
                        buffers[var_path] = np.empty(target_shape, dtype=var_meta.datatype)
                    else:
                        buffers[var_path] = None

                buf = buffers.get(var_path)
                resized = resize_var(ds_var, var_meta, max_dims, out=buf)
                merged_var[i] = resized


def _run_multi_core(merged_dataset: nc.Dataset,  # pylint: disable=too-many-locals
                    file_list: list[Path],
                    var_info: dict,
                    max_dims: dict,
                    process_count: int,
                    logger: logging.Logger):
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
    logger
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
                    target_shape = tuple(resolve_dim(max_dims, var_meta.group_path, dim) for dim in var_meta.dim_order)
                else:
                    target_shape = tuple(resolve_dim(max_dims, var_meta.group_path, dim.name) for dim in ds_var.get_dims())

                var_nbytes = math.prod(target_shape) * var_meta.datatype.itemsize if target_shape else var_meta.datatype.itemsize

                if var_nbytes > max_memory_size:
                    raise RuntimeError(f'Merging failed - MAX MEMORY REACHED: {var_nbytes}')

                while (memory_limit.value + var_nbytes) > max_memory_size > var_nbytes:
                    time.sleep(.5)

                # Allocate shared memory and resize directly into it
                shared_mem = SharedMemory(create=True, size=var_nbytes)
                shared_arr = np.ndarray(target_shape, var_meta.datatype, buffer=shared_mem.buf)

                if ds_var is None:
                    fill_value = 0 if var_meta.fill_value is None else var_meta.fill_value
                    shared_arr[:] = fill_value
                else:
                    resize_var(ds_var, var_meta, max_dims, out=shared_arr)

                with lock:
                    memory_limit.value = memory_limit.value + var_nbytes

                out_queue.put((i, var_path, shared_arr.shape, shared_mem.name))
                shared_mem.close()


def _check_exit(processes: list):
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


def resize_var(var: nc.Variable, var_info, max_dims: dict, out: np.ndarray = None) -> np.ndarray:
    """
    Resizes a variable's data to the maximum dimensions found in preprocessing.
    This method will never downscale a variable and only performs bottom and
    left padding as utilized in the original Java implementation

    Parameters
    ----------
    var : nc.Variable
        variable to be resized
    var_info
        contains a group path to this variable
    max_dims : dict
        dictionary of maximum dimensions found during preprocessing
    out : np.ndarray, optional
        pre-allocated output array to write into (avoids extra allocation)

    Returns
    -------
    np.ndarray
        An ndarray containing the resized data
    """
    if var.ndim == 0:
        return var[:]

    target_shape = tuple(resolve_dim(max_dims, var_info.group_path, dim.name) for dim in var.get_dims())
    src_shape = var.shape

    needs_padding = any(t > s for t, s in zip(target_shape, src_shape))

    if not needs_padding:
        if out is not None:
            var.set_auto_maskandscale(False)
            out[:] = var[:]
            return out
        return var[:]

    fill_value = 0 if var_info.fill_value is None else var_info.fill_value

    if out is None:
        out = np.full(target_shape, fill_value, dtype=var_info.datatype)
    else:
        out[:] = fill_value

    slices = tuple(slice(0, s) for s in src_shape)
    out[slices] = var[:]
    return out
