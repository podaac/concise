Welcome to CONCISE (CONCatenatIon SErvice)
==========================================

|version_badge|

CONCISE is a Python package and Harmony service for concatenating Level 2 (L2) dataset granules together. 
It is designed to efficiently merge large multi-dimensional datasets, leveraging `netcdf4-python` and multi-core optimizations. 
The service provides a robust, production-grade solution for NASA's data processing pipelines, enabling scalable and performant granule merging.

Features
--------

- Supports concatenation of L2 granules for Earth science data.
- Designed as a Harmony backend service for seamless integration into NASA Harmony workflows.
- High-performance merging using multi-core processing.
- Comprehensive unit tests included for code reliability.
- Distributed as a Python package (published to PyPI).
- Docker image available for containerized deployments.

Contributing
------------

If you would like to contribute to CONCISE, refer to the `contribution document <https://github.com/podaac/concise/blob/main/CONTRIBUTING.md>`_.

How to Test Locally
-------------------

CONCISE provides comprehensive unit tests that can be run locally. 
To run all tests:

.. code-block:: shell

    poetry run pytest tests/

Harmony Integration
-------------------

CONCISE is implemented as a Harmony service, enabling it to fit into NASA's scalable, cloud-based data processing environment. 
For more on Harmony, see the `harmony-service-lib <https://github.com/nasa/harmony-service-lib-py>`_.


.. toctree::
    :maxdepth: 3
    :titlesonly:
    :caption: Contents:

    modules

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`