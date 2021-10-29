Welcome to CONCISE (CONCatenatIon SErvice)
==========================================

CONCISE is a Python package for merging dataset granules together. The package is a port of the
merging functionality from the l2ss-subset-worker Java project primarily utilizing the
netcdf4-python package and *a lot* of multi-core optimizations. The primary merging logic was
derrived from the :code:`gov.nasa.jpl.podaac.subsetter.netcdf.NetCDFSubsetter` class. The
package includes an integration with the Harmony service via :code:`harmony-service-lib`.

The integration with upstream Harmony itself is currently incomplete. A fork of Harmony was
created to test the integration in the meanwhile. In order to workaround Harmony's current
lack of support for many-to-one services and OCG's Coverages API specification, a custom MIME
was defined as part of the :code:`output_formats` for the service in :code:`services.yml`. A
snippet of this workaround follows.

.. code-block:: yaml

    capabilities:
      output_formats:
        - application/x-netcdf4.merge

One possible route towards integrating CONCISE with Harmony and maintaining OCG API compliance
is to extend the NetCDF4 MIME with additional metadata. For example:
:code:`application/x-netcdf4;merged=true`. Metadata at the end of MIMEs is seen as a valid part
of the MIME specification according to
`RFC 6838 <https://datatracker.ietf.org/doc/html/rfc6838#section-4.2.5>`_.

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
