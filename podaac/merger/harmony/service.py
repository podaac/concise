"""A Harmony service wrapper around the Concise module"""

from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from shutil import copyfile
from urllib.parse import urlsplit
from uuid import uuid4
import traceback
import sys

from harmony_service_lib.adapter import BaseHarmonyAdapter
from harmony_service_lib.util import bbox_to_geometry, stage
from harmony_service_lib.exceptions import HarmonyException
from pystac import Catalog, Item
from pystac.item import Asset

from podaac.merger.merge import merge_netcdf_files
from podaac.merger.harmony.download_worker import multi_core_download
from podaac.merger.harmony.util import get_bbox, get_datetime, get_granule_url


NETCDF4_MIME = 'application/x-netcdf4'  # pylint: disable=invalid-name


class ConciseException(HarmonyException):
    """Concise Exception class for custom error messages to see in harmony api calls."""
    def __init__(self, original_exception):
        # Ensure we can extract traceback information
        if original_exception.__traceback__ is None:
            # Capture the current traceback if not already present
            try:
                raise original_exception
            except type(original_exception):
                original_exception.__traceback__ = sys.exc_info()[2]

        # Extract the last traceback entry (most recent call) for the error location
        tb = traceback.extract_tb(original_exception.__traceback__)[-1]

        # Get the error details: file, line, function, and message
        filename = tb.filename
        lineno = tb.lineno
        funcname = tb.name
        error_msg = str(original_exception)

        # Format the error message to be more readable
        readable_message = (f"Error in file '{filename}', line {lineno}, in function '{funcname}': "
                            f"{error_msg}")

        # Call the parent class constructor with the formatted message and category
        super().__init__(readable_message, 'podaac/concise')

        # Store the original exception for potential further investigation
        self.original_exception = original_exception


class ConciseService(BaseHarmonyAdapter):
    """
    A harmony-service-lib wrapper around the Concise module. This wrapper does
    not support Harmony calls that do not have STAC catalogs as support for
    this behavior is being depreciated in harmony-service-lib
    """

    def invoke(self):
        """
        Primary entrypoint into the service wrapper. Overrides BaseHarmonyAdapter.invoke
        """
        if not self.catalog:
            # Message-only support is being depreciated in Harmony, so we should expect to
            # only see requests with catalogs when invoked with a newer Harmony instance
            # https://github.com/nasa/harmony-service-lib-py/blob/21bcfbda17caf626fb14d2ac4f8673be9726b549/harmony/adapter.py#L71
            raise RuntimeError('Invoking CONCISE without a STAC catalog is not supported')

        return (self.message, self.process_catalog(self.catalog))

    def process_catalog(self, catalog: Catalog):
        """
        Recursively process a catalog and all its children. Adapted from
        BaseHarmonyAdapter._process_catalog_recursive to specifically
        support our particular use case for many-to-one

        Parameters
        ----------
        catalog : pystac.Catalog or pystac.Collection
            a catalog/collection to process for merging

        Returns
        -------
        pystac.Catalog
            A new catalog containing the results from the merge
        """

        try:
            result = catalog.clone()
            result.id = str(uuid4())
            result.clear_children()

            # Get all the items from the catalog, including from child or linked catalogs
            items = list(self.get_all_catalog_items(catalog))

            # Quick return if catalog contains no items
            if len(items) == 0:
                return result

            # -- Process metadata --
            bbox = []
            granule_urls = []
            datetimes = [
                datetime.max.replace(tzinfo=timezone.utc),  # start
                datetime.min.replace(tzinfo=timezone.utc)   # end
            ]

            for item in items:
                get_bbox(item, bbox)
                get_granule_url(item, granule_urls)
                get_datetime(item, datetimes)

            # Items did not have a bbox; valid under spec
            if len(bbox) == 0:
                bbox = None

            # -- Perform merging --
            collection = self._get_item_source(items[0]).collection
            first_granule_url = []
            get_granule_url(items[0], first_granule_url)
            first_url_name = Path(first_granule_url[0]).stem
            filename = f'{first_url_name}_{datetimes[1].strftime("%Y%m%dT%H%M%SZ")}_{collection}_merged.nc4'

            with TemporaryDirectory() as temp_dir:
                self.logger.info('Starting granule downloads')
                input_files = multi_core_download(granule_urls, temp_dir, self.message.accessToken, self.config)
                self.logger.info('Finished granule downloads')

                output_path = Path(temp_dir).joinpath(filename).resolve()
                merge_netcdf_files(input_files, output_path, granule_urls, logger=self.logger)
                staged_url = self._stage(str(output_path), filename, NETCDF4_MIME)

            # -- Output to STAC catalog --
            result.clear_items()
            properties = {
                "start_datetime": datetimes[0].isoformat(),
                "end_datetime": datetimes[1].isoformat()
            }

            item = Item(str(uuid4()), bbox_to_geometry(bbox), bbox, None, properties)
            asset = Asset(staged_url, title=filename, media_type=NETCDF4_MIME, roles=['data'])
            item.add_asset('data', asset)
            result.add_item(item)

            return result

        except Exception as ex:
            raise ConciseException(ex) from ex

    def _stage(self, local_filename, remote_filename, mime):
        """
        Stages a local file to either to S3 (utilizing harmony.util.stage) or to
        the local filesystem by performing a file copy. Staging location is
        determined by message.stagingLocation or the --harmony-data-location
        CLI argument override

        Parameters
        ----------
        local_filename : string
            A path and filename to the local file that should be staged
        remote_filename : string
            The basename to give to the remote file
        mime : string
            The mime type to apply to the staged file for use when it is served, e.g. "application/x-netcdf4"

        Returns
        -------
        url : string
            A URL to the staged file
        """
        url_components = urlsplit(self.message.stagingLocation)
        scheme = url_components.scheme

        if scheme == 'file':
            dest_path = Path(url_components.path).joinpath(remote_filename)
            self.logger.info('Staging to local filesystem: \'%s\'', str(dest_path))

            copyfile(local_filename, dest_path)
            return dest_path.as_uri()

        return stage(local_filename, remote_filename, mime,
                     logger=self.logger,
                     location=self.message.stagingLocation,
                     cfg=self.config
                     )
