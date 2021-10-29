# stress-test

Utilities to used to stress test the merge module

## benchmark

Note: benchmark requires the `EARTHDATA_TOKEN` environment variable to be set
before its first run as it uses this token to download a set of granules from
PODAAC's AWS archive. After the first run, the token is no longer needed as
the future runs will use the local copies of the granules downloaded during
the first run.

To find your Earthdata token, visit the Earthdata login system @
https://urs.earthdata.nasa.gov/
