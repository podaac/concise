# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
### Changed
### Deprecated
### Removed
### Fixed


## [0.10.0]

### Added
  - Update Github Actions
    - Added harmony deployment into github actions.
### Changed
  - [issue #117](https://github.com/podaac/concise/issues/117): Add part of URL to output file name
  - Update python libraries
    - Update harmony service lib that changed project structure
  - Add Concise exception to propogate up to harmony api calls.
### Deprecated
### Removed
### Fixed
  - Variable Merging
    - Fixed way we merge variables when granules in a collection have varying variables.


## [0.9.0]

### Added
### Changed
  - Updated python libraries
  - [issue #114](https://github.com/podaac/concise/issues/114): add type annotations
### Deprecated
### Removed
  - Removed CMR testing.  Now done in [concise-autotest](https://github.com/podaac/concise-autotest) repo
    - Moved add_collection_test.py test to verify_collection.py (in concise-autotest repo)
### Fixed


## [0.8.0]

### Added
### Changed 
  - [issues/96](https://github.com/podaac/concise/issues/96):
    - Preserve the order of the input files so the output file matches order
### Deprecated
### Removed
### Fixed


## [0.7.0]

### Added
  - PODAAC-5424
    - Update concise test in uat to test only POCLOUD collections
  - Publishing L2ss Concise Chain umm-s Update
### Changed 
  - Issue-68
    - Updated jupyter notebook
    - Update notebook test to use python code directly instead of using jupyter notebook
  - Updated python libraries
  - Update history json to have url in history 
  - Update add collection test to use url in json history
### Deprecated 
### Removed
### Fixed


## [0.6.1]

### Added
### Changed 
  - PODAAC-5065
    - Update when to sleep when not enough shared memory
### Deprecated 
### Removed
### Fixed


## [0.6.0]

### Added
  - PODAAC-4658
    - Updated jupyter-test workflow to use environment variables
    - Updated jupyter-test workflow to inlcude slack reporting, collections update and removal
  - PODAAC-4659
    - Removed branch restriction from jupyter-test workflow on success notebook pass
    - Updated token handling for association removal
    - Some log formatting for jupyter-test workflow
  - PODAAC-4660
    - Updated cmr-umm-updater to newer version and added input argument to disable association removal
  - PODAAC-4976
    - Added dateutil dependency to Jupyter-test workflow
  - PODAAC-5065
    - Calculate how much shared memory will be used and determine to use multicore or singlecore concise.
### Changed 
### Deprecated 
### Removed
### Fixed
### Security


## [0.5.0]

### Added
  - New github action workflow and Jupyter notebook that can be used for testing the concise service via Harmony
  - PODAAC-4653
    - New github action workflow and Jupyter notebook that can be used for testing the concise service via Harmony
  - PODAAC-4656
    - Added cmr-association-diff import and usage
    - Added secret usage to workflow
    - Added FileHandler class
### Changed 
  - [issues/34](https://github.com/podaac/concise/issues/34): harmony-service-lib-py version updated to ^1.0.20 to support reading/writing STAC objects to/from S3
### Deprecated 
### Removed
### Fixed
### Security

## [0.4.0]

### Added
 - HARMONY-1178
   - Handle paged STAC catalogs
### Changed 
- Updated dependency versions
- [issue-31](https://github.com/podaac/l2ss-py/issues/88): Build pipeline manually pushes tag rather than use action-push-tag
### Deprecated 
### Removed
### Fixed
### Security
- Changed CLI step in build action to use snyk monitor so that report is uploaded to SNYK podaac org

## [0.3.0]

### Added
  - PODAAC-4171
    - Add AVHRRMTA_G-NAVO-L2P-v1.0 to associations
    - Added in shared memory allocation limit in fork process
  - PODAAC-4173
    - Add AVHRRMTB_G-NAVO-L2P-v1.0 to associations
  - [issue 10](https://github.com/podaac/concise/issues/10):
    Handle empty granule files.
  - [issue-14](https://github.com/podaac/concise/issues/14): Added support 
    for concatenating granules together that have different variables
  - Added `timeout` option to `cmr-umm-updater`
### Changed 
  - Upgraded `cmr-umm-updater` to 0.2.1
### Deprecated 
### Removed 
### Fixed 
### Security

## [0.2.0]

### Added
- history_json attribute is populated
### Changed 
### Deprecated 
### Removed 
### Fixed 
  - Fixed bug where VariableInfo equality fails when _FillValue is np.nan
### Security

## [0.1.0]

### Added
  - PODAAC-3504
    - As a developer, I want to create a Python package capable of merging granules together
  - PODAAC-3526
    - As a developer, I want to parallelize the granule merge service to improve performance
  - PODAAC-3604
    - As a developer, I want to create a deployment pipeline for the granule concatenation service
  - PODAAC-3607
    - As a developer, I want to create a Harmony service lib wrapper for the granule concatenation service
  - PODAAC-3663
    - As a developer, I want the subset_files data in my concatenated data to contain original filenames
  - PODAAC-3860
    - Created a UMM-S record for Concise
    - Utilize cmr-umm-updater Github Action for auto-publication of UMM-S changes/version bumps
### Changed 
  - Moved to GitHub.com!
### Deprecated 
### Removed 
### Fixed 
### Security
