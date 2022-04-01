# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
### Changed 
- Updated dependency versions
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
