# CONCISE (CONCatenatIon SErvice)

[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=podaac_concise&metric=coverage)](https://sonarcloud.io/dashboard?id=podaac_concise)  
develop: [![Develop Build](https://github.com/podaac/concise/actions/workflows/build-pipeline.yml/badge.svg?branch=develop)](https://github.com/podaac/concise/actions/workflows/build-pipeline.yml)  
main: [![Main Build](https://github.com/podaac/concise/actions/workflows/build-pipeline.yml/badge.svg?branch=main&event=push)](https://github.com/podaac/concise/actions/workflows/build-pipeline.yml)

CONCISE is a NASA Harmony service designed for concatenating Level 2 (L2) NetCDF data files. It enables users to combine multiple NetCDF files into a single, unified file with an additional dimension that preserves the original file structure while creating a cohesive dataset for analysis.

## Overview

CONCISE integrates with NASA's [Harmony](https://harmony.earthdata.nasa.gov/) platform to provide seamless data transformation services in the cloud. The service is particularly useful for researchers and data scientists working with satellite oceanography data from PO.DAAC (Physical Oceanography Distributed Active Archive Center) who need to combine multiple granules into a single analysis-ready dataset.

### Key Features

- **NetCDF Concatenation**: Combines multiple NetCDF files into a single file
- **Dimension Preservation**: Adds an extra dimension where each slice corresponds to one input file
- **Harmony Integration**: Seamlessly works within NASA's Earthdata Cloud ecosystem
- **L2 Data Support**: Optimized for Level 2 satellite data products
- **Cloud-Native**: Designed for efficient processing in AWS cloud environment

## How It Works

When CONCISE processes multiple NetCDF files:

1. **Input**: Receives multiple NetCDF files as input
2. **Analysis**: Examines the structure and dimensions of each file
3. **Concatenation**: Creates a new NetCDF file with an additional dimension
4. **Output**: Each slice in the new dimension corresponds to data from one input file

The resulting concatenated file maintains data integrity while providing a unified structure for downstream analysis.

## Installation

### Prerequisites

- Python 3.12 or higher
- Poetry (for dependency management)

### Setup

1. Clone the repository:
```bash
git clone https://github.com/podaac/concise.git
cd concise
```

2. Install dependencies using Poetry:
```bash
poetry install
```

3. Activate the virtual environment:
```bash
poetry shell
```

### Development and Testing

For local development and testing:

```bash
# Run unit tests
poetry run pytest tests/

# Run with specific test options
poetry run pytest tests/ -v

# Run linting
poetry run pylint podaac

# Run flake
poetry run flake8 podaac
```

## Supported Data Types

CONCISE is optimized for:

- **NetCDF4 files**: Primary format for input and output
- **Level 2 satellite data**: Oceanographic and atmospheric datasets
- **Gridded data products**: Regular and irregular grids

## Contributing


We welcome contributions to CONCISE! Please refer to our [contribution document](CONTRIBUTING.md) for detailed information on:

- Code style and conventions
- Testing requirements
- Pull request process
- Issue reporting

### Development Workflow

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature-name`
3. Make your changes and add tests
4. Run the test suite: `poetry run pytest tests/`
5. Submit a pull request

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

CONCISE is developed and maintained by the Physical Oceanography Distributed Active Archive Center (PO.DAAC) at NASA's Jet Propulsion Laboratory (JPL). This work is supported by NASA's Earth Science Data and Information System (ESDIS) project.

## Contact

- **Project Maintainers**: PO.DAAC Development Team
- **Issues**: [GitHub Issues](https://github.com/podaac/concise/issues)
- **Documentation**: [PO.DAAC Documentation](https://podaac.github.io/concise)

---

For more information about NASA's Earth science data and services, visit [Earthdata](https://earthdata.nasa.gov/).