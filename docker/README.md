# Concise Service Docker Image

This directory contains the `Dockerfile` used to build the Docker image capable of running the Concise service.

## Building

The docker image is setup to install the Concise project into userspace using pip. It will look
in both PyPi and TestPyPi indexes unless building from a local wheel file.

In order to build the image the following build arguments are needed

- `SOURCE` : The value of this build arg will be used in the `pip install` command to install the Concise package 
- `DIST_PATH` (optional): The value of this build arg should be the path (relative to the context) to the directory containing a locally built wheel file 

### Building from PyPi or TestPyPi

If the version of the Concise package has already been uploaded to PyPi, all that is needed is to supply
the `SOURCE` build argument with the package specification.  

Example:

```shell script
docker build -f docker/Dockerfile --build-arg SOURCE="podaac-concise==1.1.0-alpha.9" .
```

### Building from local code

First build the project with Poetry.

```
poetry build
```

That will create a folder `dist/` and a wheel file that is named with the version of the software that was built. 

In order to use the local wheel file, the `DIST_PATH` build arg must be provided to the `docker build` command
and the `SOURCE` build arg should be set to the path to the wheel file.

Example:

```shell script
docker build -f docker/Dockerfile --build-arg SOURCE="dist/podaac-concise-1.1.0a1-py3-none-any.whl" --build-arg DIST_PATH="dist/" .
```

## Running

Running the docker image will invoke the [Harmony service](https://github.com/nasa/harmony-service-lib-py) CLI.  
