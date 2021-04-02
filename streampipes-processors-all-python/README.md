## [WIP] StreamPipes Data Processor in Python

This module is currently under development:
* [STREAMPIPES-174: New Python Wrapper](https://issues.apache.org/jira/projects/STREAMPIPES/issues/STREAMPIPES-174?filter=allopenissues) 

## Setup
Clone core repository
````bash
git clone https://github.com/apache/incubator-streampipes
````
Build StreamPipes python wheel in `streampipes-python-wrapper` in core repository
```bash
cd incubator-streampipes/streampipes-wrapper-python
./build-distribution
```
Copy wheel to `python/dist` directory
```bash
cp /path/to/incubator-streampipes/streampipes-wrapper-python/dist/apache_streampipes_python-0.68.0.dev1-py3-none-any.whl
 ./python/dist
```
>**NOTE**: We recommend creating a virtualenv before installing the packages.
 
Install StreamPipes python wheel
```bash
pip install ./python/dist/apache_streampipes_python-0.68.0.dev1-py3-none-any.whl
```

## Development
Currently, we rely on Java as an interface to the backend, where we declare the `DataProcessorDescription` model and
 in turn receive a `DataProcessorInvokation` upon pipeline start from the backend. Requests are then further routet
  via `HTTP` requests to the Python-side in order to start/stop dedicated processors.
  
Thus, start the Java main class `PythonProcessorInit.java` with the following environment variables:
```bash
SP_DEBUG=true
SP_HOST=host.docker.internal
SP_PORT=8005
SP_PYTHON_ENDPOINT=localhost:5000
```
Then start the python `main.py` with the following environment variables:
```bash
PYTHONUNBUFFERED=1
SP_HOST=host.docker.internal
SP_PORT=5000
SP_DEBUG=true
SP_SERVICE_NAME=Python Processor
```

## Build Docker image
Maven package from root
````bash
# root folder
mvn clean package
````
Build Docker image
````bash
docker build -t apachestreampipes/processors-python:0.68.0-SNAPSHOT .
````

Run Docker container
>*NOTE*: make sure that you have StreamPipes core services (backend etc) including consul up and running
```bash
docker run -ti \
    --rm \
    --net=spnet \
    --name processors-python \
    apachestreampipes/processors-python:0.68.0-SNAPSHOT
```