# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#---------------------------------------------------------------------------------------------------------
# Stage 1: Python build
#---------------------------------------------------------------------------------------------------------
FROM python:3.7-slim-buster AS python-build
RUN mkdir -p /svc/python
COPY ./python /svc/python
WORKDIR /svc/python
RUN pip install --upgrade pip; \
    pip install /svc/python/dist/apache_streampipes_python-0.68.0.dev1-py3-none-any.whl; \
    pip install --no-cache-dir --compile -r /svc/python/requirements.txt

#---------------------------------------------------------------------------------------------------------
# Stage 2: Final image
#---------------------------------------------------------------------------------------------------------
FROM fogsyio/openjdk8-openj9-python:3.7-slim-buster

EXPOSE 8090
ENV CONSUL_LOCATION=consul

RUN set -eux; \
    \
    apt-get update; \
    apt-get install -y --no-install-recommends supervisor; \
    \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

COPY supervisor.conf  /etc/supervisor.conf

# Copy from build stages
COPY ./target /svc/java
COPY --from=python-build /svc/python /svc/python
COPY --from=python-build /usr/local/lib/python3.7/site-packages /usr/local/lib/python3.7/site-packages

WORKDIR /svc

CMD ["supervisord", "-c", "/etc/supervisor.conf"]
