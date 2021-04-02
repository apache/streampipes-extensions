#
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
#
from streampipes.declarer import DeclarerSingleton
from streampipes.model.pipeline_element_config import Config
from streampipes.submitter import StandaloneModelSubmitter

from donothing import DoNothing
from greeter import Greeter


def main():

    config = Config(app_id='pe/org.apache.streampipes.processors.python')

    config.register(type='host',
                    env_key='SP_HOST',
                    default='processor-python',
                    description='processor hostname')

    config.register(type='port',
                    env_key='SP_PORT',
                    default=5000,
                    description='processor port')

    config.register(type='service',
                    env_key='SP_SERVICE_NAME',
                    default='Python Processor',
                    description='processor service name')

    config.register(type='location',
                    env_key='SP_PYTHON_ENDPOINT',
                    default='localhost:5000',
                    description='python endpoint')


    # dict with processor id and processor class
    # key: must match the id in the java part
    processors = {
        'org.apache.streampipes.processors.python.greeter': Greeter,
        'org.apache.streampipes.processors.python.donothing': DoNothing,
    }

    # Declarer
    # add the dict of processors to the Declarer
    # This is an abstract class that holds the specified processors
    DeclarerSingleton.add(processors=processors)

    # StandaloneModelSubmitter
    # Initializes the REST api
    # StandaloneModelSubmitter.init(config=config)

    # init_debug(config=config) is only used to start web server on correct port to mock future method. While we are
    # already able to register the service and config in Consul, the REST endpoint exposed does not provide any
    # description of the processors at this point - this is still done in the declareModel() in Java
    StandaloneModelSubmitter.init_debug(config=config)


if __name__ == '__main__':
    main()