/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.streampipes.processors.python.config;

import org.apache.streampipes.config.SpConfig;
import org.apache.streampipes.container.model.PeConfig;

public enum Config implements PeConfig {

  INSTANCE;

  private final String SERVICE_ID = "pe/org.apache.streampipes.processor.python";
  private final SpConfig config;

  Config() {
    config = SpConfig.getSpConfig(SERVICE_ID);
    config.register(ConfigKeys.HOST, "processors-python", "hostname");
    config.register(ConfigKeys.PORT, 8090, "port");
    config.register(ConfigKeys.SERVICE_NAME, "Processors Python", "service name");
    config.register(ConfigKeys.PYTHON_ENDPOINT, "localhost:5000", "python endpoint");
  }

  public String getHost() {
    return config.getString(ConfigKeys.HOST);
  }

  public int getPort() {
    return config.getInteger(ConfigKeys.PORT);
  }

  public String getPythonEndpointUrl() {
    return "http://" + config.getString(ConfigKeys.PYTHON_ENDPOINT) + "/";
  }

  @Override
  public String getId() {
    return SERVICE_ID;
  }

  @Override
  public String getName() {
    return config.getString(ConfigKeys.SERVICE_NAME);
  }

}
