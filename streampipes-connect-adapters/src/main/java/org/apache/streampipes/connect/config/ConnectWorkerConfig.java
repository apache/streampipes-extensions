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

package org.apache.streampipes.connect.config;


import org.apache.streampipes.config.SpConfig;
import org.apache.streampipes.container.model.ExtensionsConfig;

public enum ConnectWorkerConfig implements ExtensionsConfig {
  INSTANCE;

  private static final String CONNECT_ID = "connect/org.apache.streampipes.connect.adapter";
  private final static String SERVICE_CONTAINER_NAME = "connect-worker-main";
  private final SpConfig config;

  ConnectWorkerConfig() {
    config = SpConfig.getSpConfig(CONNECT_ID);

    config.register(ConfigKeys.SP_HOST, SERVICE_CONTAINER_NAME, "Connect container host");
    config.register(ConfigKeys.SP_PORT, 8098, "Connect container port");
    config.register(ConfigKeys.BACKEND_HOST, "backend", "Backend host");
    config.register(ConfigKeys.BACKEND_PORT, 8030, "Backend port");
  }

  @Override
  public String getId() {
    return CONNECT_ID;
  }

  @Override
  public String getHost() {
    return config.getString(ConfigKeys.SP_HOST);
  }

  @Override
  public int getPort() {
    return config.getInteger(ConfigKeys.SP_PORT);
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public String getBackendHost() {
    return config.getString(ConfigKeys.BACKEND_HOST);
  }

  @Override
  public int getBackendPort() {
    return config.getInteger(ConfigKeys.BACKEND_PORT);
  }
}
