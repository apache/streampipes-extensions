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
package org.apache.streampipes.extensions.all.jvm.config;

import org.apache.streampipes.config.SpConfig;
import org.apache.streampipes.container.model.EdgeExtensionsConfig;
import org.apache.streampipes.container.model.ExtensionsConfig;

import java.net.InetAddress;
import java.net.UnknownHostException;

public enum AllExtensionsConfig implements EdgeExtensionsConfig {
    INSTANCE;

    private final SpConfig peConfig;
    private final SpConfig adapterConfig;
    private final static String PE_ID = "pe/org.apache.streampipes.processors.all.jvm";
    private final static String CONNECT_ID = "connect/org.apache.streampipes.connect.adapter";
    private final static String SERVICE_NAME = "StreamPipes Extensions (JVM)";
    private final static String SERVICE_CONTAINER_NAME = "extensions-all-jvm";

    AllExtensionsConfig() {
        // TODO: harmonize config
        peConfig = SpConfig.getSpConfig(PE_ID);
        adapterConfig = SpConfig.getSpConfig(CONNECT_ID);

        peConfig.register(ConfigKeys.HOST, SERVICE_CONTAINER_NAME, "Host for extensions");
        peConfig.register(ConfigKeys.PORT, 8090, "Port for extensions");
        peConfig.register(ConfigKeys.SERVICE_NAME_KEY, SERVICE_NAME, "Service name");

        // TODO: del when working
        adapterConfig.register(ConfigKeys.BACKEND_HOST, "backend", "backend host");
        adapterConfig.register(ConfigKeys.BACKEND_PORT, 8030, "backed port");

//        adapterConfig.register(ConfigKeys.NODE_CONTROLLER_CONTAINER_HOST, "node-controller", "node controller host");
//        adapterConfig.register(ConfigKeys.NODE_CONTROLLER_CONTAINER_PORT, 7077, "node controller port");
    }


    @Override
    public String getHost() {
        return peConfig.getString(ConfigKeys.HOST);
    }

    @Override
    public int getPort() {
        return peConfig.getInteger(ConfigKeys.PORT);
    }

    @Override
    public String getId() {
        return PE_ID;
    }

    @Override
    public String getName() {
        return peConfig.getString(ConfigKeys.SERVICE_NAME_KEY);
    }

    @Override
    public String getNodeControllerHost() {
        return getEnvOrDefault(ConfigKeys.NODE_CONTROLLER_CONTAINER_HOST, "node-controller", String.class);
    }

    @Override
    public int getNodeControllerPort() {
        return getEnvOrDefault(ConfigKeys.NODE_CONTROLLER_CONTAINER_PORT, 7077, Integer.class);
    }

    @Override
    public String getBackendHost() {
        return adapterConfig.getString(ConfigKeys.BACKEND_HOST);
    }

    @Override
    public int getBackendPort() {
        return adapterConfig.getInteger(ConfigKeys.BACKEND_PORT);
    }


    private String checkHostOrUseDefault(String key, String defaultValue) {
        if ("true".equals(System.getenv("SP_DEBUG"))) {
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                throw new RuntimeException("Could not retrieve host IP", e);
            }
        } else {
            return getEnvOrDefault(key, defaultValue, String.class);
        }
    }

    private <T> T getEnvOrDefault(String k, T defaultValue, Class<T> type) {
        if(type.equals(Integer.class)) {
            return System.getenv(k) != null ? (T) Integer.valueOf(System.getenv(k)) : defaultValue;
        } else if(type.equals(Boolean.class)) {
            return System.getenv(k) != null ? (T) Boolean.valueOf(System.getenv(k)) : defaultValue;
        } else {
            return System.getenv(k) != null ? type.cast(System.getenv(k)) : defaultValue;
        }
    }
}
