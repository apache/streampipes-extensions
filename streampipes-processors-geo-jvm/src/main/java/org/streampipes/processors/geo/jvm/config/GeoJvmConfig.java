/*
 * Copyright 2018 FZI Forschungszentrum Informatik
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.streampipes.processors.geo.jvm.config;


import org.streampipes.config.SpConfig;
import org.streampipes.container.model.PeConfig;

public enum GeoJvmConfig implements PeConfig {
  INSTANCE;

  private SpConfig config;

  public final static String serverUrl;
  public final static String iconBaseUrl;

  private final static String service_id = "pe/org.streampipes.processors.geo.jvm";
  private final static String service_name = "Processors Geo JVM";
  private final static String service_container_name = "processors-geo-jvm";

  GeoJvmConfig() {
    config = SpConfig.getSpConfig(service_id);
    config.register(ConfigKeys.HOST, service_container_name, "Hostname for the geo container");
    config.register(ConfigKeys.PORT, 8090, "Port for the pe esper");

    config.register(ConfigKeys.ICON_HOST, "backend", "Hostname for the icon host");
    config.register(ConfigKeys.ICON_PORT, 80, "Port for the icons in nginx");

    config.registerPassword(ConfigKeys.GOOGLE_API_KEY, "", "Google API Key for the routing service");

    config.register(ConfigKeys.SERVICE_NAME_KEY, service_name, "The name of the service");


    // internal postgres
    config.register(POSTGRES_HOST, "pgrouting", "host for postgres geofence database");
    config.register(POSTGRES_PORT, 65432, "port for postgres geofence database");
    config.register(POSTGRES_DATABASE, "streampipes", "databasename for postgres geofence database");
    config.register(POSTGRES_USER, "streampipes", "username for postgres geofence database");
    config.register(POSTGRES_PASSWORD, "streampipes", "password for postgres geofence database");




  }

  static {
    serverUrl = GeoJvmConfig.INSTANCE.getHost() + ":" + GeoJvmConfig.INSTANCE.getPort();
    iconBaseUrl = "http://" + GeoJvmConfig.INSTANCE.getIconHost() + ":" + GeoJvmConfig.INSTANCE.getIconPort() + "/assets/img/pe_icons";
  }

  public static final String getIconUrl(String pictureName) {
    return iconBaseUrl + "/" + pictureName + ".png";
  }

  @Override
  public String getHost() {
    return config.getString(ConfigKeys.HOST);
  }

  @Override
  public int getPort() {
    return config.getInteger(ConfigKeys.PORT);
  }

  public String getIconHost() {
    return config.getString(ConfigKeys.ICON_HOST);
  }

  public int getIconPort() {
    return config.getInteger(ConfigKeys.ICON_PORT);
  }

  public String getGoogleApiKey() {
    return config.getString(ConfigKeys.GOOGLE_API_KEY);
  }

  @Override
  public String getId() {
    return service_id;
  }

  @Override
  public String getName() {
    return config.getString(ConfigKeys.SERVICE_NAME_KEY);
  }


  // ================= getter for internal docker pgrouting /postgis/postgresql
  public String getPostgresHost() {
    return config.getString(POSTGRES_HOST);
  }
  public String getPostgresPort() {
    return config.getString(POSTGRES_PORT);
  }
  public String getPostgresDatabase() {
    return config.getString(POSTGRES_DATABASE);
  }
  public String getPostgresUser() {
    return config.getString(POSTGRES_USER);
  }
  public String getPostgresPassword() {
    return config.getString(POSTGRES_PASSWORD);
  }

}
