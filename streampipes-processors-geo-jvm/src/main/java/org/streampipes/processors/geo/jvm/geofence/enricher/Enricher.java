package org.streampipes.processors.geo.jvm.geofence.enricher;


import org.streampipes.processors.geo.jvm.config.GeoJvmConfig;
import org.streampipes.logging.api.Logger;
import org.streampipes.model.runtime.Event;
import org.streampipes.processors.geo.jvm.geofence.helper.SpInternalDatabase;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class Enricher implements EventProcessor<EnricherParameter> {

    private static Logger LOG;
    private EnricherParameter params;
    private Connection conn;
    private SpInternalDatabase db;
    private String geofence_name;



    @Override
    public void onInvocation(EnricherParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {
        LOG = params.getGraph().getLogger(Enricher.class);
        this.params = params;
        this.geofence_name = params.getGeofence_name();

        String host = GeoJvmConfig.INSTANCE.getPostgresHost();
        Integer port = Integer.valueOf(GeoJvmConfig.INSTANCE.getPostgresPort());
        String dbName = GeoJvmConfig.INSTANCE.getPostgresDatabase();
        String user = GeoJvmConfig.INSTANCE.getPostgresUser();
        String password = GeoJvmConfig.INSTANCE.getPostgresPassword();

        this.db = new SpInternalDatabase(host, port, dbName, user, password);
        this.conn = db.connect();

    }

    @Override
    public void onEvent(Event in, SpOutputCollector out) {

        String geofence_wkt = null;
        Integer geofence_epsg = null;
        Double geofence_area = null;
        String geofence_areaUnit = null;
        Double geofence_M = null;


        String query = "SELECT * FROM geofence.info WHERE name = " + geofence_name + ";";

        // try with resources (no finally block and double try catch necessary
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)){

            //reads all names and put it into the arraylist
            while (rs.next()) {
                geofence_area = rs.getDouble("area");
                geofence_areaUnit = rs.getString("areaunit");
                geofence_wkt = rs.getString("wkt");
                geofence_epsg = rs.getInt("epsg");
                geofence_M = rs.getDouble("m");
            }

        } catch (SQLException e) {
            //todo logger
            e.printStackTrace();
        }


        in.addField(EnricherController.GEOFENCE_NAME, geofence_name);
        in.addField(EnricherController.GEOFENCE_WKT, geofence_wkt);
        in.addField(EnricherController.GEOFENCE_EPSG, geofence_epsg);
        in.addField(EnricherController.GEOFENCE_AREA, geofence_area);
        in.addField(EnricherController.GEOFENCE_AREA_UNIT, geofence_areaUnit);
        in.addField(EnricherController.GEOFENCE_M_VALUE, geofence_M);

    }

    @Override
    public void onDetach() {

    }
}
