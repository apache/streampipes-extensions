package org.streampipes.processors.geo.jvm.database.dataFromRaster.pitch;


import org.locationtech.jts.geom.CoordinateList;
import org.locationtech.jts.geom.Geometry;
import org.streampipes.processors.geo.jvm.database.helper.SpDatabase;
import org.streampipes.logging.api.Logger;
import org.streampipes.model.runtime.Event;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;

import org.streampipes.processors.geo.jvm.config.*;

import java.sql.Connection;
import java.util.ArrayList;


import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;

public class Pitch implements EventProcessor<PitchParameter> {

    private static Logger LOG;
    private PitchParameter params;
    private Integer type;

    Connection conn;
    SpDatabase db;


    @Override
    public void onInvocation(PitchParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {
        LOG = params.getGraph().getLogger(Pitch.class);
        this.params = params;
        this.type = params.getType();


        String host = GeoJvmConfig.INSTANCE.getPostgresHost();
        Integer port = Integer.valueOf(GeoJvmConfig.INSTANCE.getPostgresPort());
        String dbName = GeoJvmConfig.INSTANCE.getPostgresDatabase();
        String user = GeoJvmConfig.INSTANCE.getPostgresUser();
        String password = GeoJvmConfig.INSTANCE.getPostgresPassword();


        this.db = new SpDatabase(host, port, dbName, user, password);
        this.conn = db.connect();

    }

    @Override
    public void onEvent(Event in, SpOutputCollector out){

        String wkt = in.getFieldBySelector(params.getWkt_string()).getAsPrimitive().getAsString();
        Integer epsgCode = in.getFieldBySelector(params.getEpsg_code()).getAsPrimitive().getAsInt();

        Geometry geometry = createSPGeom(wkt, epsgCode);


        if (!(geometry.getGeometryType()== "GeometryCollection")){
            if (geometry.getDimension() ==1){
                if (geometry.isValid()){
                    String query = db.prepareElevationQuery(geometry);
                    CoordinateList coords  = db.getElevationFromDB(query, conn, geometry);
                    ArrayList<Double> pitch = db.calcPitchFromDB(coords, type);
                    Double average = pitch.stream().mapToDouble(val -> Math.abs(val)).average().orElse(0.0);

                    in.addField(PitchController.ELEVATION, average);
                    out.collect(in);
                } else {
                    LOG.warn("Geometry is not valid " + PitchController.EPA_NAME);
                }
            } else {
                LOG.warn("Only LineStrings and MultiLineStrings are supported in the " + PitchController.EPA_NAME);
            }
        } else {
            LOG.warn("Geometry collections are not supported in the " + PitchController.EPA_NAME);
        }

    }

    @Override
    public void onDetach() {

        db.closeConnection(conn);

    }
}
