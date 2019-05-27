package org.streampipes.processors.geo.jvm.database.networkAnalyst.isochrone;


import org.locationtech.jts.geom.CoordinateList;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.streampipes.processors.geo.jvm.config.GeoJvmConfig;
import org.streampipes.processors.geo.jvm.database.helper.SpDatabase;
import org.streampipes.logging.api.Logger;
import org.streampipes.model.runtime.Event;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;

import java.sql.Connection;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;


public class Isochrone implements EventProcessor<IsochroneParameter> {

    private static Logger LOG;
    private IsochroneParameter params;
    private Integer measure;
    private Boolean unitIsSecond;
    private Boolean valid;
    private Geometry isochrone;

    private Connection conn;
    private SpDatabase db;


    @Override
    public void onInvocation(IsochroneParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {


        LOG = params.getGraph().getLogger(Isochrone.class);
        this.params = params;
        this.measure = params.getMeasure();
        this.unitIsSecond = params.getUnit();


        String host = GeoJvmConfig.INSTANCE.getPostgresHost();
        Integer port = Integer.valueOf(GeoJvmConfig.INSTANCE.getPostgresPort());
        String dbName = GeoJvmConfig.INSTANCE.getPostgresDatabase();
        String user = GeoJvmConfig.INSTANCE.getPostgresUser();
        String password = GeoJvmConfig.INSTANCE.getPostgresPassword();


        this.db = new SpDatabase(host, port, dbName, user, password);
        this.conn = db.connect();
    }

    @Override
    public void onEvent(Event in, SpOutputCollector out)  {


        String wkt = in.getFieldBySelector(params.getWkt_string()).getAsPrimitive().getAsString();
        Integer epsgCode = in.getFieldBySelector(params.getEpsg_code()).getAsPrimitive().getAsInt();
        Geometry geometry = createSPGeom(wkt, epsgCode);


        if (geometry instanceof Point){

            //checks if point is inside routing area
            valid = db.isInsideRoutingArea(conn, (Point) geometry);

            if (!valid){

                // empty polygon is created
                isochrone = geometry.getFactory().createPolygon();
                LOG.warn("Region of input point is not supported in the routing network. An empty polygon is returned");

            } else {
                String query = db.prepareIsochroneQuery((Point) geometry, measure, unitIsSecond);
                CoordinateList coordList= db.getResultFromDB(query, conn, false);
                isochrone = db.createSPGeometryFromDB(coordList, false);
            }


            if (isochrone.isValid()){
                in.addField(IsochroneController.ISOCHRONE, isochrone.toText());
                out.collect(in);
            } else {
                LOG.warn("Geometry output is invalid in the " + IsochroneController.EPA_NAME + ". Either empty or self-intersecting and is not parsed into the output stream ");


            }


            //LOG.info(in.toString());

        } else {
            LOG.warn("Only points are supported in the " + IsochroneController.EPA_NAME + "but input type is " + geometry.getGeometryType());
        }

    }

    @Override
    public void onDetach() {

        db.closeConnection(conn);

    }
}
