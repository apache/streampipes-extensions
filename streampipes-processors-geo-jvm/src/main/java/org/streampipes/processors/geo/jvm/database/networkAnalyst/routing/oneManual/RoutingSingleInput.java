package org.streampipes.processors.geo.jvm.database.networkAnalyst.routing.oneManual;


import org.locationtech.jts.geom.CoordinateList;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
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

public class RoutingSingleInput implements EventProcessor<RoutingSingleInputParameter> {

    private static Logger LOG;
    private RoutingSingleInputParameter params;
    private Point inputPoint;
    private String chooser;
    private Boolean valid;
    private Connection conn;
    private SpDatabase db;
    private String query;
    private LineString routingGeom;



    @Override
    public void onInvocation(RoutingSingleInputParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {
        {

            LOG = params.getGraph().getLogger(RoutingSingleInputParameter.class);

            this.params = params;
            this.inputPoint = params.getPoint();
            this.chooser = params.getChooser();

            String host = GeoJvmConfig.INSTANCE.getPostgresHost();
            Integer port = Integer.valueOf(GeoJvmConfig.INSTANCE.getPostgresPort());
            String dbName = GeoJvmConfig.INSTANCE.getPostgresDatabase();
            String user = GeoJvmConfig.INSTANCE.getPostgresUser();
            String password = GeoJvmConfig.INSTANCE.getPostgresPassword();


            this.db = new SpDatabase(host, port, dbName, user, password);
            this.conn = db.connect();


        }
    }


    @Override
    public void onEvent(Event in, SpOutputCollector out)  {


        String wkt = in.getFieldBySelector(params.getWkt()).getAsPrimitive().getAsString();
        Integer epsgCode = in.getFieldBySelector(params.getEpsg()).getAsPrimitive().getAsInt();

        Geometry geometry = createSPGeom(wkt, epsgCode);



        if (geometry instanceof Point){


            //checks if point is inside routing area
            valid = db.isInsideRoutingArea(conn, (Point) geometry);


            // if wkt was wrong empty point ia created, wich leeds to empty routing line
            if (geometry.isEmpty()){
                routingGeom = geometry.getFactory().createLineString();
                LOG.warn("Empty points result in" + RoutingSingleInputController.EPA_NAME + ". " +
                        "Manual input coordinates seems to be invalid. An empty LineString will be returned");
            }
            if (!valid){
                // if not valid, create an empty line
                LOG.warn("Region of input points is not supported in the routing network. An empty polygon is returned");
                routingGeom = geometry.getFactory().createLineString();
            } else {

                if (chooser.equals("Start")) {
                    query = db.prepareRoutingQuery(inputPoint, (Point) geometry);
                } else {
                    query = db.prepareRoutingQuery((Point) geometry, inputPoint );

                }

                CoordinateList routingPointList = db.getResultFromDB(query, conn, true);
                routingGeom =  (LineString) db.createSPGeometryFromDB(routingPointList, true);

            }

            in.addField(RoutingSingleInputController.ROUTING_TEXT, routingGeom.toString());
            out.collect(in);

        } else {
            LOG.warn("Only points are supported in the " + RoutingSingleInputController.EPA_NAME + "but input type is " + geometry.getGeometryType());

        }



    }

    @Override
    public void onDetach() {
        db.closeConnection(conn);
    }
}
