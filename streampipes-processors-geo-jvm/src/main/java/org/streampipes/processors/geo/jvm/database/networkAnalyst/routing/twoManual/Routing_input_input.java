package org.streampipes.processors.geo.jvm.database.networkAnalyst.routing.twoManual;


import org.locationtech.jts.geom.CoordinateList;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.streampipes.processors.geo.jvm.database.helper.SpDatabase;
import org.streampipes.logging.api.Logger;
import org.streampipes.model.runtime.Event;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;

import java.sql.Connection;


public class Routing_input_input implements EventProcessor<Routing_input_inputParameter> {

    private static Logger LOG;
    private Routing_input_inputParameter params;
    private Point start;
    private Point dest;
    private Boolean valid;
    private Connection conn;
    private SpDatabase db;
    LineString routingGeom;




    @Override
    public void onInvocation(Routing_input_inputParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(Routing_input_inputParameter.class);
        this.params = params;
        this.start = params.getStart();
        this.dest = params.getDest();



        String host = "localhost";
        int port = 65432;
        String dbName = "bw";
        String user = "florian";
        String password = "guge8adi";


        this.db = new SpDatabase(host, port, dbName, user, password);
        this.conn = db.connect();


    }

    @Override
    public void onEvent(Event in, SpOutputCollector out) {



        valid = (db.isInsideRoutingArea(conn, start)) && (db.isInsideRoutingArea(conn, dest));

        if (start.isEmpty() || dest.isEmpty()){
            routingGeom = start.getFactory().createLineString();
            LOG.warn("Empty points result in" + Routing_input_inputController.EPA_NAME+ ". " +
                    "Manual input coordinates seems to be invalid. An empty LineString will be returned");        }
        if (!valid){
            LOG.warn("One of the Input Point is to far away from routing network. Empty LineString will be returned");
            routingGeom = start.getFactory().createLineString();
        } else {
            String queryRoutingListResult = db.prepareRoutingQuery(start, dest);
            CoordinateList routingPointList = db.getResultFromDB(queryRoutingListResult, conn, true);
            routingGeom =  (LineString) db.createSPGeometryFromDB(routingPointList, true);

        }


        in.addField(Routing_input_inputController.ROUTING_TEXT, routingGeom.toString());
        out.collect(in);

    }

    @Override
    public void onDetach() {
        db.closeConnection(conn);
    }
}
