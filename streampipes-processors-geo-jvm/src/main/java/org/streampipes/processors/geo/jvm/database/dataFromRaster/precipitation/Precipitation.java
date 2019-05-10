package org.streampipes.processors.geo.jvm.database.dataFromRaster.precipitation;


import org.locationtech.jts.geom.Geometry;
import org.streampipes.processors.geo.jvm.database.helper.SpDatabase;
import org.streampipes.logging.api.Logger;
import org.streampipes.model.runtime.Event;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;

import java.sql.Connection;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;

public class Precipitation implements EventProcessor<PrecipitationParameter> {

    private static Logger LOG;
    private PrecipitationParameter params;
    private Integer year_start;
    private Integer year_end;
    private String type;
    private Connection conn;
    private SpDatabase db;


    @Override
    public void onInvocation(PrecipitationParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(Precipitation.class);
        this.params = params;
        this.year_start = params.getYear_start();
        this.year_end = params.getYear_end();
        this.type = params.getType();


        String host = Config.INSTANCE.getPostgresHost();
        Integer port = Integer.valueOf(Config.INSTANCE.getPostgresPort());
        String dbName = Config.INSTANCE.getPostgresDatabase();
        String user = Config.INSTANCE.getPostgresUser();
        String password = Config.INSTANCE.getPostgresPassword();


        this.db = new SpDatabase(host, port, dbName, user, password);
        this.conn = db.connect();
    }

    @Override
    public void onEvent(Event in, SpOutputCollector out) {


        String wkt = in.getFieldBySelector(params.getWkt_string()).getAsPrimitive().getAsString();
        Integer epsgCode = in.getFieldBySelector(params.getEpsg_code()).getAsPrimitive().getAsInt();
        Geometry geometry = createSPGeom(wkt, epsgCode);


        double result = db.getTotalResult(year_start, year_end, conn, geometry, type);

        //todo check what kind of value occurs, if region is outside of germany. if it in null

        in.addField(PrecipitationController.PREC_result, result);
        out.collect(in);


        //LOG.info(in.toString());


    }

    @Override
    public void onDetach() {
        db.closeConnection(conn);

    }
}
