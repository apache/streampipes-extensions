package org.streampipes.processors.geo.jvm.database.networkAnalyst.routing.oneManual;



import org.locationtech.jts.geom.Point;
import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;


public class RoutingSingleInputParameter extends EventProcessorBindingParams {


    private Point point;
    private String chooser;
    private String epsg;
    private String wkt;


    public RoutingSingleInputParameter(DataProcessorInvocation graph, Double lat, Double lng, String chooser, String wkt, String epsg) {
        super(graph);
        this.point = (Point) createSPGeom(lng, lat, 4326);
        this.chooser = chooser;
        this.wkt = wkt;
        this.epsg = epsg;
    }

    public Point getPoint() {
        return point;
    }

    public String getEpsg() {
        return epsg;
    }

    public String getWkt() {
        return wkt;
    }

    public String getChooser() {
        return chooser;




    }
}
