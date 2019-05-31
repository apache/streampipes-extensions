
package org.streampipes.processors.geo.jvm.window.voronoi;



import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;
import org.streampipes.logging.api.Logger;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;
import static org.streampipes.processors.geo.jvm.helpers.Helper.*;

public class VoronoiOperator implements EventProcessor<VoronoiOperatorParameters>{


  private static Logger LOG;
  
  private Map<String, Event> state;

  private Map<String, Point> voronoi;

  public VoronoiOperatorParameters params;
  private Map<String, Long> stateDelete;
  private int counter;
  private MultiPolygon voronoiPoly;



    @Override
    public void onInvocation(VoronoiOperatorParameters params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext eventProcessorRuntimeContext) {


        LOG = params.getGraph().getLogger(VoronoiOperator.class);
        this.state = new HashMap<>();
        this.voronoi = new HashMap<>();
        this.stateDelete = new HashMap<>();
        this.params = params;

        voronoiPoly = null;
    }

    @Override
  public void onEvent(Event in, SpOutputCollector out)  {


      String wkt = in.getFieldBySelector(params.getWkt()).getAsPrimitive().getAsString();
      Integer epsgCode = in.getFieldBySelector(params.getEpsg_Code()).getAsPrimitive().getAsInt();
      Integer uid = in.getFieldBySelector(params.getUid()).getAsPrimitive().getAsInt();
      Geometry geometry =  createSPGeom(wkt, epsgCode);

      state.put(uid.toString(), in);


      if (geometry instanceof Point) {
          // add point to HashMap
          voronoi.put(uid.toString(), (Point) geometry);

          // creates from HashMap a collection
          List<Point> coll_points = voronoi.values().stream().collect(Collectors.toList());


          //voronoi with 1 points lead to an empty result. so next step should only be occurs if size is higher than 1
          if (coll_points.size() > 1) {

              // creates a multipoint
              MultiPoint points = createSPMultiPoint(coll_points);

              voronoiPoly = createVoronoi(points);



              // if voronoi is not empty then put into stream
              if (!(voronoiPoly.isEmpty())) {

                  in.addField(VoronoiOperatorController.VORONOI, voronoiPoly.toText());
                  in.addField(VoronoiOperatorController.EPSG_CODE_VORONOI, voronoiPoly.getSRID());

                  out.collect(in);
              }

          }

      }


      stateDelete.put(uid.toString(), System.currentTimeMillis() + 60000);

// set cleanup time
      counter++;
      if (counter > 100) {
          counter = 0;
          for (String key: stateDelete.keySet()) {
              if (stateDelete.get(key) < System.currentTimeMillis() ) {
                  state.remove(key);
                  voronoi.remove(key);
                  stateDelete.remove(key);
              }
          }
      }






  }

  @Override
  public void onDetach() {

  }
}
