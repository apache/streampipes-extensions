
package org.streampipes.processors.geo.jvm.window.collect.multipoint;



import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPoint;
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


public class CollectMultiPoint implements EventProcessor<CollectMultiPointParameters>{


  private static Logger LOG;
  
  private Map<String, Event> state;

  private Map<String, Point> multipoint_window;

  public CollectMultiPointParameters params;
  private Map<String, Long> stateDelete;
  private int counter;
  private MultiPoint multipoint;



    @Override
    public void onInvocation(CollectMultiPointParameters params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext eventProcessorRuntimeContext) {


        LOG = params.getGraph().getLogger(CollectMultiPoint.class);
        this.state = new HashMap<>();
        this.multipoint_window = new HashMap<>();
        this.stateDelete = new HashMap<>();
        this.params = params;

        multipoint = null;
    }

    @Override
  public void onEvent(Event in, SpOutputCollector out)  {


      String wkt = in.getFieldBySelector(params.getWkt()).getAsPrimitive().getAsString();
      Integer epsgCode = in.getFieldBySelector(params.getEpsg_Code()).getAsPrimitive().getAsInt();
      Integer uid = in.getFieldBySelector(params.getUid()).getAsPrimitive().getAsInt();
      Geometry geometry =  createSPGeom(wkt, epsgCode);


      if (geometry instanceof Point) {


          state.put(uid.toString(), in);

          // add point to HashMap
          multipoint_window.put(uid.toString(), (Point) geometry);

          // creates a collection from from HashMap
          List<Point> collection = multipoint_window.values().stream().collect(Collectors.toList());


          // creates the multipoint from collection
          multipoint = createSPMultiPoint(collection);


          in.addField(CollectMultiPointController.COLLECT, multipoint.toText());
          out.collect(in);



          //LOG.info(multipoint.toText());
      } else {
          LOG.warn("Point is required in " + CollectMultiPointController.EPA_NAME + " but input type is " + geometry.getGeometryType());
      }


      stateDelete.put(uid.toString(), System.currentTimeMillis() + 60000);

// set cleanup time
      counter++;
      if (counter > 100) {
          counter = 0;
          for (String key: stateDelete.keySet()) {
              if (stateDelete.get(key) < System.currentTimeMillis() ) {
                  state.remove(key);
                  multipoint_window.remove(key);
                  stateDelete.remove(key);
              }
          }
      }






  }

  @Override
  public void onDetach() {

  }
}
