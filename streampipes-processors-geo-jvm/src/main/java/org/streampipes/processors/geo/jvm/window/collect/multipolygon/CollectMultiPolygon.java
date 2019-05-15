
package org.streampipes.processors.geo.jvm.window.collect.multipolygon;



import org.locationtech.jts.geom.*;
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

public class CollectMultiPolygon implements EventProcessor<CollectMultiPolygonParameters>{


  private static Logger LOG;
  
  private Map<String, Event> state;

  private Map<String, Polygon> multipolygon_window;

  public CollectMultiPolygonParameters params;
  private Map<String, Long> stateDelete;
  private int counter;
  private MultiPolygon multipolygon;

  private boolean dissolve;



    @Override
    public void onInvocation(CollectMultiPolygonParameters params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext eventProcessorRuntimeContext) {


        LOG = params.getGraph().getLogger(CollectMultiPolygon.class);
        this.state = new HashMap<>();
        this.multipolygon_window = new HashMap<>();
        this.stateDelete = new HashMap<>();
        this.params = params;
        this.dissolve = params.isDissolve();

        multipolygon = null;
    }

    @Override
  public void onEvent(Event in, SpOutputCollector out)  {


      String wkt = in.getFieldBySelector(params.getWkt()).getAsPrimitive().getAsString();
      Integer epsgCode = in.getFieldBySelector(params.getEpsg_Code()).getAsPrimitive().getAsInt();
      Integer uid = in.getFieldBySelector(params.getUid()).getAsPrimitive().getAsInt();
      Geometry geometry =  createSPGeom(wkt, epsgCode);



      if (geometry instanceof Polygon) {

          state.put(uid.toString(), in);

          // add point to HashMap
          multipolygon_window.put(uid.toString(), (Polygon) geometry);

          // creates a collection from from HashMap
          List<Polygon> collection = multipolygon_window.values().stream().collect(Collectors.toList());


          // creates the multipolygon from collection
          multipolygon = createSPMultiPolygon(collection, dissolve);


          in.addField(CollectMultiPolygonController.COLLECT, multipolygon.toText());
          out.collect(in);


      } else {

          LOG.warn("Polygon is required in " + CollectMultiPolygonController.EPA_NAME + " but input type is " + geometry.getGeometryType());

      }


      stateDelete.put(uid.toString(), System.currentTimeMillis() + 60000);

// set cleanup time
      counter++;
      if (counter > 100) {
          counter = 0;
          for (String key: stateDelete.keySet()) {
              if (stateDelete.get(key) < System.currentTimeMillis() ) {
                  state.remove(key);
                  multipolygon_window.remove(key);
                  stateDelete.remove(key);
              }
          }
      }






  }

  @Override
  public void onDetach() {

  }
}
