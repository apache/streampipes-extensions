
package org.streampipes.processors.geo.jvm.window.collect.multiline;



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

public class CollectMultiLineString implements EventProcessor<CollectMultiLineStringParameters>{


  private static Logger LOG;
  
  private Map<String, Event> state;

  private Map<String, LineString> multiline_window;

  public CollectMultiLineStringParameters params;
  private Map<String, Long> stateDelete;
  private int counter;
  private MultiLineString multiline;



    @Override
    public void onInvocation(CollectMultiLineStringParameters params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext eventProcessorRuntimeContext) {


        LOG = params.getGraph().getLogger(CollectMultiLineString.class);
        this.state = new HashMap<>();
        this.multiline_window = new HashMap<>();
        this.stateDelete = new HashMap<>();
        this.params = params;

        multiline = null;
    }

    @Override
  public void onEvent(Event in, SpOutputCollector out)  {


      String wkt = in.getFieldBySelector(params.getWkt()).getAsPrimitive().getAsString();
      Integer epsgCode = in.getFieldBySelector(params.getEpsg_Code()).getAsPrimitive().getAsInt();
      Integer uid = in.getFieldBySelector(params.getUid()).getAsPrimitive().getAsInt();
      Geometry geometry =  createSPGeom(wkt, epsgCode);




      if (geometry instanceof LineString) {

          //add into state window
          state.put(uid.toString(), in);
          // add point to HashMap
          multiline_window.put(uid.toString(), (LineString) geometry);

          // creates a collection from from HashMap
          List<LineString> collection = multiline_window.values().stream().collect(Collectors.toList());


          // creates the multiline from collection
          multiline = createSPMultiLineString(collection, false);


          in.addField(CollectMultiLineStringController.COLLECT, multiline.toText());
          in.addField(CollectMultiLineStringController.EPSG_CODE_COLLECTED_MULTILINE, multiline.getSRID());

          out.collect(in);


          //LOG.info(multiline.toText());
      } else {
          LOG.warn("LineString is required in " + CollectMultiLineStringController.EPA_NAME + " but input type is " + geometry.getGeometryType());
      }


      stateDelete.put(uid.toString(), System.currentTimeMillis() + 60000);

// set cleanup time
      counter++;
      if (counter > 100) {
          counter = 0;
          for (String key: stateDelete.keySet()) {
              if (stateDelete.get(key) < System.currentTimeMillis() ) {
                  state.remove(key);
                  multiline_window.remove(key);
                  stateDelete.remove(key);
              }
          }
      }






  }

  @Override
  public void onDetach() {

  }
}
