
package org.streampipes.processors.geo.jvm.window.count;


import org.locationtech.jts.geom.Geometry;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;
import org.streampipes.logging.api.Logger;


import java.util.HashMap;
import java.util.Map;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;

public class CountGeometry implements EventProcessor<CountGeometryParameters> {


  private static Logger LOG;
  private Map<String, Event> state;
  private Map<String, Long> stateDelete;
  private int counter;
  public CountGeometryParameters params;
  private int max;


  @Override
  public void onInvocation(CountGeometryParameters countGeometryParameters, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

      LOG = params.getGraph().getLogger(CountGeometry.class);
      this.state = new HashMap<>();
      this.stateDelete = new HashMap<>();
      this.params = countGeometryParameters;
      max = 0;

  }

  @Override
  public void onEvent(Event in, SpOutputCollector out)  {

      String wkt = in.getFieldBySelector(params.getWkt_string()).getAsPrimitive().getAsString();
      Integer epsgCode = in.getFieldBySelector(params.getEpsg_Code()).getAsPrimitive().getAsInt();

      Integer uid = in.getFieldBySelector(params.getUid()).getAsPrimitive().getAsInt();


      Geometry geometry =  createSPGeom(wkt, epsgCode);


      state.put(uid.toString(), in);

      stateDelete.put(uid.toString(), System.currentTimeMillis() + 300000);

// set cleanup time
      counter++;
      if (counter > 100) {
          counter = 0;
          for (String key: stateDelete.keySet()) {
              if (stateDelete.get(key) < System.currentTimeMillis() ) {
                  state.remove(key);
                  stateDelete.remove(key);
              }
          }
      }


      // calculates the maximum if all stream inputs
      //max = 0;
       if (state.size() > max){
           max = state.size();
       }


      in.addField(CountGeometryController.SIZE, state.size());
      in.addField(CountGeometryController.MAX, max);

      out.collect(in);
  }

  @Override
  public void onDetach() {

  }
}
