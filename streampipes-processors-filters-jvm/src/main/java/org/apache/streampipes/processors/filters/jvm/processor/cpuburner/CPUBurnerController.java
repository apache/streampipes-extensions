package org.apache.streampipes.processors.filters.jvm.processor.cpuburner;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.DataProcessorType;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.processors.filters.jvm.processor.cpuburner.util.CPUBurner;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.helpers.Labels;
import org.apache.streampipes.sdk.helpers.Locales;
import org.apache.streampipes.sdk.helpers.OutputStrategies;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;
import org.apache.streampipes.wrapper.standalone.ProcessorParams;
import org.apache.streampipes.wrapper.standalone.StreamPipesReconfigurableProcessor;

public class CPUBurnerController extends StreamPipesReconfigurableProcessor {

    private static double load;
    private CPUBurner cpuBurner;

    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder.create("org.apache.streampipes.processors.filters.jvm.cpuburner")
                .category(DataProcessorType.FILTER)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .withLocales(Locales.EN)
                .requiredStream(StreamRequirementsBuilder.any())
                .requiredIntegerParameter(Labels.withId("ramp-up-duration"))
                .requiredIntegerParameter(Labels.withId("ramp-up-delay"))
                .requiredReconfigurableFloatParameter(Labels.withId("load"))
                .outputStrategy(OutputStrategies.keep())
                .build();
    }

    @Override
    public void onInvocation(ProcessorParams parameters, SpOutputCollector spOutputCollector,
                             EventProcessorRuntimeContext runtimeContext) throws SpRuntimeException {
        load = parameters.extractor().singleValueParameter("load", Double.class);
        long rampUpDuration = parameters.extractor().singleValueParameter("ramp-up-duration", Integer.class);
        long rampUpDelay = parameters.extractor().singleValueParameter("ramp-up-delay", Integer.class);
        cpuBurner = new CPUBurner(Runtime.getRuntime().availableProcessors(), load, rampUpDuration, rampUpDelay);
        if(load>0) cpuBurner.startBurner();
    }

    @Override
    public void onEvent(Event event, SpOutputCollector collector) throws SpRuntimeException {
        // do nothing with event
        collector.collect(event);
    }

    @Override
    public void onDetach() throws SpRuntimeException {
        cpuBurner.stopBurners();
    }

    @Override
    public void onReconfigurationEvent(Event event) throws SpRuntimeException {
        load = event.getFieldByRuntimeName("load").getAsPrimitive().getAsDouble();
        if (cpuBurner.isRunning()){
            if (load == 0)
                cpuBurner.stopBurners();
            else
                cpuBurner.updateLoad(load);
        }else {
            cpuBurner.startBurner(load);
        }
    }
}
