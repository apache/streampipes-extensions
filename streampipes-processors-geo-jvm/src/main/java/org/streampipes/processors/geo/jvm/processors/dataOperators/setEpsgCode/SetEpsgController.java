package org.streampipes.processors.geo.jvm.processors.dataOperators.setEpsgCode;



import org.streampipes.processors.geo.jvm.processors.dataOperators.validator.GeoValidator;
import org.streampipes.model.DataProcessorType;
import org.streampipes.model.graph.DataProcessorDescription;
import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.sdk.builder.ProcessingElementBuilder;
import org.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.streampipes.sdk.helpers.*;
import org.streampipes.vocabulary.SO;
import org.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;
import org.streampipes.sdk.utils.Assets;

public class SetEpsgController extends StandaloneEventProcessingDeclarer<SetEpsgParameter> {

    public final static String EPSG_CODE = "epsg_code";
    public final static String EPA_NAME = "EPSG";

    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.dataOperators.setEpsgCode",
                        EPA_NAME,
                        "Adds an EPSG Code")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream
                        (StreamRequirementsBuilder
                                .create()
                                .build())
                .requiredIntegerParameter(
                        Labels.from(
                                EPSG_CODE,
                                "Set EPSG Code number",
                                "Sets an EPSG Code. Default ist WGS84 with 4326"),
                        4326)
                .outputStrategy(
                        OutputStrategies.append(
                                EpProperties.numberEp(
                                        Labels.from(
                                                "EPSG Code",
                                                "EPSG Code",
                                                "EPSG Code for SRID"),
                                        EPSG_CODE, SO.Number)))
                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }

    @Override
    public ConfiguredEventProcessor<SetEpsgParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {

        Integer epsg_value = extractor.singleValueParameter(EPSG_CODE, Integer.class);
        SetEpsgParameter params = new SetEpsgParameter(graph, epsg_value);

        return new ConfiguredEventProcessor<>(params, SetEPSG::new);

    }
}
