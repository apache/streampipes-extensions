package org.streampipes.processors.geo.jvm.processors.derivedGeometry.envelope;

import org.streampipes.model.DataProcessorType;
import org.streampipes.model.graph.DataProcessorDescription;
import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.model.schema.PropertyScope;
import org.streampipes.sdk.builder.ProcessingElementBuilder;
import org.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.streampipes.sdk.helpers.*;
import org.streampipes.vocabulary.SO;
import org.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;


public class EnvelopeController extends StandaloneEventProcessingDeclarer<EnvelopeParameter> {


    public final static String WKT_TEXT = "wkt_text";
    public final static String EPSG_CODE = "epsg_code";
    public final static String ENVELOPE = "envelope_text";
    public final static String EPA_NAME = "Envelope";


    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.derivedGeometry.envelope",
                        EPA_NAME,
                        "Creates an envelope around the geometry")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(StreamRequirementsBuilder
                        .create()
                        .requiredPropertyWithUnaryMapping(EpRequirements.stringReq(),
                                Labels.from(WKT_TEXT, "WKT_String", "Field with WKT String"), PropertyScope.NONE)
                        .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                                Labels.from(EPSG_CODE, "EPSG Field", "EPSG Code for SRID"),
                                PropertyScope.NONE)
                        .build()
                )
                .outputStrategy(
                        OutputStrategies.append(
                                EpProperties.stringEp(
                                        Labels.from(
                                                "envelope_wkt",
                                                "envelope_wkt",
                                                "envelope wkt"),
                                        ENVELOPE,
                                        SO.Text)
                        )
                )

                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<EnvelopeParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {

        String wkt_string = extractor.mappingPropertyValue(WKT_TEXT);
        String epsg_code = extractor.mappingPropertyValue(EPSG_CODE);

        EnvelopeParameter params = new EnvelopeParameter(graph, wkt_string, epsg_code);

        return new ConfiguredEventProcessor<>(params, Envelope::new);



    }
}
