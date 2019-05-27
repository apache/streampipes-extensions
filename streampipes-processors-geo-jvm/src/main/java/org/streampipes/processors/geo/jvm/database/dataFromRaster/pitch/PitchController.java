package org.streampipes.processors.geo.jvm.database.dataFromRaster.pitch;


import org.streampipes.processors.geo.jvm.database.helper.SpDatabase;
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
import org.streampipes.sdk.utils.Assets;

public class PitchController extends StandaloneEventProcessingDeclarer<PitchParameter> {


    public final static String WKT_TEXT = "wkt_text";
    public final static String EPSG_CODE = "epsg_code";
    public final static String ELEVATION = "pitch_value";
    public final static String TYPE = "type";

    public final static String EPA_NAME = "Pitch Calculation from SRTM";


    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.database.dataFromRaster.pitch", EPA_NAME,
                        "Calculates the pitch of a LineString or MultiLineString from an internal SRTM database. +" +
                                " Result is a standardised value relative to 100m distance")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(StreamRequirementsBuilder
                        .create()
                        .requiredPropertyWithUnaryMapping(EpRequirements.stringReq(),
                                Labels.from(WKT_TEXT, "WKT String", "Field with WKT String"), PropertyScope.NONE)
                        .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                                Labels.from(EPSG_CODE, "EPSG Field", "EPSG Code for SRID"),
                                PropertyScope.NONE)
                        .build()
                )
                .requiredSingleValueSelection(
                        Labels.from(
                                TYPE,
                                "Result type",
                                "Choose result type. Alpha in degree or percent" ),
                        Options.from(
                                SpDatabase.calcPitchResultOption.PERCENT.name(),
                                SpDatabase.calcPitchResultOption.ALPHA.name()
                        )
                )


                .outputStrategy(
                        OutputStrategies.append(
                                EpProperties.numberEp(
                                        Labels.from(
                                                "pitch_result",
                                                "pitch_value",
                                                "information from raster pitch"),
                                        ELEVATION,
                                        SO.Number)
                        )
                )
                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<PitchParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor){

        String wkt_string = extractor.mappingPropertyValue(WKT_TEXT);
        String epsg_code = extractor.mappingPropertyValue(EPSG_CODE);
        String read_type = extractor.selectedSingleValue(TYPE, String.class);


        //default percent
        Integer type = 1;
        if (read_type.equals(SpDatabase.calcPitchResultOption.ALPHA.name())) {
            type = (SpDatabase.calcPitchResultOption.ALPHA.getNumber());
        }

        PitchParameter params = new PitchParameter(graph, wkt_string, epsg_code, type);

        return new ConfiguredEventProcessor<>(params, Pitch::new);
    }
}
