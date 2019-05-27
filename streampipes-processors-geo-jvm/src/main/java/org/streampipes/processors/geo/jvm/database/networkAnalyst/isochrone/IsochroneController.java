package org.streampipes.processors.geo.jvm.database.networkAnalyst.isochrone;


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

public class IsochroneController extends StandaloneEventProcessingDeclarer<IsochroneParameter> {


    public final static String WKT_TEXT = "wkt_text";
    public final static String EPSG_CODE = "epsg_code";
    public final static String UNITS = "units";
    public final static String MEASURE_VALUE = "value";
    public final static String ISOCHRONE = "isochrone_wkt";


    public final static String EPA_NAME = "Isochrone";


    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.database.networkAnalyst.isochrone", EPA_NAME,
                        "calculates an isochrone from an internal routing database." +
                                "Input geometry has to be a point")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(StreamRequirementsBuilder
                        .create()
                        .requiredPropertyWithUnaryMapping(EpRequirements.stringReq(),
                                Labels.from(WKT_TEXT, "Point WKT String", "Field with WKT String"), PropertyScope.NONE)
                        .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                                Labels.from(EPSG_CODE, "EPSG Field", "EPSG Code for SRID"),
                                PropertyScope.NONE)
                        .build()
                )

                .requiredSingleValueSelection(
                        Labels.from(
                                UNITS,
                                "unit measurement",
                                "Choose unit for  isochrone calculation" ),
                        Options.from(
                                "seconds",
                                "meters"
                        )
                )

                .requiredIntegerParameter(
                        Labels.from(
                                MEASURE_VALUE,
                                "measure value",
                                "Set the value for calculation depending on the chosen unit." +
                                        "If you choose seconds, minimum will be set to 300 seconds (5min) automatically " +
                                        "If you choose meter, minimum will be set to 1000m automatically")
                )



                .outputStrategy(
                        OutputStrategies.append(
                                EpProperties.stringEp(
                                        Labels.from(
                                                "isochrone_wkt",
                                                "isochrone",
                                                "isochrone wkt"),
                                        ISOCHRONE,
                                        SO.Text)
                        )
                )
                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<IsochroneParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {


        String wkt_string = extractor.mappingPropertyValue(WKT_TEXT);
        String epsg_code = extractor.mappingPropertyValue(EPSG_CODE);

        Integer measure = extractor.singleValueParameter(MEASURE_VALUE, Integer.class);
        String unit = extractor.selectedSingleValue(UNITS, String.class);




        Boolean usesUnitSecond = false;

        if(unit.equals("seconds")){
            usesUnitSecond = true;

        }


        // set minimum
        if (unit.equals("seconds")){
            if (measure < 300) {
                measure = 300;
            }
        } else {
            if (measure < 1000){
                measure = 1000;
            }
        }


        IsochroneParameter params = new IsochroneParameter(graph, wkt_string, epsg_code, measure, usesUnitSecond);

        return new ConfiguredEventProcessor<>(params, Isochrone::new);
    }
}
