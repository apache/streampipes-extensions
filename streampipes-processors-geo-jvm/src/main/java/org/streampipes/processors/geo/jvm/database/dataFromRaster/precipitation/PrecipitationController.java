package org.streampipes.processors.geo.jvm.database.dataFromRaster.precipitation;

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


public class PrecipitationController extends StandaloneEventProcessingDeclarer<PrecipitationParameter> {


    public final static String WKT_TEXT = "wkt_text";
    public final static String EPSG_CODE = "epsg_code";
    public final static String YEAR_START = "year_start";
    public final static String YEAR_END = "year_end";
    public final static String TYPE = "type";
    public final static String PREC_result = "precipitation_value";


    public final static String EPA_Name = "Precipitation from Raster";


    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.database.dataFromRaster.precipitation", EPA_Name,
                        "calculates a precipitation value (volume in mm per year) into a single value from an internal database from a geometry." +
                        "Valid region Germany")
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
                .requiredIntegerParameter(
                        Labels.from(
                                YEAR_START,
                                "start year of specified time period",
                                "choose start year for calculation"),
                        1881,2017,1
                )
                .requiredIntegerParameter(
                        Labels.from(
                                YEAR_END,
                                "end year of specified time period",
                                "choose end year for calculation"),
                        1882, 2018, 1
                )

                .requiredSingleValueSelection(
                        Labels.from(
                                TYPE,
                                "or.org.streampipes.processors.geo.jvm.geofence.storing type",
                                "choose type of or.org.streampipes.processors.geo.jvm.geofence.storing" ),
                        Options.from(
                                SpDatabase.AggregationType.avg.name(),
                                SpDatabase.AggregationType.sum.name(),
                                SpDatabase.AggregationType.min.name(),
                                SpDatabase.AggregationType.max.name()
                        )
                )


                .outputStrategy(
                        OutputStrategies.append(
                                EpProperties.numberEp(
                                        Labels.from(
                                                "prec_result",
                                                "precipitation",
                                                "information from raster precipitation"),
                                        PREC_result,
                                        SO.Number)
                        )
                )
                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<PrecipitationParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor){

        String wkt_string = extractor.mappingPropertyValue(WKT_TEXT);
        String epsg_code = extractor.mappingPropertyValue(EPSG_CODE);
        Integer year_start = extractor.singleValueParameter(YEAR_START, Integer.class);
        Integer year_end = extractor.singleValueParameter(YEAR_END, Integer.class);
        String type = extractor.selectedSingleValue(TYPE, String.class);


        PrecipitationParameter params = new PrecipitationParameter(graph, wkt_string, epsg_code, year_start, year_end,  type);

        return new ConfiguredEventProcessor<>(params, Precipitation::new);
    }
}
