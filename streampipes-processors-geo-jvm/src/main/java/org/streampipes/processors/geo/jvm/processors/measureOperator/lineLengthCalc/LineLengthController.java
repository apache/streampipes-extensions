package org.streampipes.processors.geo.jvm.processors.measureOperator.lineLengthCalc;

import org.streampipes.processors.geo.jvm.helpers.LengthSpOperator;
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

public class LineLengthController extends StandaloneEventProcessingDeclarer<LineLengthParameter> {


    public final static String WKT_TEXT = "wkt_text";
    public final static String EPSG_CODE = "epsg_code";
    protected final static String LENGTH = "length";
    protected final static String UNIT = "unitOfLength";

    protected final static String DECIMAL_CHOICE = "decimalNumber";

    public final static String EPA_NAME = "Line Length Calculation";



    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.measureOperator.lineLengthCalc",
                        EPA_NAME,
                        "Calculates the length of a LineString4. If input geometry  is a MultiLineString, the sum " +
                                "of all lines will be calculated")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(StreamRequirementsBuilder
                        .create()
                        .requiredPropertyWithUnaryMapping(
                                EpRequirements.stringReq(),
                                Labels.from(
                                        WKT_TEXT,
                                        "WKT_String",
                                        "Field with WKT String"),
                                PropertyScope.NONE)
                        .requiredPropertyWithUnaryMapping(
                                EpRequirements.numberReq(),
                                Labels.from(
                                        EPSG_CODE,
                                        "EPSG Field",
                                        "EPSG Code for SRID"),
                                PropertyScope.NONE)
                        .build()
                )

                .requiredIntegerParameter(
                        Labels.from(
                                DECIMAL_CHOICE,
                                "decimal positions",
                                "Choose the amount of decimal position"),
                        0,10,1)


                .requiredSingleValueSelection(
                        Labels.from(
                                UNIT,
                                "Goal Unit",
                                "Select the unit of your choice"),
                        Options.from(
                                LengthSpOperator.ValidLengthUnits.meter.name(),
                                LengthSpOperator.ValidLengthUnits.km.name(),
                                LengthSpOperator.ValidLengthUnits.mile.name(),
                                LengthSpOperator.ValidLengthUnits.foot.name()
                        )
                )


                .outputStrategy(OutputStrategies.append(
                        EpProperties.numberEp(
                                Labels.from(
                                        LENGTH,
                                        "line length",
                                        "line length value"),
                                LENGTH, SO.Number),
                        EpProperties.stringEp(
                                Labels.from(
                                        UNIT,
                                        "line length value",
                                        "unit of the line length"),
                                UNIT, SO.Text)
                        )
                )


                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<LineLengthParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {

        String wkt_string = extractor.mappingPropertyValue(WKT_TEXT);
        String epsg_code = extractor.mappingPropertyValue(EPSG_CODE);

        Integer decimalPos = extractor.singleValueParameter(DECIMAL_CHOICE, Integer.class);
        String chosenUnit = extractor.selectedSingleValue(UNIT, String.class);

        // convert enum to integer values
        int unit = 1;
        if (chosenUnit.equals(LengthSpOperator.ValidLengthUnits.km.name())){
            unit = LengthSpOperator.ValidLengthUnits.km.getNumber();
        } else if (chosenUnit.equals(LengthSpOperator.ValidLengthUnits.mile.name())){
            unit = LengthSpOperator.ValidLengthUnits.mile.getNumber();
        } else if (chosenUnit.equals(LengthSpOperator.ValidLengthUnits.foot.name())){
            unit = LengthSpOperator.ValidLengthUnits.foot.getNumber();
        }


        LineLengthParameter params = new LineLengthParameter(graph, wkt_string, epsg_code, unit, decimalPos);

        return new ConfiguredEventProcessor<>(params, LineLength::new);
    }
}
