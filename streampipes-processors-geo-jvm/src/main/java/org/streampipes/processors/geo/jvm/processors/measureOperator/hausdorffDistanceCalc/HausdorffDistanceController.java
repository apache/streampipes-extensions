package org.streampipes.processors.geo.jvm.processors.measureOperator.hausdorffDistanceCalc;

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

public class HausdorffDistanceController extends StandaloneEventProcessingDeclarer<HausdorffDistanceParameter> {


    public final static String GEOMETRY_1 = "geometry1";
    public final static String GEOMETRY_2 = "geometry2";


    public final static String EPSG_GEOM_1 = "epsg_geom1";
    public final static String EPSG_GEOM_2 = "epsg_geom2";

    public final static String HAUSDORFF = "hausdorffDistance";
    public final static String UNIT = "unitOfDistance";

    public final static String DECIMAL_CHOICE = "decimalNumber";
    public final static String WHO_with_WHOM = "who_with_whom";


    public final static String EPA_NAME = "Hausdorff-Distance Calculation";

    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.measureOperator.hausdorffDistanceCalc",
                        EPA_NAME,
                        "Calculates the Hausdorff distance between two geometries.")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(StreamRequirementsBuilder
                        .create()
                        .requiredPropertyWithUnaryMapping(
                                EpRequirements.stringReq(),
                                Labels.from(
                                        GEOMETRY_1,
                                        "Geometry I",
                                        "First Geometry"),
                                PropertyScope.NONE)

                        .requiredPropertyWithUnaryMapping(
                                EpRequirements.numberReq(),
                                Labels.from(
                                        EPSG_GEOM_1,
                                        "EPSG Field I",
                                        "EPSG code for geometry I"),
                                PropertyScope.NONE)


                        .requiredPropertyWithUnaryMapping(
                                EpRequirements.stringReq(),
                                Labels.from(
                                        GEOMETRY_2,
                                        "Geometry I",
                                        "Second Geometry"),
                                PropertyScope.NONE)

                        .requiredPropertyWithUnaryMapping(
                                EpRequirements.numberReq(),
                                Labels.from(
                                        EPSG_GEOM_2,
                                        "EPSG Field II",
                                        "EPSG code for geometry II"),
                                PropertyScope.NONE)
                        .build()
                )

                .requiredIntegerParameter(
                        Labels.from(
                                DECIMAL_CHOICE,
                                "decimal positions",
                                "Choose the amount of decimal position"),
                        0, 10, 1)


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

                .requiredSingleValueSelection(
                        Labels.from(
                                WHO_with_WHOM,
                                "Choose First and Second Geometry",
                                "Choose Geometry. First geometry will be checked against second"),
                        Options.from(
                                "Geometry 1 vs. Geometry 2",
                                "Geometry 2 vs. Geometry 1")
                )


                .outputStrategy(OutputStrategies.append(
                        EpProperties.numberEp(
                                Labels.from(
                                        HAUSDORFF,
                                        "hausdorff distance",
                                        "hausdorff distance value"),
                                HAUSDORFF, SO.Number),
                        EpProperties.stringEp(
                                Labels.from(
                                        UNIT,
                                        "unitLength",
                                        "unit of the length"),
                                UNIT, SO.Text)
                        )
                )


                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<HausdorffDistanceParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {

       
        
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



        String epsg_geom1 = extractor.mappingPropertyValue(EPSG_GEOM_1);
        String geom1 = extractor.mappingPropertyValue(GEOMETRY_1);


        String epsg_geom2 = extractor.mappingPropertyValue(EPSG_GEOM_2);
        String geom2 = extractor.mappingPropertyValue(GEOMETRY_2);


        String whoVsWhom = extractor.selectedSingleValue(WHO_with_WHOM, String.class);


        boolean vsChecker = false;
        if (whoVsWhom.equals("Geometry 1 vs. Geometry 2")){
            vsChecker = true;
        } else if (whoVsWhom.equals("Geometry 2 vs. Geometry 1")){
            vsChecker = false;
            // not necessary but helpful to understand
        }


        HausdorffDistanceParameter params = new HausdorffDistanceParameter(graph, geom1, epsg_geom1, geom2, epsg_geom2, unit, decimalPos, vsChecker);

        return new ConfiguredEventProcessor<>(params, HausdorffDistance::new);
    }
}
