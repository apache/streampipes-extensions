package org.streampipes.processors.geo.jvm.processors.measureOperator.distanceCalc.planar;


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

public class DistancePlanarController extends StandaloneEventProcessingDeclarer<DistancePlanarParameter> {


    public final static String GEOMETRY_1 = "geometry1";
    public final static String GEOMETRY_2 = "geometry2";


    public final static String EPSG_GEOM_1 = "epsg_geom1";
    public final static String EPSG_GEOM_2 = "epsg_geom2";

    public final static String PLANAR_DISTANCE = "planarDistance";
    public final static String UNIT = "unitOfLength";

    public final static String DECIMAL_CHOICE = "decimalNumber";
    
    public final static String EPA_NAME = "Planar Distance Calculator";


    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.measureOperator.distanceCalc.planar",
                        EPA_NAME,
                        "Calculates the euclidean distance in a planar CRS. " +
                                "This calculation should be used if the geometries are not far away from each other. ")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(StreamRequirementsBuilder
                        .create()
                        .requiredPropertyWithUnaryMapping(
                                EpRequirements.stringReq(),
                                Labels.from(
                                        GEOMETRY_1,
                                        "Geometry I",
                                        "First Geometry to check with"),
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
                                                "Geometry II",
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


                .outputStrategy(OutputStrategies.append(
                        EpProperties.numberEp(
                                Labels.from(
                                        PLANAR_DISTANCE,
                                        "planar distance",
                                        "planar distance value"),
                                PLANAR_DISTANCE, SO.Number),
                        EpProperties.stringEp(
                                Labels.from(
                                        UNIT,
                                        "planar distance unit",
                                        "unit of planar distance"),
                                UNIT, SO.Text)
                        )
                )
                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<DistancePlanarParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {
        

        String epsg_geom1 = extractor.mappingPropertyValue(EPSG_GEOM_1);
        String geom1 = extractor.mappingPropertyValue(GEOMETRY_1);


        String epsg_geom2 = extractor.mappingPropertyValue(EPSG_GEOM_2);
        String geom2 = extractor.mappingPropertyValue(GEOMETRY_2);


        Integer decimalPosition = extractor.singleValueParameter(DECIMAL_CHOICE, Integer.class);
        
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



        DistancePlanarParameter params = new DistancePlanarParameter(graph, geom1, epsg_geom1, geom2, epsg_geom2, unit, decimalPosition);

        return new ConfiguredEventProcessor<>(params, DistancePlanar::new);
    }
}

