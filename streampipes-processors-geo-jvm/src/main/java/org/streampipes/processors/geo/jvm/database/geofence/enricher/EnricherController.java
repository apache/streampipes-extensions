package org.streampipes.processors.geo.jvm.database.geofence.enricher;


import org.streampipes.commons.exceptions.SpRuntimeException;
import org.streampipes.container.api.ResolvesContainerProvidedOptions;
import org.streampipes.container.api.ResolvesContainerProvidedOutputStrategy;
import org.streampipes.model.DataProcessorType;
import org.streampipes.model.graph.DataProcessorDescription;
import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.model.runtime.RuntimeOptions;
import org.streampipes.model.schema.EventProperty;
import org.streampipes.model.schema.EventSchema;
import org.streampipes.processors.geo.jvm.config.GeoJvmConfig;
import org.streampipes.processors.geo.jvm.database.helper.SpDatabase;
import org.streampipes.sdk.builder.ProcessingElementBuilder;
import org.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.streampipes.sdk.helpers.*;
import org.streampipes.vocabulary.SO;
import org.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;
import org.streampipes.sdk.utils.Assets;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

//public class EnricherController extends StandaloneEventProcessingDeclarer<EnricherParameter>  implements ResolvesContainerProvidedOptions<DataProcessorInvocation, ProcessingElementParameterExtractor> {

public class EnricherController extends
            StandaloneEventProcessingDeclarer<EnricherParameter> implements
            //todo change back to custom
            ResolvesContainerProvidedOutputStrategy<DataProcessorInvocation,ProcessingElementParameterExtractor>,
            ResolvesContainerProvidedOptions {


    public final static String EPA_NAME = "Enricher from Geofence";
    public final static String GEOFENCE_NAME = "geofence_name";
    public final static String DB_TABLE = "geofence_table";


    public final static String GEOFENCE_WKT = "geofence_wkt";
    public final static String GEOFENCE_EPSG = "geofence_epsg";

    public final static String GEOFENCE_AREA = "geofence_area";
    public final static String GEOFENCE_AREA_UNIT = "geofence_areaUnit";
    public final static String GEOFENCE_M_VALUE = "geofence_M_Value";



    @Override
        public DataProcessorDescription declareModel() {
            return ProcessingElementBuilder
                    .create("org.streampipes.processors.geo.jvm.database.geofence.enricher",
                            EPA_NAME,
                            "Enrich stream with properties from geofence")
                    .category(DataProcessorType.GEO)
                    .withAssets(Assets.DOCUMENTATION, Assets.ICON)

                    .requiredStream(StreamRequirementsBuilder
                            .create()
                            .requiredProperty(EpRequirements.anyProperty())
                            .build()
                    )
                    .requiredSingleValueSelectionFromContainer(
                            Labels.from(
                                    DB_TABLE,
                                    "Table names from DB",
                                    "Chose table name to enrich from"
                            )
                    )


                    //todo change back to custom
                    .outputStrategy(OutputStrategies.customTransformation())

//                    .outputStrategy(OutputStrategies.append(
//                            EpProperties.stringEp(Labels.from(GEOFENCE_NAME, "geofence_name", "name of the geofence"), "geofence_name", SO.Text),
//                            EpProperties.stringEp(Labels.from(GEOFENCE_WKT, "geofence_wkt", "WKT String of geofence"), "geofence_wkt", SO.Text),
//                            EpProperties.integerEp(Labels.from(GEOFENCE_EPSG, "geofence_epsg", "EPSG Code of geofence"), "geofence_epsg", SO.Number),
//                            EpProperties.doubleEp(Labels.from(GEOFENCE_AREA, "geofence_area", "area of geofence"), "geofence_area", SO.Number),
//                            EpProperties.stringEp(Labels.from(GEOFENCE_AREA_UNIT, "geofence_areaUnit", "unit of geofence area"), "geofence_area_unit", SO.Text),
//                            EpProperties.doubleEp(Labels.from(GEOFENCE_M_VALUE, "geofence_M_Value", "M value of geofence"), "geofence_M", SO.Number)
//                            )
//                    )

                    .supportedFormats(SupportedFormats.jsonFormat())
                    .supportedProtocols(SupportedProtocols.kafka())
                    .build();
        }


        @Override
        public ConfiguredEventProcessor<EnricherParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {

            String geofence_name = extractor.selectedSingleValueFromRemote(DB_TABLE, String.class);
            EnricherParameter params = new EnricherParameter(graph, geofence_name);
            return new ConfiguredEventProcessor<>(params, Enricher::new);
        }

        @Override
        public List<RuntimeOptions> resolveOptions(String s, EventProperty eventProperty) {
            List<RuntimeOptions> results = new ArrayList<>();

            List<String> geofence_names;
            Connection conn;
            SpDatabase db;

            String host = GeoJvmConfig.INSTANCE.getPostgresHost();
            Integer port = Integer.valueOf(GeoJvmConfig.INSTANCE.getPostgresPort());
            String dbName = GeoJvmConfig.INSTANCE.getPostgresDatabase();
            String user = GeoJvmConfig.INSTANCE.getPostgresUser();
            String password = GeoJvmConfig.INSTANCE.getPostgresPassword();

            db = new SpDatabase(host, port, dbName, user, password);
            conn = db.connect();
            String query = db.prepareQueryGeofence();
            geofence_names = db.geofenceReadOut(query, conn);


            for (String name : geofence_names) {
                results.add(new RuntimeOptions(name, ""));
            }
            return results;
        }



        //todo change back to custom output strategie later on? instead of using append
        @Override
        public EventSchema resolveOutputStrategy(DataProcessorInvocation processingElement, ProcessingElementParameterExtractor parameterExtractor) throws SpRuntimeException {
            return new EventSchema(Arrays.asList(
                    EpProperties.stringEp(Labels.from(GEOFENCE_NAME, "geofence_name", "name of the geofence"), "geofence_name", SO.Text),
                    EpProperties.stringEp(Labels.from(GEOFENCE_WKT, "geofence_wkt", "WKT String of geofence"), "geofence_wkt", SO.Text),
                    EpProperties.integerEp(Labels.from(GEOFENCE_EPSG, "geofence_epsg", "EPSG Code of geofence"), "geofence_epsg", SO.Number),
                    EpProperties.doubleEp(Labels.from(GEOFENCE_AREA, "geofence_area", "area of geofence"), "geofence_area", SO.Number),
                    EpProperties.stringEp(Labels.from(GEOFENCE_AREA_UNIT, "geofence_areaUnit", "unit of geofence area"), "geofence_area_unit", SO.Text),
                    EpProperties.doubleEp(Labels.from(GEOFENCE_M_VALUE, "geofence_M_Value", "M value of geofence"), "geofence_M", SO.Number)
                    ));
        }


}

