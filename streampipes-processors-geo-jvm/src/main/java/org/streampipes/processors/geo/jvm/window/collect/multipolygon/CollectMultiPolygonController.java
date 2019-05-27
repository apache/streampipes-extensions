package org.streampipes.processors.geo.jvm.window.collect.multipolygon;

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

public class CollectMultiPolygonController extends StandaloneEventProcessingDeclarer<CollectMultiPolygonParameters> {

	protected final static String WKT_TEXT = "wkt_text";
	protected final static String EPSG_CODE = "epsg_code";
	protected final static String UID = "uid";
	protected final static String COLLECT = "collect_multipolygon_wkt";
	protected final static String DISSOLVE = "dissolve";

	protected final static String EPA_NAME = "Collect as MultiPolygon";


	@Override
	public DataProcessorDescription declareModel() {
		return ProcessingElementBuilder.create(
				"org.streampipes.processors.geo.jvm.window.collect.multipolygon",
				EPA_NAME,
				"Description")

				.category(DataProcessorType.GEO)
				.withAssets(Assets.DOCUMENTATION, Assets.ICON)
				.requiredStream(StreamRequirementsBuilder
						.create()
						.requiredPropertyWithUnaryMapping(EpRequirements.stringReq(),
								Labels.from(
										WKT_TEXT,
										"Field with WKT String",
										"Contains wkt"),
								PropertyScope.NONE)
						.requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
								Labels.from(
										EPSG_CODE,
										"EPSG Code",
										"EPSG Code for SRID"),
								PropertyScope.NONE)

						.requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
								Labels.from(
										UID,
										"Unique ID field",
										"UID from geometry"),
								PropertyScope.NONE)
						.build()
				)

				.requiredSingleValueSelection(
						Labels.from(
								DISSOLVE,
								"aggregation type",
								"choose type of aggregation" ),
						Options.from(
								"True",
								"False"
						)
				)
				.supportedFormats(SupportedFormats.jsonFormat())
				.supportedProtocols(SupportedProtocols.kafka())
				.outputStrategy(OutputStrategies.append(
						EpProperties.stringEp(
								Labels.from(COLLECT,
										"multipolygon_wkt",
										"multipolygon"),
								COLLECT, SO.Text)
						)
				)
				.build();
	}

	@Override
	public ConfiguredEventProcessor<CollectMultiPolygonParameters> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor){


		String wkt = extractor.mappingPropertyValue(WKT_TEXT);
		String epsg = extractor.mappingPropertyValue(EPSG_CODE);
		String uid = extractor.mappingPropertyValue(UID);

		Boolean dissolve = extractor.selectedSingleValue(DISSOLVE, Boolean.class);

		CollectMultiPolygonParameters params = new CollectMultiPolygonParameters(graph, wkt, epsg, uid, dissolve);

		return new ConfiguredEventProcessor<>(params, CollectMultiPolygon::new);
	}

}
