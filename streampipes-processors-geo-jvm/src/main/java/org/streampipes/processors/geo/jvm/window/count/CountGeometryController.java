package org.streampipes.processors.geo.jvm.window.count;

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

public class CountGeometryController extends StandaloneEventProcessingDeclarer<CountGeometryParameters> {

	protected final static String WKT_TEXT = "wkt_text";
	protected final static String EPSG_CODE = "epsg_code";
	protected final static String UID = "uid";
	protected final static String SIZE = "size";
	protected final static String MAX = "maximum";

	protected final static String EPA_NAME = "Count Geometry";

	@Override
	public DataProcessorDescription declareModel() {
		return ProcessingElementBuilder.create("org.streampipes.processors.geo.jvm.window.count", EPA_NAME,
				"Counts the Geometry ")
				.category(DataProcessorType.GEO)
				.withAssets(Assets.DOCUMENTATION, Assets.ICON)
				.requiredStream(StreamRequirementsBuilder
						.create()
						.requiredPropertyWithUnaryMapping(EpRequirements.stringReq(),
								Labels.from(
										WKT_TEXT,
										"WKT String",
										"Field with WKT String"),
								PropertyScope.NONE)
						.requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
								Labels.from(
										EPSG_CODE,
										"EPSG Field",
										"EPSG Code for SRID"),
								PropertyScope.NONE)

						.requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
								Labels.from(
										UID,
										"Unique ID",
										"Unique ID field"),
								PropertyScope.NONE)
						.build()

				)
				.supportedFormats(SupportedFormats.jsonFormat())
				.supportedProtocols(SupportedProtocols.kafka())
				.outputStrategy(OutputStrategies.append(
						EpProperties.numberEp(
								Labels.from(SIZE,
										"sizeGeometries",
										"Size of geometries"),
								SIZE, SO.Number),

						EpProperties.numberEp(
								Labels.from(MAX,
										"max size in whole stream",
										"max size"),
								MAX, SO.Number)
						)
				)
				.build();
	}

	@Override
	public ConfiguredEventProcessor<CountGeometryParameters> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor){


		String wkt_String = extractor.mappingPropertyValue(WKT_TEXT);
		String epsg_code = extractor.mappingPropertyValue(EPSG_CODE);
		String uid = extractor.mappingPropertyValue(UID);

		CountGeometryParameters params = new CountGeometryParameters(graph, wkt_String, epsg_code, uid);

		return new ConfiguredEventProcessor<>(params, CountGeometry::new);
	}

}
