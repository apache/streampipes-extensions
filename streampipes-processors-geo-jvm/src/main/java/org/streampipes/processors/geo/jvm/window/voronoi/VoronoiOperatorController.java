package org.streampipes.processors.geo.jvm.window.voronoi;

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

public class VoronoiOperatorController extends StandaloneEventProcessingDeclarer<VoronoiOperatorParameters> {

	protected final static String WKT_TEXT = "wkt_text";
	protected final static String EPSG_CODE = "epsg_code";
	protected final static String UID = "uid";
	protected final static String VORONOI = "voronoi_wkt";

	protected final static String EPA_NAME = "Voronoi Operator";


	@Override
	public DataProcessorDescription declareModel() {
		return ProcessingElementBuilder.create(
				"org.streampipes.processors.geo.jvm.window.voronoi",
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
				.supportedFormats(SupportedFormats.jsonFormat())
				.supportedProtocols(SupportedProtocols.kafka())
				.outputStrategy(OutputStrategies.append(
						EpProperties.stringEp(
								Labels.from(VORONOI,
										"sizeGeometries",
										"Size of Geometry stream"),
								VORONOI, SO.Text)
						)
				)
				.build();
	}

	@Override
	public ConfiguredEventProcessor<VoronoiOperatorParameters> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor){


		String wkt = extractor.mappingPropertyValue(WKT_TEXT);
		String epsg = extractor.mappingPropertyValue(EPSG_CODE);
		String uid = extractor.mappingPropertyValue(UID);

		VoronoiOperatorParameters params = new VoronoiOperatorParameters(graph, wkt, epsg, uid);

		return new ConfiguredEventProcessor<>(params, VoronoiOperator::new);
	}

}
