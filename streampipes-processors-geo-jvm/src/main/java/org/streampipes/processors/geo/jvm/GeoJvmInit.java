/*
 * Copyright 2018 FZI Forschungszentrum Informatik
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.streampipes.processors.geo.jvm;

import org.streampipes.container.init.DeclarersSingleton;
import org.streampipes.container.standalone.init.StandaloneModelSubmitter;
import org.streampipes.dataformat.json.JsonDataFormatFactory;
import org.streampipes.messaging.jms.SpJmsProtocolFactory;
import org.streampipes.messaging.kafka.SpKafkaProtocolFactory;
import org.streampipes.processors.geo.jvm.config.GeoJvmConfig;

import org.streampipes.processors.geo.jvm.database.dataFromRaster.pitch.PitchController;
import org.streampipes.processors.geo.jvm.database.dataFromRaster.precipitation.PrecipitationController;
import org.streampipes.processors.geo.jvm.database.networkAnalyst.isochrone.IsochroneController;
import org.streampipes.processors.geo.jvm.database.networkAnalyst.routing.oneManual.RoutingSingleInputController;
import org.streampipes.processors.geo.jvm.database.networkAnalyst.routing.twoManual.Routing_input_inputController;
import org.streampipes.processors.geo.jvm.database.geofence.storing.StoreGeofenceController;
import org.streampipes.processors.geo.jvm.database.geofence.enricher.EnricherController;
import org.streampipes.processors.geo.jvm.processors.changeGeometries.direction.DirectionController;
import org.streampipes.processors.geo.jvm.processors.changeGeometries.refineLine.RefineLineController;
import org.streampipes.processors.geo.jvm.processors.dataOperators.latLngToGeo.LatLngToGeoController;
import org.streampipes.processors.geo.jvm.processors.dataOperators.projTransformation.ProjTransformationController;
import org.streampipes.processors.geo.jvm.processors.dataOperators.setEpsgCode.SetEpsgController;
import org.streampipes.processors.geo.jvm.processors.dataOperators.validator.GeoValidatorController;
import org.streampipes.processors.geo.jvm.processors.derivedGeometry.bufferOperator.bufferGeometry.BufferGeometryController;
import org.streampipes.processors.geo.jvm.processors.derivedGeometry.bufferOperator.bufferPoint.BufferPointController;
import org.streampipes.processors.geo.jvm.processors.derivedGeometry.centroidPoint.CentroidPointController;
import org.streampipes.processors.geo.jvm.processors.derivedGeometry.convexHull.ConvexHullController;
import org.streampipes.processors.geo.jvm.processors.derivedGeometry.envelope.EnvelopeController;
import org.streampipes.processors.geo.jvm.processors.derivedGeometry.interiorPoint.InteriorPointController;
import org.streampipes.processors.geo.jvm.processors.derivedGeometry.overlayOperator.filter.OverlaySpFilterController;
import org.streampipes.processors.geo.jvm.processors.measureOperator.areaCalc.AreaCalcController;
import org.streampipes.processors.geo.jvm.processors.measureOperator.distanceCalc.geodesic.DistanceGeodesicController;
import org.streampipes.processors.geo.jvm.processors.measureOperator.distanceCalc.planar.DistancePlanarController;
import org.streampipes.processors.geo.jvm.processors.measureOperator.hausdorffDistanceCalc.HausdorffDistanceController;
import org.streampipes.processors.geo.jvm.processors.measureOperator.lineLengthCalc.LineLengthController;
import org.streampipes.processors.geo.jvm.processors.measureOperator.polygonPerimeterCalc.PolygonPerimeterController;
import org.streampipes.processors.geo.jvm.processors.thematicQueries.arithmeticOperators.NumAttributeCalcController;
import org.streampipes.processors.geo.jvm.processors.thematicQueries.comparisonOperators.NumericalFilterController;
import org.streampipes.processors.geo.jvm.processors.thematicQueries.multiTextAttributeFilter.MultiTextAttributeFilterController;
import org.streampipes.processors.geo.jvm.processors.thematicQueries.textAttributeFilter.TextAttributeFilterController;
import org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter.TopoRelationController;
import org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter.typeFilter.equals.EqualsTopoController;
import org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter.typeFilter.intersects.IntersectsTopoController;
import org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter.typeFilter.touches.TouchesTopoController;
import org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter.typeFilter.touchesPolygons.TouchesPolygonController;
import org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter.typeFilter.within.WithinTopoController;
import org.streampipes.processors.geo.jvm.window.collect.multiline.CollectMultiLineStringController;
import org.streampipes.processors.geo.jvm.window.collect.multipoint.CollectMultiPointController;
import org.streampipes.processors.geo.jvm.window.collect.multipolygon.CollectMultiPolygonController;
import org.streampipes.processors.geo.jvm.window.count.CountGeometryController;
import org.streampipes.processors.geo.jvm.window.voronoi.VoronoiOperatorController;

public class GeoJvmInit extends StandaloneModelSubmitter {

  public static void main(String[] args) {
    DeclarersSingleton
            .getInstance()
             //==========databaseFromRaster
            .add(new PitchController())
            .add(new PrecipitationController())
            //==========databaseFromRaster
            .add(new IsochroneController())
            .add(new RoutingSingleInputController())
            .add(new Routing_input_inputController())
            //==========changeGeometries
            .add(new DirectionController())
            .add(new RefineLineController())
            //==========dataOperator
            .add(new LatLngToGeoController())
            .add(new ProjTransformationController())
            .add(new SetEpsgController())
            .add(new GeoValidatorController())
            //==========derivedGeometry
            .add(new BufferGeometryController())
            .add(new BufferPointController())
            .add(new CentroidPointController())
            .add(new ConvexHullController())
            .add(new EnvelopeController())
            .add(new InteriorPointController())
            //==========overlay
            .add(new OverlaySpFilterController())
            .add(new EnricherController())
            //==========measurement
            .add(new AreaCalcController())
            .add(new DistanceGeodesicController())
            .add(new DistancePlanarController())
            .add(new HausdorffDistanceController())
            .add(new LineLengthController())
            .add(new PolygonPerimeterController())
            //==========thematic
            .add(new NumAttributeCalcController())
            .add(new NumericalFilterController())
            .add(new TextAttributeFilterController())
            .add(new MultiTextAttributeFilterController())
            //==========topology
            .add(new TopoRelationController())
            .add(new EqualsTopoController())
            .add(new IntersectsTopoController())
            .add(new TouchesTopoController())
            .add(new TouchesPolygonController())
            .add(new WithinTopoController())
            //==========geofence
            .add(new StoreGeofenceController())
            .add(new EnricherController())
            //==========Sink
            //.add(new PostgreSqlController())
            //==========Window
            .add(new CountGeometryController())
            .add(new VoronoiOperatorController())
            .add(new CollectMultiPointController())
            .add(new CollectMultiPolygonController())
            .add(new CollectMultiLineStringController());

    DeclarersSingleton.getInstance().registerDataFormat(new JsonDataFormatFactory());
    DeclarersSingleton.getInstance().registerProtocol(new SpKafkaProtocolFactory());
    DeclarersSingleton.getInstance().registerProtocol(new SpJmsProtocolFactory());

    new GeoJvmInit().init(GeoJvmConfig.INSTANCE);
  }
}
