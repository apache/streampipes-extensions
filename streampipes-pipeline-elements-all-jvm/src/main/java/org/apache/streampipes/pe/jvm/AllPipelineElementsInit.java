/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.streampipes.pe.jvm;

import org.apache.streampipes.container.init.DeclarersSingleton;
import org.apache.streampipes.container.standalone.init.StandaloneModelSubmitter;
import org.apache.streampipes.dataformat.cbor.CborDataFormatFactory;
import org.apache.streampipes.dataformat.fst.FstDataFormatFactory;
import org.apache.streampipes.dataformat.json.JsonDataFormatFactory;
import org.apache.streampipes.dataformat.smile.SmileDataFormatFactory;
import org.apache.streampipes.messaging.jms.SpJmsProtocolFactory;
import org.apache.streampipes.messaging.kafka.SpKafkaProtocolFactory;
import org.apache.streampipes.pe.jvm.config.AllPipelineElementsConfig;
import org.apache.streampipes.processors.enricher.jvm.processor.sizemeasure.SizeMeasureController;
import org.apache.streampipes.processors.filters.jvm.processor.compose.ComposeController;
import org.apache.streampipes.processors.filters.jvm.processor.enrich.MergeByEnrichController;
import org.apache.streampipes.processors.filters.jvm.processor.merge.MergeByTimeController;
import org.apache.streampipes.processors.filters.jvm.processor.numericalfilter.NumericalFilterController;
import org.apache.streampipes.processors.filters.jvm.processor.numericaltextfilter.NumericalTextFilterController;
import org.apache.streampipes.processors.filters.jvm.processor.projection.ProjectionController;
import org.apache.streampipes.processors.filters.jvm.processor.textfilter.TextFilterController;
import org.apache.streampipes.processors.filters.jvm.processor.threshold.ThresholdDetectionController;
import org.apache.streampipes.processors.geo.jvm.jts.processor.latLngToGeo.LatLngToGeoController;
import org.apache.streampipes.processors.geo.jvm.jts.processor.setEPSG.SetEpsgController;
import org.apache.streampipes.processors.geo.jvm.processor.distancecalculator.DistanceCalculatorController;
import org.apache.streampipes.processors.geo.jvm.processor.geocoder.GoogleMapsGeocodingController;
import org.apache.streampipes.processors.geo.jvm.processor.revgeocoder.ReverseGeocodingController;
import org.apache.streampipes.processors.geo.jvm.processor.speed.SpeedCalculatorController;
import org.apache.streampipes.processors.geo.jvm.processor.staticdistancecalculator.StaticDistanceCalculatorController;
import org.apache.streampipes.processors.geo.jvm.processor.staticgeocoder.StaticGoogleMapsGeocodingController;
import org.apache.streampipes.processors.imageprocessing.jvm.processor.genericclassification.GenericImageClassificationController;
import org.apache.streampipes.processors.imageprocessing.jvm.processor.imagecropper.ImageCropperController;
import org.apache.streampipes.processors.imageprocessing.jvm.processor.imageenrichment.ImageEnrichmentController;
import org.apache.streampipes.processors.imageprocessing.jvm.processor.qrreader.QrCodeReaderController;
import org.apache.streampipes.processors.siddhi.stop.StreamStopController;
import org.apache.streampipes.processors.siddhi.trend.TrendController;
import org.apache.streampipes.processors.transformation.jvm.processor.array.count.CountArrayController;
import org.apache.streampipes.processors.transformation.jvm.processor.array.split.SplitArrayController;
import org.apache.streampipes.processors.transformation.jvm.processor.booloperator.counter.BooleanCounterController;
import org.apache.streampipes.processors.transformation.jvm.processor.booloperator.inverter.BooleanInverterController;
import org.apache.streampipes.processors.transformation.jvm.processor.booloperator.timekeeping.BooleanTimekeepingController;
import org.apache.streampipes.processors.transformation.jvm.processor.booloperator.timer.BooleanTimerController;
import org.apache.streampipes.processors.transformation.jvm.processor.csvmetadata.CsvMetadataEnrichmentController;
import org.apache.streampipes.processors.transformation.jvm.processor.stringoperator.timer.StringTimerController;
import org.apache.streampipes.processors.transformation.jvm.processor.stringoperator.counter.StringCounterController;
import org.apache.streampipes.processors.transformation.jvm.processor.task.TaskDurationController;
import org.apache.streampipes.processors.transformation.jvm.processor.timestampextractor.TimestampExtractorController;
import org.apache.streampipes.processors.transformation.jvm.processor.value.change.ChangedValueDetectionController;
import org.apache.streampipes.processors.transformation.jvm.processor.value.duration.CalculateDurationController;
import org.apache.streampipes.sinks.brokers.jvm.bufferrest.BufferRestController;
import org.apache.streampipes.sinks.brokers.jvm.jms.JmsController;
import org.apache.streampipes.sinks.brokers.jvm.kafka.KafkaController;
import org.apache.streampipes.sinks.brokers.jvm.mqtt.MqttController;
import org.apache.streampipes.sinks.brokers.jvm.pulsar.PulsarController;
import org.apache.streampipes.sinks.brokers.jvm.rabbitmq.RabbitMqController;
import org.apache.streampipes.sinks.brokers.jvm.rest.RestController;
import org.apache.streampipes.sinks.databases.jvm.couchdb.CouchDbController;
import org.apache.streampipes.sinks.databases.jvm.influxdb.InfluxDbController;
import org.apache.streampipes.sinks.databases.jvm.iotdb.IotDbController;
import org.apache.streampipes.sinks.databases.jvm.opcua.UpcUaController;
import org.apache.streampipes.sinks.databases.jvm.postgresql.PostgreSqlController;
import org.apache.streampipes.sinks.internal.jvm.dashboard.DashboardController;
import org.apache.streampipes.sinks.internal.jvm.datalake.DataLakeController;
import org.apache.streampipes.sinks.internal.jvm.notification.NotificationController;
import org.apache.streampipes.sinks.notifications.jvm.email.EmailController;
import org.apache.streampipes.sinks.notifications.jvm.onesignal.OneSignalController;
import org.apache.streampipes.sinks.notifications.jvm.slack.SlackNotificationController;

public class AllPipelineElementsInit extends StandaloneModelSubmitter {

  public static void main(String[] args) {
    DeclarersSingleton
            .getInstance()
            .add(new NumericalFilterController())
            .add(new ThresholdDetectionController())
            .add(new TextFilterController())
            .add(new ProjectionController())
            .add(new ComposeController())
            .add(new MergeByEnrichController())
            .add(new MergeByTimeController())
            .add(new TrendController())
            .add(new StreamStopController())
            .add(new CountArrayController())
            .add(new SplitArrayController())
            .add(new CalculateDurationController())
            .add(new ChangedValueDetectionController())
            .add(new TimestampExtractorController())
            .add(new BooleanCounterController())
            .add(new BooleanInverterController())
            .add(new BooleanTimekeepingController())
            .add(new BooleanTimerController())
            .add(new CsvMetadataEnrichmentController())
            .add(new TaskDurationController())
            .add(new ImageEnrichmentController())
            .add(new ImageCropperController())
            .add(new QrCodeReaderController())
            .add(new GenericImageClassificationController())
            .add(new KafkaController())
            .add(new MqttController())
            .add(new JmsController())
            .add(new RestController())
            .add(new BufferRestController())
            .add(new RabbitMqController())
            .add(new PulsarController())
            .add(new CouchDbController())
            .add(new InfluxDbController())
            .add(new UpcUaController())
            .add(new PostgreSqlController())
            .add(new IotDbController())
            .add(new NotificationController())
            .add(new DataLakeController())
            .add(new DashboardController())
            .add(new EmailController())
            .add(new OneSignalController())
            .add(new SlackNotificationController())
            .add(new NumericalTextFilterController())
            .add(new SizeMeasureController())
            .add(new DistanceCalculatorController())
            .add(new GoogleMapsGeocodingController())
            .add(new StaticGoogleMapsGeocodingController())
            .add(new ReverseGeocodingController())
            .add(new SetEpsgController())
            .add(new LatLngToGeoController())
            .add(new SpeedCalculatorController())
            .add(new StaticDistanceCalculatorController())
            .add(new StringCounterController())
            .add(new StringTimerController());

    DeclarersSingleton.getInstance().registerDataFormats(new JsonDataFormatFactory(),
            new CborDataFormatFactory(),
            new SmileDataFormatFactory(),
            new FstDataFormatFactory());

    DeclarersSingleton.getInstance().registerProtocols(new SpKafkaProtocolFactory(),
            new SpJmsProtocolFactory());

    new AllPipelineElementsInit().init(AllPipelineElementsConfig.INSTANCE);
  }
}
