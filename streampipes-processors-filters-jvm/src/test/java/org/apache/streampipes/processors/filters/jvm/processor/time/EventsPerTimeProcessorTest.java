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
package org.apache.streampipes.processors.filters.jvm.processor.time;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class EventsPerTimeProcessorTest {
    private List<Long> timestamps = Arrays.asList(1000L, 2000L, 3000L, 4000L);

    @Test
    public void testFieldAddition1() {
        EventsPerTimeProcessor eventsPerTimeProcessor = new EventsPerTimeProcessor(
                timestamps,
                3,
                5,
                EventsPerTimeProcessor.SECOND,
                2,
                EventsPerTimeProcessor.EACH);

        assertEquals(true, eventsPerTimeProcessor.applyRule());
        assertEquals(4, eventsPerTimeProcessor.getTimestamps().size());
    }

    @Test
    public void testFieldAddition2() {
        EventsPerTimeProcessor eventsPerTimeProcessor = new EventsPerTimeProcessor(
                timestamps,
                5,
                5,
                EventsPerTimeProcessor.SECOND,
                2,
                EventsPerTimeProcessor.EACH);

        assertEquals(false, eventsPerTimeProcessor.applyRule());
        assertEquals(4, eventsPerTimeProcessor.getTimestamps().size());
    }

    @Test
    public void testFieldAddition3() {
        EventsPerTimeProcessor eventsPerTimeProcessor = new EventsPerTimeProcessor(
                timestamps,
                2,
                2,
                EventsPerTimeProcessor.SECOND,
                2,
                EventsPerTimeProcessor.EACH);

        assertEquals(false, eventsPerTimeProcessor.applyRule());
        assertEquals(2, eventsPerTimeProcessor.getTimestamps().size());
    }
}