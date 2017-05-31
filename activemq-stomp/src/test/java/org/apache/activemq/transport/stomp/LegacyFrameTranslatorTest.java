/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.stomp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.UUID;

import org.apache.activemq.command.ActiveMQDestination;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Tests for conversion capabilities of LegacyFrameTranslator
 */
public class LegacyFrameTranslatorTest {

    private ProtocolConverter converter;
    private LegacyFrameTranslator translator;

    @Before
    public void setUp() {
        converter = Mockito.mock(ProtocolConverter.class);

        // Stub out a temp destination creation
        Mockito.when(converter.createTempDestination(Mockito.anyString(), Mockito.anyBoolean())).thenAnswer(new Answer<ActiveMQDestination>() {

            @Override
            public ActiveMQDestination answer(InvocationOnMock invocation) throws Throwable {

                String name = invocation.getArgumentAt(0, String.class);
                boolean topic = invocation.getArgumentAt(1, Boolean.class);

                name = "temp-" + (topic ? "topic://" : "queue://X:") + UUID.randomUUID().toString();

                return ActiveMQDestination.createDestination(name, ActiveMQDestination.QUEUE_TYPE);
            }
        });

        translator = new LegacyFrameTranslator();
    }

    @Test(timeout = 10000)
    public void testConvertQueue() throws Exception {
        ActiveMQDestination destination = translator.convertDestination(converter, "/queue/test", false);

        assertFalse(destination.isComposite());
        assertEquals("test", destination.getPhysicalName());
        assertEquals(ActiveMQDestination.QUEUE_TYPE, destination.getDestinationType());
    }

    @Test(timeout = 10000)
    public void testConvertTopic() throws Exception {
        ActiveMQDestination destination = translator.convertDestination(converter, "/topic/test", false);

        assertFalse(destination.isComposite());
        assertEquals("test", destination.getPhysicalName());
        assertEquals(ActiveMQDestination.TOPIC_TYPE, destination.getDestinationType());
    }

    @Test(timeout = 10000)
    public void testConvertTemporaryQueue() throws Exception {
        ActiveMQDestination destination = translator.convertDestination(converter, "/temp-queue/test", false);

        assertFalse(destination.isComposite());
        assertEquals(ActiveMQDestination.TEMP_QUEUE_TYPE, destination.getDestinationType());
    }

    @Test(timeout = 10000)
    public void testConvertTemporaryTopic() throws Exception {
        ActiveMQDestination destination = translator.convertDestination(converter, "/temp-topic/test", false);

        assertFalse(destination.isComposite());
        assertEquals(ActiveMQDestination.TEMP_TOPIC_TYPE, destination.getDestinationType());
    }

    @Test(timeout = 10000)
    public void testConvertRemoteTempQueue() throws Exception {
        ActiveMQDestination destination = translator.convertDestination(converter, "/remote-temp-queue/test", false);

        assertFalse(destination.isComposite());
        assertEquals("test", destination.getPhysicalName());
        assertEquals(ActiveMQDestination.TEMP_QUEUE_TYPE, destination.getDestinationType());
    }

    @Test(timeout = 10000)
    public void testConvertRemoteTempTopic() throws Exception {
        ActiveMQDestination destination = translator.convertDestination(converter, "/remote-temp-topic/test", false);

        assertFalse(destination.isComposite());
        assertEquals("test", destination.getPhysicalName());
        assertEquals(ActiveMQDestination.TEMP_TOPIC_TYPE, destination.getDestinationType());
    }

    @Test(timeout = 10000)
    public void testConvertCompositeQueues() throws Exception {
        String destinationA = "destinationA";
        String destinationB = "destinationB";

        String composite = "/queue/" + destinationA + ",/queue/" + destinationB;

        ActiveMQDestination destination = translator.convertDestination(converter, composite, false);

        assertEquals(ActiveMQDestination.QUEUE_TYPE, destination.getDestinationType());
        assertTrue(destination.isComposite());
        ActiveMQDestination[] composites = destination.getCompositeDestinations();
        assertEquals(2, composites.length);

        Arrays.sort(composites);

        assertEquals(ActiveMQDestination.QUEUE_TYPE, composites[0].getDestinationType());
        assertEquals(ActiveMQDestination.QUEUE_TYPE, composites[1].getDestinationType());

        assertEquals(destinationA, composites[0].getPhysicalName());
        assertEquals(destinationB, composites[1].getPhysicalName());
    }

    @Test(timeout = 10000)
    public void testConvertCompositeTopics() throws Exception {
        String destinationA = "destinationA";
        String destinationB = "destinationB";

        String composite = "/topic/" + destinationA + ",/topic/" + destinationB;

        ActiveMQDestination destination = translator.convertDestination(converter, composite, false);

        assertEquals(ActiveMQDestination.TOPIC_TYPE, destination.getDestinationType());
        assertTrue(destination.isComposite());
        ActiveMQDestination[] composites = destination.getCompositeDestinations();
        assertEquals(2, composites.length);

        Arrays.sort(composites);

        assertEquals(ActiveMQDestination.TOPIC_TYPE, composites[0].getDestinationType());
        assertEquals(ActiveMQDestination.TOPIC_TYPE, composites[1].getDestinationType());

        assertEquals(destinationA, composites[0].getPhysicalName());
        assertEquals(destinationB, composites[1].getPhysicalName());
    }

    @Test(timeout = 10000)
    public void testConvertCompositeQueueAndTopic() throws Exception {
        String destinationA = "destinationA";
        String destinationB = "destinationB";

        String composite = "/queue/" + destinationA + ",/topic/" + destinationB;

        ActiveMQDestination destination = translator.convertDestination(converter, composite, false);

        assertEquals(ActiveMQDestination.QUEUE_TYPE, destination.getDestinationType());
        assertTrue(destination.isComposite());
        ActiveMQDestination[] composites = destination.getCompositeDestinations();
        assertEquals(2, composites.length);

        Arrays.sort(composites);

        assertEquals(ActiveMQDestination.QUEUE_TYPE, composites[0].getDestinationType());
        assertEquals(ActiveMQDestination.TOPIC_TYPE, composites[1].getDestinationType());

        assertEquals(destinationA, composites[0].getPhysicalName());
        assertEquals(destinationB, composites[1].getPhysicalName());
    }

    @Test(timeout = 10000)
    public void testConvertCompositeMixture() throws Exception {
        String destinationA = "destinationA";
        String destinationB = "destinationB";
        String destinationC = "destinationC";
        String destinationD = "destinationD";

        String composite = "/queue/" + destinationA + ",/topic/" + destinationB +
                           ",/temp-queue/" + destinationC + ",/temp-topic/" + destinationD;

        ActiveMQDestination destination = translator.convertDestination(converter, composite, false);

        assertEquals(ActiveMQDestination.QUEUE_TYPE, destination.getDestinationType());
        assertTrue(destination.isComposite());
        ActiveMQDestination[] composites = destination.getCompositeDestinations();
        assertEquals(4, composites.length);

        Arrays.sort(composites);

        boolean foundQueue = false;
        boolean foundTopic = false;
        boolean foundTempTopic = false;
        boolean foundTempQueue = false;

        for (ActiveMQDestination dest : composites) {
            if (dest.getDestinationType() == ActiveMQDestination.QUEUE_TYPE) {
                foundQueue = true;
            } else if (dest.getDestinationType() == ActiveMQDestination.TOPIC_TYPE) {
                foundTopic = true;
            } else if (dest.getDestinationType() == ActiveMQDestination.TEMP_TOPIC_TYPE) {
                foundTempTopic = true;
            } else if (dest.getDestinationType() == ActiveMQDestination.TEMP_QUEUE_TYPE) {
                foundTempQueue = true;
            }
        }

        assertTrue(foundQueue);
        assertTrue(foundTopic);
        assertTrue(foundTempTopic);
        assertTrue(foundTempQueue);
    }
}
