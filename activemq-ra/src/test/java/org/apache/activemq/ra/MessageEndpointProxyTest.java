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
package org.apache.activemq.ra;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;

import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.resource.ResourceException;
import jakarta.resource.spi.endpoint.MessageEndpoint;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class MessageEndpointProxyTest {

    private MessageEndpoint mockEndpoint;
    private EndpointAndListener mockEndpointAndListener;
    private Message stubMessage;
    private MessageEndpointProxy endpointProxy;

    @Before
    public void setUp() {
        mockEndpoint = mock(MessageEndpoint.class);
        mockEndpointAndListener = mock(EndpointAndListener.class);
        stubMessage = mock(Message.class);
        endpointProxy = new MessageEndpointProxy(mockEndpointAndListener);
    }

    @Test(timeout = 60000)
    public void testInvalidConstruction() {
        try {
            new MessageEndpointProxy(mockEndpoint);
            fail("An exception should have been thrown");
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
    }

    @Test(timeout = 60000)
    public void testSuccessfulCallSequence() throws Exception {
        doBeforeDeliveryExpectSuccess();
        doOnMessageExpectSuccess();
        doAfterDeliveryExpectSuccess();

        verify(mockEndpointAndListener).beforeDelivery(any(Method.class));
        verify(mockEndpointAndListener).onMessage(stubMessage);
        verify(mockEndpointAndListener).afterDelivery();
    }

    @Test(timeout = 60000)
    public void testBeforeDeliveryFailure() throws Exception {
        doThrow(new ResourceException()).when(mockEndpointAndListener).beforeDelivery(any(Method.class));

        try {
            endpointProxy.beforeDelivery(ActiveMQEndpointWorker.ON_MESSAGE_METHOD);
            fail("An exception should have been thrown");
        } catch (Exception e) {
            assertTrue(true);
        }
        doOnMessageExpectInvalidMessageEndpointException();
        doAfterDeliveryExpectInvalidMessageEndpointException();

        verify(mockEndpointAndListener, never()).onMessage(any());
        verify(mockEndpointAndListener, never()).afterDelivery();
        verify(mockEndpointAndListener).release();

        doFullyDeadCheck();
    }

    @Test(timeout = 60000)
    public void testOnMessageFailure() throws Exception {
        doBeforeDeliveryExpectSuccess();

        doThrow(new RuntimeException()).when(mockEndpointAndListener).onMessage(same(stubMessage));

        try {
            endpointProxy.onMessage(stubMessage);
            fail("An exception should have been thrown");
        } catch (Exception e) {
            assertTrue(true);
        }
        doAfterDeliveryExpectSuccess();

        verify(mockEndpointAndListener).beforeDelivery(any(Method.class));
        verify(mockEndpointAndListener).onMessage(same(stubMessage));
        verify(mockEndpointAndListener).afterDelivery();
    }

    @Test(timeout = 60000)
    public void testAfterDeliveryFailure() throws Exception {
        doBeforeDeliveryExpectSuccess();
        doOnMessageExpectSuccess();

        doThrow(new ResourceException()).when(mockEndpointAndListener).afterDelivery();

        try {
            endpointProxy.afterDelivery();
            fail("An exception should have been thrown");
        } catch (Exception e) {
            assertTrue(true);
        }

        verify(mockEndpointAndListener).beforeDelivery(any(Method.class));
        verify(mockEndpointAndListener).onMessage(stubMessage);
        verify(mockEndpointAndListener).afterDelivery();
        verify(mockEndpointAndListener).release();

        doFullyDeadCheck();
    }

    private void doFullyDeadCheck() {
        doBeforeDeliveryExpectInvalidMessageEndpointException();
        doOnMessageExpectInvalidMessageEndpointException();
        doAfterDeliveryExpectInvalidMessageEndpointException();
        doReleaseExpectInvalidMessageEndpointException();
    }

    private void doBeforeDeliveryExpectSuccess() {
        try {
            endpointProxy.beforeDelivery(ActiveMQEndpointWorker.ON_MESSAGE_METHOD);
        } catch (Exception e) {
            fail("No exception should have been thrown");
        }
    }

    private void doOnMessageExpectSuccess() {
        try {
            endpointProxy.onMessage(stubMessage);
        } catch (Exception e) {
            fail("No exception should have been thrown");
        }
    }

    private void doAfterDeliveryExpectSuccess() {
        try {
            endpointProxy.afterDelivery();
        } catch (Exception e) {
            fail("No exception should have been thrown");
        }
    }

    private void doBeforeDeliveryExpectInvalidMessageEndpointException() {
        try {
            endpointProxy.beforeDelivery(ActiveMQEndpointWorker.ON_MESSAGE_METHOD);
            fail("An InvalidMessageEndpointException should have been thrown");
        } catch (InvalidMessageEndpointException e) {
            assertTrue(true);
        } catch (Exception e) {
            fail("An InvalidMessageEndpointException should have been thrown");
        }
    }

    private void doOnMessageExpectInvalidMessageEndpointException() {
        try {
            endpointProxy.onMessage(stubMessage);
            fail("An InvalidMessageEndpointException should have been thrown");
        } catch (InvalidMessageEndpointException e) {
            assertTrue(true);
        }
    }

    private void doAfterDeliveryExpectInvalidMessageEndpointException() {
        try {
            endpointProxy.afterDelivery();
            fail("An InvalidMessageEndpointException should have been thrown");
        } catch (InvalidMessageEndpointException e) {
            assertTrue(true);
        } catch (Exception e) {
            fail("An InvalidMessageEndpointException should have been thrown");
        }
    }

    private void doReleaseExpectInvalidMessageEndpointException() {
        try {
            endpointProxy.release();
            fail("An InvalidMessageEndpointException should have been thrown");
        } catch (InvalidMessageEndpointException e) {
            assertTrue(true);
        }
    }

    private interface EndpointAndListener extends MessageListener, MessageEndpoint {
    }
}
