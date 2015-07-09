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

import java.lang.reflect.Method;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.resource.ResourceException;
import javax.resource.spi.endpoint.MessageEndpoint;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JMock.class)
public class MessageEndpointProxyTest {

    private MessageEndpoint mockEndpoint;
    private EndpointAndListener mockEndpointAndListener;
    private Message stubMessage;
    private MessageEndpointProxy endpointProxy;
    private Mockery context;

    @Before
    public void setUp() {
        context = new Mockery();
        mockEndpoint = context.mock(MessageEndpoint.class);
        context.mock(MessageListener.class);
        mockEndpointAndListener = context.mock(EndpointAndListener.class);
        stubMessage = context.mock(Message.class);
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
        setupBeforeDeliverySuccessful();
        setupOnMessageSuccessful();
        setupAfterDeliverySuccessful();

        doBeforeDeliveryExpectSuccess();
        doOnMessageExpectSuccess();
        doAfterDeliveryExpectSuccess();
    }

    @Test(timeout = 60000)
    public void testBeforeDeliveryFailure() throws Exception {
        context.checking(new Expectations() {{
            oneOf (mockEndpointAndListener).beforeDelivery(with(any(Method.class)));
            will(throwException(new ResourceException()));
        }});
        context.checking(new Expectations() {{
            never (mockEndpointAndListener).onMessage(null);
            never (mockEndpointAndListener).afterDelivery();
        }});

        setupExpectRelease();

        try {
            endpointProxy.beforeDelivery(ActiveMQEndpointWorker.ON_MESSAGE_METHOD);
            fail("An exception should have been thrown");
        } catch (Exception e) {
            assertTrue(true);
        }
        doOnMessageExpectInvalidMessageEndpointException();
        doAfterDeliveryExpectInvalidMessageEndpointException();

        doFullyDeadCheck();
    }

    @Test(timeout = 60000)
    public void testOnMessageFailure() throws Exception {
        setupBeforeDeliverySuccessful();

        context.checking(new Expectations() {{
            oneOf (mockEndpointAndListener).onMessage(with(same(stubMessage)));
            will(throwException(new RuntimeException()));
        }});

        setupAfterDeliverySuccessful();

        doBeforeDeliveryExpectSuccess();
        try {
            endpointProxy.onMessage(stubMessage);
            fail("An exception should have been thrown");
        } catch (Exception e) {
            assertTrue(true);
        }
        doAfterDeliveryExpectSuccess();

    }

    @Test(timeout = 60000)
    public void testAfterDeliveryFailure() throws Exception {
        setupBeforeDeliverySuccessful();
        setupOnMessageSuccessful();

        context.checking(new Expectations() {{
            oneOf (mockEndpointAndListener).afterDelivery(); will(throwException(new ResourceException()));
        }});

        setupExpectRelease();

        doBeforeDeliveryExpectSuccess();
        doOnMessageExpectSuccess();
        try {
            endpointProxy.afterDelivery();
            fail("An exception should have been thrown");
        } catch (Exception e) {
            assertTrue(true);
        }

        doFullyDeadCheck();
    }

    private void doFullyDeadCheck() {
        doBeforeDeliveryExpectInvalidMessageEndpointException();
        doOnMessageExpectInvalidMessageEndpointException();
        doAfterDeliveryExpectInvalidMessageEndpointException();
        doReleaseExpectInvalidMessageEndpointException();
    }

    private void setupAfterDeliverySuccessful() throws Exception {
        context.checking(new Expectations() {{
            oneOf (mockEndpointAndListener).afterDelivery();
        }});
    }

    private void setupOnMessageSuccessful() {
        context.checking(new Expectations() {{
            oneOf (mockEndpointAndListener).onMessage(with(stubMessage));
        }});
    }

    private void setupBeforeDeliverySuccessful() throws Exception {
        context.checking(new Expectations() {{
            oneOf (mockEndpointAndListener).beforeDelivery(with(any(Method.class)));
        }});
    }

    private void setupExpectRelease() {
        context.checking(new Expectations() {{
            oneOf (mockEndpointAndListener).release();
        }});
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
