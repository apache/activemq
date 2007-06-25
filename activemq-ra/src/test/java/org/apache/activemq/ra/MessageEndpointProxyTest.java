/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.ra;

import org.apache.activemq.ra.ActiveMQEndpointWorker;
import org.apache.activemq.ra.InvalidMessageEndpointException;
import org.apache.activemq.ra.MessageEndpointProxy;
import org.jmock.MockObjectTestCase;
import org.jmock.Mock;

import javax.jms.MessageListener;
import javax.jms.Message;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.ResourceException;
import java.lang.reflect.Method;

/**
 * @author <a href="mailto:michael.gaffney@panacya.com">Michael Gaffney </a>
 */
public class MessageEndpointProxyTest extends MockObjectTestCase {

    private Mock mockEndpoint;
    private Mock stubMessage;
    private MessageEndpointProxy endpointProxy;

    public MessageEndpointProxyTest(String name) {
        super(name);
    }
    
    protected void setUp() {
        mockEndpoint = new Mock(EndpointAndListener.class);
        stubMessage = new Mock(Message.class);
        endpointProxy = new MessageEndpointProxy((MessageEndpoint) mockEndpoint.proxy());       
    }

    public void testInvalidConstruction() {
        Mock mockEndpoint = new Mock(MessageEndpoint.class);
        try {
            new MessageEndpointProxy((MessageEndpoint) mockEndpoint.proxy());
            fail("An exception should have been thrown");
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
    }

    public void testSuccessfulCallSequence() {
        setupBeforeDeliverySuccessful();
        setupOnMessageSuccessful();
        setupAfterDeliverySuccessful();

        doBeforeDeliveryExpectSuccess();
        doOnMessageExpectSuccess();
        doAfterDeliveryExpectSuccess();
    }

    public void testBeforeDeliveryFailure() {
        mockEndpoint.expects(once()).method("beforeDelivery").with(isA(Method.class))
                .will(throwException(new ResourceException()));
        mockEndpoint.expects(never()).method("onMessage");
        mockEndpoint.expects(never()).method("afterDelivery");
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

    public void testOnMessageFailure() {
        setupBeforeDeliverySuccessful();
        mockEndpoint.expects(once()).method("onMessage").with(same(stubMessage.proxy()))
                .will(throwException(new RuntimeException()));
        setupAfterDeliverySuccessful();

        doBeforeDeliveryExpectSuccess();
        try {
            endpointProxy.onMessage((Message) stubMessage.proxy());
            fail("An exception should have been thrown");
        } catch (Exception e) {
            assertTrue(true);
        }
        doAfterDeliveryExpectSuccess();

    }

    public void testAfterDeliveryFailure() {
        setupBeforeDeliverySuccessful();
        setupOnMessageSuccessful();
        mockEndpoint.expects(once()).method("afterDelivery")
                .will(throwException(new ResourceException()));
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

    private void setupAfterDeliverySuccessful() {
        mockEndpoint.expects(once()).method("afterDelivery");
    }

    private void setupOnMessageSuccessful() {
        mockEndpoint.expects(once()).method("onMessage").with(same(stubMessage.proxy()));
    }

    private void setupBeforeDeliverySuccessful() {
        mockEndpoint.expects(once()).method("beforeDelivery").with(isA(Method.class));
    }

    private void setupExpectRelease() {
        mockEndpoint.expects(once()).method("release");
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
            endpointProxy.onMessage((Message) stubMessage.proxy());
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
            endpointProxy.onMessage((Message) stubMessage.proxy());
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
