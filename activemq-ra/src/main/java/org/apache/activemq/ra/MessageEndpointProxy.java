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

import java.lang.reflect.Method;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.resource.ResourceException;
import javax.resource.spi.endpoint.MessageEndpoint;

/**
 * @author <a href="mailto:michael.gaffney@panacya.com">Michael Gaffney </a>
 */
public class MessageEndpointProxy implements MessageListener, MessageEndpoint {

    private static final MessageEndpointState ALIVE = new MessageEndpointAlive();
    private static final MessageEndpointState DEAD = new MessageEndpointDead();

    private static int proxyCount;
    private final int proxyID;

    private final MessageEndpoint endpoint;
    private final MessageListener messageListener;
    private MessageEndpointState state = ALIVE;

    public MessageEndpointProxy(MessageEndpoint endpoint) {
        if (!(endpoint instanceof MessageListener)) {
            throw new IllegalArgumentException("MessageEndpoint is not a MessageListener");
        }
        messageListener = (MessageListener)endpoint;
        proxyID = getID();
        this.endpoint = endpoint;
    }

    private static int getID() {
        return ++proxyCount;
    }

    public void beforeDelivery(Method method) throws NoSuchMethodException, ResourceException {
        state.beforeDelivery(this, method);
    }

    public void onMessage(Message message) {
        state.onMessage(this, message);
    }

    public void afterDelivery() throws ResourceException {
        state.afterDelivery(this);
    }

    public void release() {
        state.release(this);
    }

    public String toString() {
        return "MessageEndpointProxy{ " + "proxyID: " + proxyID + ", endpoint: " + endpoint + " }";
    }

    private abstract static class MessageEndpointState {

        public void beforeDelivery(MessageEndpointProxy proxy, Method method) throws NoSuchMethodException, ResourceException {
            throw new IllegalStateException();
        }

        public void onMessage(MessageEndpointProxy proxy, Message message) {
            throw new IllegalStateException();
        }

        public void afterDelivery(MessageEndpointProxy proxy) throws ResourceException {
            throw new IllegalStateException();
        }

        public void release(MessageEndpointProxy proxy) {
            throw new IllegalStateException();
        }

        protected final void transition(MessageEndpointProxy proxy, MessageEndpointState nextState) {
            proxy.state = nextState;
            nextState.enter(proxy);
        }

        protected void enter(MessageEndpointProxy proxy) {
        }
    }

    private static class MessageEndpointAlive extends MessageEndpointState {

        public void beforeDelivery(MessageEndpointProxy proxy, Method method) throws NoSuchMethodException, ResourceException {
            try {
                proxy.endpoint.beforeDelivery(method);
            } catch (NoSuchMethodException e) {
                transition(proxy, DEAD);
                throw e;
            } catch (ResourceException e) {
                transition(proxy, DEAD);
                throw e;
            }
        }

        public void onMessage(MessageEndpointProxy proxy, Message message) {
            proxy.messageListener.onMessage(message);
        }

        public void afterDelivery(MessageEndpointProxy proxy) throws ResourceException {
            try {
                proxy.endpoint.afterDelivery();
            } catch (ResourceException e) {
                transition(proxy, DEAD);
                throw e;
            }
        }

        public void release(MessageEndpointProxy proxy) {
            transition(proxy, DEAD);
        }
    }

    private static class MessageEndpointDead extends MessageEndpointState {

        protected void enter(MessageEndpointProxy proxy) {
            proxy.endpoint.release();
        }

        public void beforeDelivery(MessageEndpointProxy proxy, Method method) throws NoSuchMethodException, ResourceException {
            throw new InvalidMessageEndpointException();
        }

        public void onMessage(MessageEndpointProxy proxy, Message message) {
            throw new InvalidMessageEndpointException();
        }

        public void afterDelivery(MessageEndpointProxy proxy) throws ResourceException {
            throw new InvalidMessageEndpointException();
        }

        public void release(MessageEndpointProxy proxy) {
            throw new InvalidMessageEndpointException();
        }
    }
}
