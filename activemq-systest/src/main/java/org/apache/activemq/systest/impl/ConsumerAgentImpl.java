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
package org.apache.activemq.systest.impl;

import org.apache.activemq.systest.AgentStopper;
import org.apache.activemq.systest.ConsumerAgent;
import org.apache.activemq.systest.MessageList;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Topic;

/**
 * A simple in JVM implementation of a {@link ConsumerAgent}
 * 
 * @version $Revision: 1.1 $
 */
public class ConsumerAgentImpl extends JmsClientSupport implements ConsumerAgent {

    private String selector;
    private String durableSubscriber;
    private boolean noLocal;
    private MessageConsumer consumer;
    private AgentMessageListener listener;

    public void start() throws Exception {
        listener = new AgentMessageListener();
        getConsumer().setMessageListener(listener);
        super.start();
    }

    public void assertConsumed(MessageList messageList) throws JMSException {
        int size = messageList.getSize();
        listener.waitForMessagesToArrive(size);

        // now we've received them, lets check that they are identical
        messageList.assertMessagesCorrect(listener.flushMessages());

        System.out.println("Consumer received all: " + size + " message(s)");
    }

    public void waitUntilConsumed(MessageList messageList, int percentOfList) {
        int size = messageList.getSize();
        int limit = (size * percentOfList) / 100;
        listener.waitForMessagesToArrive(limit);
    }

    // Properties
    // -------------------------------------------------------------------------
    public MessageConsumer getConsumer() throws JMSException {
        if (consumer == null) {
            consumer = createConsumer();
        }
        return consumer;
    }

    public void setConsumer(MessageConsumer consumer) {
        this.consumer = consumer;
    }

    public String getDurableSubscriber() {
        return durableSubscriber;
    }

    public void setDurableSubscriber(String durableSubscriber) {
        this.durableSubscriber = durableSubscriber;
    }

    public boolean isNoLocal() {
        return noLocal;
    }

    public void setNoLocal(boolean noLocal) {
        this.noLocal = noLocal;
    }

    public String getSelector() {
        return selector;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    public void stop(AgentStopper stopper) {
        if (listener != null) {
            listener.stop();
            listener = null;
        }

        if (consumer != null) {
            try {
                consumer.close();
            }
            catch (JMSException e) {
                stopper.onException(this, e);
            }
            finally {
                consumer = null;
            }
        }
        super.stop(stopper);
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected MessageConsumer createConsumer() throws JMSException {
        if (durableSubscriber != null) {
            if (selector != null) {
                return getSession().createDurableSubscriber((Topic) getDestination(), durableSubscriber, selector, noLocal);
            }
            else {
                return getSession().createDurableSubscriber((Topic) getDestination(), durableSubscriber);
            }
        }
        else {
            if (selector != null) {
                if (noLocal) {
                    return getSession().createConsumer(getDestination(), selector, noLocal);
                }
                else {
                    return getSession().createConsumer(getDestination(), selector);
                }
            }
            else {
                return getSession().createConsumer(getDestination());
            }
        }

    }
}
