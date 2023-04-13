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
package org.apache.activemq.web;

import java.util.LinkedList;

import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;

import org.apache.activemq.MessageAvailableListener;
import org.apache.activemq.web.async.AsyncServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Listen for available messages and wakeup any asyncRequests.
 */
public class AjaxListener implements MessageAvailableListener {
    private static final Logger LOG = LoggerFactory.getLogger(AjaxListener.class);

    private final long maximumReadTimeout;
    private final AjaxWebClient client;
    private long lastAccess;
    private AsyncServletRequest asyncRequest;
    private final LinkedList<UndeliveredAjaxMessage> undeliveredMessages = new LinkedList<UndeliveredAjaxMessage>();

    AjaxListener(AjaxWebClient client, long maximumReadTimeout) {
        this.client = client;
        this.maximumReadTimeout = maximumReadTimeout;
        access();
    }

    public void access() {
        lastAccess = System.currentTimeMillis();
    }

    public synchronized void setAsyncRequest(AsyncServletRequest asyncRequest) {
        this.asyncRequest = asyncRequest;
    }

    public LinkedList<UndeliveredAjaxMessage> getUndeliveredMessages() {
        return undeliveredMessages;
    }

    @Override
    public synchronized void onMessageAvailable(MessageConsumer consumer) {
        LOG.debug("Message for consumer: {} asyncRequest: {}", consumer, asyncRequest);

        if (asyncRequest != null) {
            try {
                Message message = consumer.receive(10);
                LOG.debug("message is " + message);
                if (message != null) {
                    if (!asyncRequest.isDispatched()) {
                        LOG.debug("Resuming suspended asyncRequest {}", asyncRequest);
                        asyncRequest.setAttribute("undelivered_message", new UndeliveredAjaxMessage(message, consumer));
                        asyncRequest.dispatch();
                    } else {
                        LOG.debug("Message available, but asyncRequest is already resumed. Buffer for next time.");
                        bufferMessageForDelivery(message, consumer);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Error receiving message " + e.getMessage() + ". This exception is ignored.", e);
            }

        } else if (System.currentTimeMillis() - lastAccess > 2 * this.maximumReadTimeout) {
            new Thread() {
                @Override
                public void run() {
                    LOG.debug("Closing consumers on client: {}", client);
                    client.closeConsumers();
                }
            }.start();
        } else {
            try {
                Message message = consumer.receive(10);
                bufferMessageForDelivery(message, consumer);
            } catch (Exception e) {
                LOG.warn("Error receiving message " + e.getMessage() + ". This exception is ignored.", e);
            }
        }
    }

    public void bufferMessageForDelivery(Message message, MessageConsumer consumer) {
        if (message != null) {
            synchronized (undeliveredMessages) {
                undeliveredMessages.addLast(new UndeliveredAjaxMessage(message, consumer));
            }
        }
    }
}
