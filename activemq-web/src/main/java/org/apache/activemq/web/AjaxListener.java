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

import javax.jms.Message;
import javax.jms.MessageConsumer;

import org.eclipse.jetty.continuation.Continuation;
import org.eclipse.jetty.continuation.ContinuationSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.activemq.MessageAvailableListener;

/*
 * Listen for available messages and wakeup any continuations.
 */
public class AjaxListener implements MessageAvailableListener {
    private static final Log LOG = LogFactory.getLog(AjaxListener.class);
    
    private long maximumReadTimeout;
    private AjaxWebClient client;
    private long lastAccess;
    private Continuation continuation;

    AjaxListener(AjaxWebClient client, long maximumReadTimeout) {
        this.client = client;
        this.maximumReadTimeout = maximumReadTimeout;
    }

    public void access() {
        lastAccess = System.currentTimeMillis();
    }

    public synchronized void setContinuation(Continuation continuation) {
        this.continuation = continuation;
    }

    public synchronized void onMessageAvailable(MessageConsumer consumer) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("message for " + consumer + "continuation=" + continuation);
        }
        if (continuation != null) {
            try {
                Message message = consumer.receive(10);
                continuation.setAttribute("message", message);
                continuation.setAttribute("consumer", consumer);
            } catch (Exception e) {
                LOG.error("Error receiving message " + e, e);
            }
            continuation.resume();
        } else if (System.currentTimeMillis() - lastAccess > 2 * this.maximumReadTimeout) {
            new Thread() {
                public void run() {
                    client.closeConsumers();
                };
            }.start();
        }
    }
}
