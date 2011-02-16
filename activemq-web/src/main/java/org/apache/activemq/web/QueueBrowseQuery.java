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

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.springframework.beans.factory.DisposableBean;

/**
 * 
 * 
 */
public class QueueBrowseQuery extends DestinationFacade implements DisposableBean {
    private SessionPool sessionPool;
    private String selector;
    private Session session;
    private Queue queue;
    private QueueBrowser browser;

    public QueueBrowseQuery(BrokerFacade brokerFacade, SessionPool sessionPool) throws JMSException {
        super(brokerFacade);
        this.sessionPool = sessionPool;
        this.session = sessionPool.borrowSession();
        setJMSDestinationType("query");
    }

    public void destroy() throws Exception {
        if (browser != null) {
            browser.close();
        }
        sessionPool.returnSession(session);
        session = null;
    }

    public QueueBrowser getBrowser() throws JMSException {
        if (browser == null) {
            browser = createBrowser();
        }
        return browser;
    }

    public void setBrowser(QueueBrowser browser) {
        this.browser = browser;
    }

    public Queue getQueue() throws JMSException {
        if (queue == null) {
            queue = session.createQueue(getValidDestination());
        }
        return queue;
    }

    public void setQueue(Queue queue) {
        this.queue = queue;
    }

    public String getSelector() {
        return selector;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    public Session getSession() {
        return session;
    }

    public boolean isQueue() {
        return true;
    }

    protected QueueBrowser createBrowser() throws JMSException {
        return getSession().createBrowser(getQueue(), getSelector());
    }

    
}
