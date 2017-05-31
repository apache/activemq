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
package org.apache.activemq.console.command;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.util.ConsumerThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.Session;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ConsumerCommand extends AbstractCommand {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerCommand.class);

    String brokerUrl = ActiveMQConnectionFactory.DEFAULT_BROKER_URL;
    String user = ActiveMQConnectionFactory.DEFAULT_USER;
    String password = ActiveMQConnectionFactory.DEFAULT_PASSWORD;
    String destination = "queue://TEST";
    int messageCount = 1000;
    int sleep;
    boolean transacted;
    private boolean durable;
    private String clientId;
    int batchSize = 10;
    int ackMode = Session.AUTO_ACKNOWLEDGE;
    int parallelThreads = 1;
    boolean bytesAsText;

    @Override
    protected void runTask(List<String> tokens) throws Exception {
        LOG.info("Connecting to URL: " + brokerUrl + " as user: " + user);
        LOG.info("Consuming " + destination);
        LOG.info("Sleeping between receives " + sleep + " ms");
        LOG.info("Running " + parallelThreads + " parallel threads");

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        Connection conn = null;
        try {
            conn = factory.createConnection(user, password);
            if (durable && clientId != null && clientId.length() > 0 && !"null".equals(clientId)) {
                conn.setClientID(clientId);
            }
            conn.start();


            CountDownLatch active = new CountDownLatch(parallelThreads);

            for (int i = 1; i <= parallelThreads; i++) {
                Session sess;
                if (transacted) {
                   sess = conn.createSession(true, Session.SESSION_TRANSACTED);
                } else {
                   sess = conn.createSession(false, ackMode);
                }
                ConsumerThread consumer = new ConsumerThread(sess, ActiveMQDestination.createDestination(destination, ActiveMQDestination.QUEUE_TYPE));
                consumer.setName("consumer-" + i);
                consumer.setDurable(durable);
                consumer.setBreakOnNull(false);
                consumer.setMessageCount(messageCount);
                consumer.setSleep(sleep);
                consumer.setBatchSize(batchSize);
                consumer.setFinished(active);
                consumer.setBytesAsText(bytesAsText);
                consumer.start();
            }

            active.await();
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public String getBrokerUrl() {
        return brokerUrl;
    }

    public void setBrokerUrl(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(int messageCount) {
        this.messageCount = messageCount;
    }

    public int getSleep() {
        return sleep;
    }

    public void setSleep(int sleep) {
        this.sleep = sleep;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getParallelThreads() {
        return parallelThreads;
    }

    public void setParallelThreads(int parallelThreads) {
        this.parallelThreads = parallelThreads;
    }

    public boolean isBytesAsText() {
        return bytesAsText;
    }

    public void setBytesAsText(boolean bytesAsText) {
        this.bytesAsText = bytesAsText;
    }

    public boolean isTransacted() {
        return transacted;
    }

    public void setTransacted(boolean transacted) {
        this.transacted = transacted;
    }

    public int getAckMode() {
        return ackMode;
    }

    public void setAckMode(String ackMode) {
        if ("CLIENT_ACKNOWLEDGE".equals(ackMode)) {
            this.ackMode = Session.CLIENT_ACKNOWLEDGE;
        }
        if ("AUTO_ACKNOWLEDGE".equals(ackMode)) {
            this.ackMode = Session.AUTO_ACKNOWLEDGE;
        }
        if ("DUPS_OK_ACKNOWLEDGE".equals(ackMode)) {
            this.ackMode = Session.DUPS_OK_ACKNOWLEDGE;
        }
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    @Override
    protected void printHelp() {
        printHelpFromFile();
    }

    @Override
    public String getName() {
        return "consumer";
    }

    @Override
    public String getOneLineDescription() {
        return "Receives messages from the broker";
    }
}
