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
import org.apache.activemq.util.ProducerThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.Session;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ProducerCommand extends AbstractCommand {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerCommand.class);

    String brokerUrl = ActiveMQConnectionFactory.DEFAULT_BROKER_URL;
    String user = ActiveMQConnectionFactory.DEFAULT_USER;
    String password = ActiveMQConnectionFactory.DEFAULT_PASSWORD;
    String destination = "queue://TEST";
    int messageCount = 1000;
    int sleep = 0;
    boolean persistent = true;
    String message = null;
    String payloadUrl = null;
    int messageSize = 0;
    int textMessageSize;
    long msgTTL = 0L;
    String msgGroupID=null;
    int transactionBatchSize;
    private int parallelThreads = 1;

    @Override
    protected void runTask(List<String> tokens) throws Exception {
        LOG.info("Connecting to URL: " + brokerUrl + " as user: " + user);
        LOG.info("Producing messages to " + destination);
        LOG.info("Using " + (persistent ? "persistent" : "non-persistent") + " messages");
        LOG.info("Sleeping between sends " + sleep + " ms");
        LOG.info("Running " + parallelThreads + " parallel threads");

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        Connection conn = null;
        try {
            conn = factory.createConnection(user, password);
            conn.start();

            CountDownLatch active = new CountDownLatch(parallelThreads);

            for (int i = 1; i <= parallelThreads; i++) {
                Session sess;
                if (transactionBatchSize != 0) {
                    sess = conn.createSession(true, Session.SESSION_TRANSACTED);
                } else {
                    sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                }
                ProducerThread producer = new ProducerThread(sess, ActiveMQDestination.createDestination(destination, ActiveMQDestination.QUEUE_TYPE));
                producer.setName("producer-" + i);
                producer.setMessageCount(messageCount);
                producer.setSleep(sleep);
                producer.setMsgTTL(msgTTL);
                producer.setPersistent(persistent);
                producer.setTransactionBatchSize(transactionBatchSize);
                producer.setMessage(message);
                producer.setPayloadUrl(payloadUrl);
                producer.setMessageSize(messageSize);
                producer.setMsgGroupID(msgGroupID);
                producer.setTextMessageSize(textMessageSize);
                producer.setFinished(active);
                producer.start();
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

    public boolean isPersistent() {
        return persistent;
    }

    public void setPersistent(boolean persistent) {
        this.persistent = persistent;
    }

    public int getMessageSize() {
        return messageSize;
    }

    public void setMessageSize(int messageSize) {
        this.messageSize = messageSize;
    }

    public int getTextMessageSize() {
        return textMessageSize;
    }

    public void setTextMessageSize(int textMessageSize) {
        this.textMessageSize = textMessageSize;
    }

    public long getMsgTTL() {
        return msgTTL;
    }

    public void setMsgTTL(long msgTTL) {
        this.msgTTL = msgTTL;
    }

    public String getMsgGroupID() {
        return msgGroupID;
    }

    public void setMsgGroupID(String msgGroupID) {
        this.msgGroupID = msgGroupID;
    }

    public int getTransactionBatchSize() {
        return transactionBatchSize;
    }

    public void setTransactionBatchSize(int transactionBatchSize) {
        this.transactionBatchSize = transactionBatchSize;
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

    public int getParallelThreads() {
        return parallelThreads;
    }

    public void setParallelThreads(int parallelThreads) {
        this.parallelThreads = parallelThreads;
    }

    public String getPayloadUrl() {
        return payloadUrl;
    }

    public void setPayloadUrl(String payloadUrl) {
        this.payloadUrl = payloadUrl;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    protected void printHelp() {
        printHelpFromFile();
    }

    @Override
    public String getName() {
        return "producer";
    }

    @Override
    public String getOneLineDescription() {
        return "Sends messages to the broker";
    }
}
