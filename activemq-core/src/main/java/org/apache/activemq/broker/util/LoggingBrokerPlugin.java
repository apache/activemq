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
package org.apache.activemq.broker.util;

import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A simple Broker interceptor which allows you to enable/disable logging.
 * 
 * @org.apache.xbean.XBean
 * @version $Revision$
 */
public class LoggingBrokerPlugin extends BrokerPluginSupport {

    private Log log = LogFactory.getLog(LoggingBrokerPlugin.class);
    private Log sendLog = LogFactory.getLog(LoggingBrokerPlugin.class.getName() + ".Send");
    private Log ackLog = LogFactory.getLog(LoggingBrokerPlugin.class.getName() + ".Ack");

    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        if (sendLog.isInfoEnabled()) {
            sendLog.info("Sending: " + messageSend);
        }
        super.send(producerExchange, messageSend);
    }

    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        if (ackLog.isInfoEnabled()) {
            ackLog.info("Acknowledge: " + ack);
        }
        super.acknowledge(consumerExchange, ack);
    }

    // Properties
    // -------------------------------------------------------------------------
    public Log getAckLog() {
        return ackLog;
    }

    public void setAckLog(Log ackLog) {
        this.ackLog = ackLog;
    }

    public Log getLog() {
        return log;
    }

    public void setLog(Log log) {
        this.log = log;
    }

    public Log getSendLog() {
        return sendLog;
    }

    public void setSendLog(Log sendLog) {
        this.sendLog = sendLog;
    }

}
