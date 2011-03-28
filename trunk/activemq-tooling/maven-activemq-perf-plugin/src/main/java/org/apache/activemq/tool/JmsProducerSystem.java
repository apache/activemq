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
package org.apache.activemq.tool;

import javax.jms.JMSException;

import org.apache.activemq.tool.properties.JmsClientProperties;
import org.apache.activemq.tool.properties.JmsClientSystemProperties;
import org.apache.activemq.tool.properties.JmsProducerProperties;
import org.apache.activemq.tool.properties.JmsProducerSystemProperties;
import org.apache.activemq.tool.sampler.ThroughputSamplerTask;

public class JmsProducerSystem extends AbstractJmsClientSystem {
    protected JmsProducerSystemProperties sysTest = new JmsProducerSystemProperties();
    protected JmsProducerProperties producer = new JmsProducerProperties();

    public JmsClientSystemProperties getSysTest() {
        return sysTest;
    }

    public void setSysTest(JmsClientSystemProperties sysTestProps) {
        sysTest = (JmsProducerSystemProperties)sysTestProps;
    }

    public JmsClientProperties getJmsClientProperties() {
        return getProducer();
    }

    public JmsProducerProperties getProducer() {
        return producer;
    }

    public void setProducer(JmsProducerProperties producer) {
        this.producer = producer;
    }

    protected void runJmsClient(String clientName, int clientDestIndex, int clientDestCount) {
        ThroughputSamplerTask sampler = getTpSampler();

        JmsProducerClient producerClient = new JmsProducerClient(producer, jmsConnFactory);
        producerClient.setClientName(clientName);

        if (sampler != null) {
            sampler.registerClient(producerClient);
        }

        try {
            producerClient.sendMessages(clientDestIndex, clientDestCount);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        JmsProducerSystem sys = new JmsProducerSystem();
        sys.configureProperties(AbstractJmsClientSystem.parseStringArgs(args));

        try {
            sys.runSystemTest();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
