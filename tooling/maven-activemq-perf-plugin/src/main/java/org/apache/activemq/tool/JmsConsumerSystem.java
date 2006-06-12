/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.tool;

import javax.jms.JMSException;
import java.util.Properties;

public class JmsConsumerSystem extends JmsClientSystemSupport {

    public String getReportName() {
        if (reportName == null) {
            return "JmsConsumer_ClientCount" + getNumClients() + "_DestCount" + getTotalDests() + "_" + getDestDistro() + ".xml";
        } else {
            return reportName;
        }
    }

    public String getClientName() {
        if (clientName == null) {
            return "JmsConsumer";
        } else {
            return clientName;
        }
    }

    protected void runJmsClient(String clientName, Properties clientSettings) {
        PerfMeasurementTool sampler = getPerformanceSampler();

        JmsConsumerClient consumer = new JmsConsumerClient();
        consumer.setSettings(clientSettings);
        consumer.setConsumerName(clientName); // For durable subscribers
        consumer.setClientName(clientName);

        if (sampler != null) {
            sampler.registerClient(consumer);
            consumer.setPerfEventListener(sampler);
        }

        try {
            consumer.receiveMessages();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    protected String getThreadName() {
        return "JMS Consumer Thread: ";
    }

    protected String getThreadGroupName() {
        return "JMS Consumer Thread Group";
    }

    protected String getDestCountKey() {
        return "consumer.destCount";
    }

    protected String getDestIndexKey() {
        return "consumer.destIndex";
    }

    public static void main(String[] args) throws JMSException {
        Properties sysSettings = new Properties();
        for (int i = 0; i < args.length; i++) {
            // Get property define options only
            int index = args[i].indexOf("=");
            String key = args[i].substring(0, index);
            String val = args[i].substring(index + 1);
            sysSettings.setProperty(key, val);
        }

        JmsConsumerSystem sysTest = new JmsConsumerSystem();
        sysTest.setSettings(sysSettings);
        sysTest.runSystemTest();
    }
}
