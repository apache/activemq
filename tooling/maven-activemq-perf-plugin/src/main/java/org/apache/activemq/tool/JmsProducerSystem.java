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

public class JmsProducerSystem extends JmsClientSystemSupport {

    public String getReportName() {
        if (reportName == null) {
            return "JmsProducer_Client" + getNumClients() + "_Dest" + getTotalDests() + "_" + getDestDistro() + ".xml";
        } else {
            return reportName;
        }
    }

    public String getClientName() {
        if (clientName == null) {
            return "JmsProducer";
        } else {
            return clientName;
        }
    }

    protected void runJmsClient(String clientName, Properties clientSettings) {
        PerfMeasurementTool sampler = getPerformanceSampler();

        JmsProducerClient producer = new JmsProducerClient();
        producer.setSettings(clientSettings);
        producer.setClientName(clientName);

        if (sampler != null) {
            sampler.registerClient(producer);
            producer.setPerfEventListener(sampler);
        }

        try {
            producer.createJmsTextMessage();
            producer.sendMessages();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    protected String getThreadName() {
        return "JMS Producer Thread: ";
    }

    protected String getThreadGroupName() {
        return "JMS Producer Thread Group";
    }

    protected String getDestCountKey() {
        return "producer.destCount";
    }

    protected String getDestIndexKey() {
        return "producer.destIndex";
    }

    public static void main(String[] args) {
        Properties sysSettings = new Properties();

        for (int i = 0; i < args.length; i++) {
            // Get property define options only
            int index = args[i].indexOf("=");
            String key = args[i].substring(0, index);
            String val = args[i].substring(index + 1);
            sysSettings.setProperty(key, val);
        }

        JmsProducerSystem sysTest = new JmsProducerSystem();
        sysTest.setSettings(sysSettings);
        sysTest.runSystemTest();
    }
}
