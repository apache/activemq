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
    public void runJmsClient(String clientName, Properties clientSettings) {
        PerfMeasurementTool sampler = getPerformanceSampler();

        JmsProducerClient producer = new JmsProducerClient();
        producer.setSettings(clientSettings);

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

    public String getClientName() {
        return "JMS Producer: ";
    }

    public String getThreadName() {
        return "JMS Producer Thread: ";
    }

    public String getThreadGroupName() {
        return "JMS Producer Thread Group";
    }

    public static void main(String[] args) {
        /*String[] options = new String[19];
        options[0] = "-Dsampler.duration=60000";     // 1 min
        options[1] = "-Dsampler.interval=5000";      // 5 secs
        options[2] = "-Dsampler.rampUpTime=10000";   // 10 secs
        options[3] = "-Dsampler.rampDownTime=10000"; // 10 secs

        options[4] = "-Dclient.spiClass=org.apache.activemq.tool.spi.ActiveMQPojoSPI";
        options[5] = "-Dclient.sessTransacted=false";
        options[6] = "-Dclient.sessAckMode=autoAck";
        options[7] = "-Dclient.destName=topic://FOO.BAR.TEST";
        options[8] = "-Dclient.destCount=1";
        options[9] = "-Dclient.destComposite=false";

        options[10] = "-Dproducer.messageSize=1024";
        options[11] = "-Dproducer.sendCount=1000";     // 1000 messages
        options[12] = "-Dproducer.sendDuration=60000"; // 1 min
        options[13] = "-Dproducer.sendType=time";

        options[14] = "-Dfactory.brokerUrl=tcp://localhost:61616";
        options[15] = "-Dfactory.asyncSend=true";

        options[16] = "-DsysTest.numClients=5";
        options[17] = "-DsysTest.totalDests=5";
        options[18] = "-DsysTest.destDistro=all";

        args = options;*/

        Properties sysSettings  = new Properties();

        for (int i=0; i<args.length; i++) {
            // Get property define options only
            if (args[i].startsWith("-D")) {
                String propDefine = args[i].substring("-D".length());
                int  index = propDefine.indexOf("=");
                String key = propDefine.substring(0, index);
                String val = propDefine.substring(index+1);
                sysSettings.setProperty(key, val);
            }
        }

        JmsProducerSystem sysTest = new JmsProducerSystem();
        sysTest.setReportDirectory("./target/Test-perf");
        sysTest.setSettings(sysSettings);
        sysTest.runSystemTest();
    }
}
