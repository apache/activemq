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
    public void runJmsClient(String clientName, Properties clientSettings) {
        PerfMeasurementTool sampler = getPerformanceSampler();

        JmsConsumerClient consumer = new JmsConsumerClient();
        consumer.setSettings(clientSettings);
        consumer.setConsumerName(clientName); // For durable subscribers

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

    public String getClientName() {
        return "JMS Consumer: ";
    }

    public String getThreadName() {
        return "JMS Consumer Thread: ";
    }

    public String getThreadGroupName() {
        return "JMS Consumer Thread Group";
    }

    public static void main(String[] args) throws JMSException {
        String[] options = new String[22];
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

        options[10] = "-Dconsumer.durable=false";
        options[11] = "-Dconsumer.asyncRecv=true";
        options[12] = "-Dconsumer.recvCount=1000";     // 1000 messages
        options[13] = "-Dconsumer.recvDuration=60000"; // 1 min
        options[14] = "-Dconsumer.recvType=time";

        options[15] = "-Dfactory.brokerUrl=tcp://localhost:61616";
        options[16] = "-Dfactory.optimAck=true";
        options[17] = "-Dfactory.optimDispatch=true";
        options[18] = "-Dfactory.prefetchQueue=10";
        options[19] = "-Dfactory.prefetchTopic=10";
        options[20] = "-Dfactory.useRetroactive=false";

        options[21] = "-DsysTest.numClients=5";

        args = options;

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

        JmsConsumerSystem sysTest = new JmsConsumerSystem();
        sysTest.setSettings(sysSettings);
        sysTest.runSystemTest();
    }
}
