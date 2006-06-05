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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.MessageProducer;
import javax.jms.Destination;
import javax.jms.TextMessage;
import javax.jms.JMSException;
import java.util.Properties;
import java.util.Arrays;

public class JmsProducerClient extends JmsPerformanceSupport {
    private static final Log log = LogFactory.getLog(JmsProducerClient.class);

    private static final String PREFIX_CONFIG_PRODUCER = "producer.";
    public  static final String TIME_BASED_SENDING  = "time";
    public  static final String COUNT_BASED_SENDING = "count";

    protected Properties      jmsProducerSettings = new Properties();
    protected MessageProducer jmsProducer;
    protected TextMessage     jmsTextMessage;

    protected int    messageSize  = 1024;          // Send 1kb messages by default
    protected long   sendCount    = 1000000;       // Send a million messages by default
    protected long   sendDuration = 5 * 60 * 1000; // Send for 5 mins by default
    protected String sendType     = TIME_BASED_SENDING;

    public void sendMessages() throws JMSException {
        if (listener != null) {
            listener.onConfigEnd(this);
        }
        // Send a specific number of messages
        if (sendType.equalsIgnoreCase(COUNT_BASED_SENDING)) {
            sendCountBasedMessages(getSendCount());

        // Send messages for a specific duration
        } else {
            sendTimeBasedMessages(getSendDuration());
        }
    }

    public void sendCountBasedMessages(long messageCount) throws JMSException {
        // Parse through different ways to send messages
        // Avoided putting the condition inside the loop to prevent effect on performance
        Destination[] dest = createDestination();

        // Create a producer, if none is created.
        if (getJmsProducer() == null) {
            if (dest.length == 1) {
                createJmsProducer(dest[0]);
            } else {
                createJmsProducer();
            }
        }
        try {
            getConnection().start();
            if (listener != null) {
                listener.onPublishStart(this);
            }
            // Send one type of message only, avoiding the creation of different messages on sending
            if (getJmsTextMessage() != null) {
                // Send to more than one actual destination
                if (dest.length > 1) {
                    for (int i=0; i<messageCount; i++) {
                        for (int j=0; j<dest.length; j++) {
                            getJmsProducer().send(dest[j], getJmsTextMessage());
                            incThroughput();
                        }
                    }
                // Send to only one actual destination
                } else {
                    for (int i=0; i<messageCount; i++) {
                        getJmsProducer().send(getJmsTextMessage());
                        incThroughput();
                    }
                }

            // Send different type of messages using indexing to identify each one.
            // Message size will vary. Definitely slower, since messages properties
            // will be set individually each send.
            } else {
                // Send to more than one actual destination
                if (dest.length > 1) {
                    for (int i=0; i<messageCount; i++) {
                        for (int j=0; j<dest.length; j++) {
                            getJmsProducer().send(dest[j], createJmsTextMessage("Text Message [" + i + "]"));
                            incThroughput();
                        }
                    }

                // Send to only one actual destination
                } else {
                    for (int i=0; i<messageCount; i++) {
                        getJmsProducer().send(createJmsTextMessage("Text Message [" + i + "]"));
                        incThroughput();
                    }
                }
            }
        } finally {
            if (listener != null) {
                listener.onPublishEnd(this);
            }
            getConnection().close();
        }
    }

    public void sendTimeBasedMessages(long duration) throws JMSException {
        long endTime   = System.currentTimeMillis() + duration;
        // Parse through different ways to send messages
        // Avoided putting the condition inside the loop to prevent effect on performance

        Destination[] dest = createDestination();

        // Create a producer, if none is created.
        if (getJmsProducer() == null) {
            if (dest.length == 1) {
                createJmsProducer(dest[0]);
            } else {
                createJmsProducer();
            }
        }

        try {
            getConnection().start();
            if (listener != null) {
                listener.onPublishStart(this);
            }

            // Send one type of message only, avoiding the creation of different messages on sending
            if (getJmsTextMessage() != null) {
                // Send to more than one actual destination
                if (dest.length > 1) {
                    while (System.currentTimeMillis() < endTime) {
                        for (int j=0; j<dest.length; j++) {
                            getJmsProducer().send(dest[j], getJmsTextMessage());
                            incThroughput();
                        }
                    }
                // Send to only one actual destination
                } else {
                    while (System.currentTimeMillis() < endTime) {
                        getJmsProducer().send(getJmsTextMessage());
                        incThroughput();
                    }
                }

            // Send different type of messages using indexing to identify each one.
            // Message size will vary. Definitely slower, since messages properties
            // will be set individually each send.
            } else {
                // Send to more than one actual destination
                long count = 1;
                if (dest.length > 1) {
                    while (System.currentTimeMillis() < endTime) {
                        for (int j=0; j<dest.length; j++) {
                            getJmsProducer().send(dest[j], createJmsTextMessage("Text Message [" + count++ + "]"));
                            incThroughput();
                        }
                    }

                // Send to only one actual destination
                } else {
                    while (System.currentTimeMillis() < endTime) {

                        getJmsProducer().send(createJmsTextMessage("Text Message [" + count++ + "]"));
                        incThroughput();
                    }
                }
            }
        } finally {
            if (listener != null) {
                listener.onPublishEnd(this);
            }
            getConnection().close();
        }
    }

    public Properties getJmsProducerSettings() {
        return jmsProducerSettings;
    }

    public void setJmsProducerSettings(Properties jmsProducerSettings) {
        this.jmsProducerSettings = jmsProducerSettings;
        ReflectionUtil.configureClass(this, jmsProducerSettings);
    }

    public MessageProducer createJmsProducer() throws JMSException {
        jmsProducer = getSession().createProducer(null);
        return jmsProducer;
    }

    public MessageProducer createJmsProducer(Destination dest) throws JMSException {
        jmsProducer = getSession().createProducer(dest);
        return jmsProducer;
    }

    public MessageProducer getJmsProducer() {
        return jmsProducer;
    }

    public TextMessage createJmsTextMessage() throws JMSException {
        return createJmsTextMessage(getMessageSize());
    }

    public TextMessage createJmsTextMessage(int size) throws JMSException {
        jmsTextMessage = getSession().createTextMessage(buildText("", size));
        return jmsTextMessage;
    }

    public TextMessage createJmsTextMessage(String text) throws JMSException {
        jmsTextMessage = getSession().createTextMessage(buildText(text, getMessageSize()));
        return jmsTextMessage;
    }

    public TextMessage getJmsTextMessage() {
        return jmsTextMessage;
    }

    protected String buildText(String text, int size) {
        byte[] data = new byte[size - text.length()];
        Arrays.fill(data, (byte)0);
        return text + new String(data);
    }

    public int getMessageSize() {
        return messageSize;
    }

    public void setMessageSize(int messageSize) {
        this.messageSize = messageSize;
    }

    public long getSendCount() {
        return sendCount;
    }

    public void setSendCount(long sendCount) {
        this.sendCount = sendCount;
    }

    public long getSendDuration() {
        return sendDuration;
    }

    public void setSendDuration(long sendDuration) {
        this.sendDuration = sendDuration;
    }

    public String getSendType() {
        return sendType;
    }

    public void setSendType(String sendType) {
        this.sendType = sendType;
    }

    public Properties getSettings() {
        Properties allSettings = new Properties(jmsProducerSettings);
        allSettings.putAll(super.getSettings());
        return allSettings;
    }

    public void setSettings(Properties settings) {
        super.setSettings(settings);
        ReflectionUtil.configureClass(this, jmsProducerSettings);
    }

    public void setProperty(String key, String value) {
        if (key.startsWith(PREFIX_CONFIG_PRODUCER)) {
            jmsProducerSettings.setProperty(key, value);
        } else {
            super.setProperty(key, value);
        }
    }

    public static void main(String[] args) throws JMSException {
        /*String[] options = new String[16];
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

        args = options;*/

        Properties samplerSettings  = new Properties();
        Properties producerSettings = new Properties();

        for (int i=0; i<args.length; i++) {
            // Get property define options only
            if (args[i].startsWith("-D")) {
                String propDefine = args[i].substring("-D".length());
                int  index = propDefine.indexOf("=");
                String key = propDefine.substring(0, index);
                String val = propDefine.substring(index+1);
                if (key.startsWith("sampler.")) {
                    samplerSettings.setProperty(key, val);
                } else {
                    producerSettings.setProperty(key, val);
                }
            }
        }

        JmsProducerClient client = new JmsProducerClient();
        client.setSettings(producerSettings);

        PerfMeasurementTool sampler = new PerfMeasurementTool();
        sampler.setSamplerSettings(samplerSettings);
        sampler.registerClient(client);
        sampler.startSampler();

        client.setPerfEventListener(sampler);

        // This will reuse only a single message every send, which will improve performance
        client.createJmsTextMessage();
        client.sendMessages();
    }
}
