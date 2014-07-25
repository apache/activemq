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

package org.apache.activemq.bugs;

import java.io.IOException;
import java.util.Map;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.util.ByteSequence;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ4893Test {

    private static final transient Logger LOG = LoggerFactory.getLogger(AMQ4893Test.class);

    @Test
    public void testPropertiesInt() throws Exception {
        ActiveMQObjectMessage message = new ActiveMQObjectMessage();
        message.setIntProperty("TestProp", 333);
        fakeUnmarshal(message);
        roundTripProperties(message);
    }

    @Test
    public void testPropertiesString() throws Exception {
        ActiveMQObjectMessage message = new ActiveMQObjectMessage();
        message.setStringProperty("TestProp", "Value");
        fakeUnmarshal(message);
        roundTripProperties(message);
    }

    @Test
    public void testPropertiesObject() throws Exception {
        ActiveMQObjectMessage message = new ActiveMQObjectMessage();
        message.setObjectProperty("TestProp", "Value");
        fakeUnmarshal(message);
        roundTripProperties(message);
    }

    @Test
    public void testPropertiesObjectNoMarshalling() throws Exception {
        ActiveMQObjectMessage message = new ActiveMQObjectMessage();
        message.setObjectProperty("TestProp", "Value");
        roundTripProperties(message);
    }

    private void roundTripProperties(ActiveMQObjectMessage message) throws IOException, JMSException {
        ActiveMQObjectMessage copy = new ActiveMQObjectMessage();
        for (Map.Entry<String, Object> prop : message.getProperties().entrySet()) {
            LOG.debug("{} -> {}", prop.getKey(), prop.getValue().getClass());
            copy.setObjectProperty(prop.getKey(), prop.getValue());
        }
    }

    private void fakeUnmarshal(ActiveMQObjectMessage message) throws IOException {
        // we need to force the unmarshalled property field to be set so it
        // gives us a hawtbuffer for the string
        OpenWireFormat format = new OpenWireFormat();
        message.beforeMarshall(format);
        message.afterMarshall(format);

        ByteSequence seq = message.getMarshalledProperties();
        message.clearProperties();
        message.setMarshalledProperties(seq);
    }
}