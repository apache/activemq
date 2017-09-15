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
package org.apache.activemq.command;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.util.ByteSequence;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test to make sure message.isMarshalled() returns the correct value
 */
@RunWith(Parameterized.class)
public class ActiveMQMessageIsMarshalledTest {

    protected enum MessageType {BYTES, MAP, TEXT, OBJECT, STREAM, MESSAGE}

    private final MessageType messageType;

    @Parameters(name="messageType={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {MessageType.BYTES},
                {MessageType.MAP},
                {MessageType.TEXT},
                {MessageType.OBJECT},
                {MessageType.STREAM},
                {MessageType.MESSAGE}
            });
    }

    public ActiveMQMessageIsMarshalledTest(final MessageType messageType) {
        super();
        this.messageType = messageType;
    }

    @Test
    public void testIsMarshalledWithBodyAndProperties() throws Exception {
        ActiveMQMessage message = getMessage(true, true);
        assertIsMarshalled(message, true, true);
    }

    @Test
    public void testIsMarshalledWithPropertyEmptyBody() throws Exception {
        ActiveMQMessage message = getMessage(false, true);
        assertIsMarshalled(message, false, true);
    }

    @Test
    public void testIsMarshalledWithBodyEmptyProperties() throws Exception {
        ActiveMQMessage message = getMessage(true, false);
        assertIsMarshalled(message, true, false);
    }

    @Test
    public void testIsMarshalledWithEmptyBodyEmptyProperties() throws Exception {
        ActiveMQMessage message = getMessage(false, false);

        //No body or properties so the message should be considered marshalled already
        assertTrue(message.isMarshalled());
    }

    private ActiveMQMessage getMessage(boolean includeBody, boolean includeProperties) throws Exception {
        if (MessageType.BYTES == messageType) {
            return getBytesMessage(includeBody, includeProperties);
        } else if (MessageType.TEXT == messageType) {
            return getTextMessage(includeBody, includeProperties);
        } else if (MessageType.MAP == messageType) {
            return getMapMessage(includeBody, includeProperties);
        } else if (MessageType.OBJECT == messageType) {
            return getObjectMessage(includeBody, includeProperties);
        } else if (MessageType.STREAM == messageType) {
            return getStreamMessage(includeBody, includeProperties);
        } else if (MessageType.MESSAGE == messageType) {
            return getActiveMQMessage(includeBody, includeProperties);
        }

        return null;
    }

    private ActiveMQBytesMessage getBytesMessage(boolean includeBody, boolean includeProperties) throws Exception {
        ActiveMQBytesMessage message = new ActiveMQBytesMessage();
        if (includeBody) {
            message.writeBytes(new byte[10]);
        }
        if (includeProperties) {
            message.setProperty("test", "test");
        }
        return message;
    }

    private ActiveMQMapMessage getMapMessage(boolean includeBody, boolean includeProperties) throws Exception {
        ActiveMQMapMessage message = new ActiveMQMapMessage();
        if (includeBody) {
            message.setString("stringbody", "stringbody");
        }
        if (includeProperties) {
            message.setProperty("test", "test");
        }
        return message;
    }

    private ActiveMQTextMessage getTextMessage(boolean includeBody, boolean includeProperties) throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        if (includeBody) {
            message.setText("test");
        }
        if (includeProperties) {
            message.setProperty("test", "test");
        }
        return message;
    }

    private ActiveMQObjectMessage getObjectMessage(boolean includeBody, boolean includeProperties) throws Exception {
        ActiveMQObjectMessage message = new ActiveMQObjectMessage();
        ActiveMQConnection con = ActiveMQConnection.makeConnection();
        con.setObjectMessageSerializationDefered(true);
        message.setConnection(con);
        if (includeBody) {
            message.setObject("test");
        }
        if (includeProperties) {
            message.setProperty("test", "test");
        }
        return message;
    }

    private ActiveMQStreamMessage getStreamMessage(boolean includeBody, boolean includeProperties) throws Exception {
        ActiveMQStreamMessage message = new ActiveMQStreamMessage();
        if (includeBody) {
            message.writeBytes(new byte[10]);
        }
        if (includeProperties) {
            message.setProperty("test", "test");
        }
        return message;
    }

    private ActiveMQMessage getActiveMQMessage(boolean includeBody, boolean includeProperties) throws Exception {
        ActiveMQMessage message = new ActiveMQMessage();
        if (includeBody) {
            message.setContent(new ByteSequence(new byte[10]));
        }
        if (includeProperties) {
            message.setProperty("test", "test");
        }
        return message;
    }

    private void assertIsMarshalled(final ActiveMQMessage message, boolean includeBody, boolean includeProperties) throws Exception {
        if (ActiveMQMessage.class.equals(message.getClass())) {
            //content is either not set or already marshalled for ActiveMQMessage so this only
            //relies on
            assertFalse(message.isMarshalled() == includeProperties);
        } else {
            assertFalse(message.isMarshalled());
            message.onSend();
            message.beforeMarshall(new OpenWireFormat());
            assertTrue(message.isMarshalled());
            assertTrue(message.getMarshalledProperties() != null == includeProperties);
            assertTrue(message.getContent() != null == includeBody);
        }
    }

}
