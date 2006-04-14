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
package org.apache.activemq.transport.stomp;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;
import java.net.ProtocolException;
import java.util.Properties;

import javax.jms.JMSException;

import org.apache.activeio.packet.ByteSequence;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.TransactionId;

class Send implements StompCommand {
    private final HeaderParser parser = new HeaderParser();
    private final StompWireFormat format;

    Send(StompWireFormat format) {
        this.format = format;
    }

    public CommandEnvelope build(String commandLine, DataInput in) throws IOException, JMSException {
        Properties headers = parser.parse(in);
        String destination = (String) headers.remove(Stomp.Headers.Send.DESTINATION);
        // now the body
        ActiveMQMessage msg;
        if (headers.containsKey(Stomp.Headers.CONTENT_LENGTH)) {
            ActiveMQBytesMessage bm = new ActiveMQBytesMessage();
            String content_length = headers.getProperty(Stomp.Headers.CONTENT_LENGTH).trim();
            int length;
            try {
                length = Integer.parseInt(content_length);
            }
            catch (NumberFormatException e) {
                throw new ProtocolException("Specified content-length is not a valid integer");
            }
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            byte nil = in.readByte();
            if (nil != 0)
                throw new ProtocolException("content-length bytes were read and " + "there was no trailing null byte");
            ByteSequence content = new ByteSequence(bytes, 0, bytes.length);
            bm.setContent(content);
            msg = bm;
        }
        else {
            ActiveMQTextMessage text = new ActiveMQTextMessage();
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            byte b;
            while ((b = in.readByte()) != 0) {
                bytes.write(b);
            }
            bytes.close();
            String body = new String(bytes.toByteArray());
            try {
                text.setText(body);
            }
            catch (JMSException e) {
                throw new RuntimeException("Something is really wrong, we instantiated this thing!");
            }
            msg = text;
        }

        msg.setProducerId(format.getProducerId());
        msg.setMessageId(format.createMessageId());
        

        ActiveMQDestination d = DestinationNamer.convert(destination);
        msg.setDestination(d);
        // msg.setJMSClientID(format.getClientId());

        // the standard JMS headers
        msg.setJMSCorrelationID((String) headers.remove(Stomp.Headers.Send.CORRELATION_ID));

        Object expiration = headers.remove(Stomp.Headers.Send.EXPIRATION_TIME);
        if (expiration != null) {
            msg.setJMSExpiration(asLong(expiration));
        }
        Object priority = headers.remove(Stomp.Headers.Send.PRIORITY);
        if (priority != null) {
            msg.setJMSPriority(asInt(priority));
        }
        Object type = headers.remove(Stomp.Headers.Send.TYPE);
        if (type != null) {
            msg.setJMSType((String) type);
        }

        msg.setJMSReplyTo(DestinationNamer.convert((String) headers.remove(Stomp.Headers.Send.REPLY_TO)));

        Object persistent = headers.remove(Stomp.Headers.Send.PERSISTENT);
        if (persistent != null) {
            msg.setPersistent(asBool(persistent));
        }
        
        // No need to carry the content length in the JMS headers.
        headers.remove(Stomp.Headers.CONTENT_LENGTH);
        
        // now the general headers
        msg.setProperties(headers);

        if (headers.containsKey(Stomp.Headers.TRANSACTION)) {
            TransactionId tx_id = format.getTransactionId(headers.getProperty(Stomp.Headers.TRANSACTION));
            if (tx_id == null)
                throw new ProtocolException(headers.getProperty(Stomp.Headers.TRANSACTION) + " is an invalid transaction id");
            msg.setTransactionId(tx_id);
        }
        
        msg.setReadOnlyBody(true);
        msg.setReadOnlyProperties(true);

        return new CommandEnvelope(msg, headers);
    }

    protected boolean asBool(Object value) {
        if (value != null) {
            return String.valueOf(value).equals("true");
        }
        return false;
    }

    protected long asLong(Object value) {
        if (value instanceof Number) {
            Number n = (Number) value;
            return n.longValue();
        }
        return Long.parseLong(value.toString());
    }

    protected int asInt(Object value) {
        if (value instanceof Number) {
            Number n = (Number) value;
            return n.intValue();
        }
        return Integer.parseInt(value.toString());
    }
}
