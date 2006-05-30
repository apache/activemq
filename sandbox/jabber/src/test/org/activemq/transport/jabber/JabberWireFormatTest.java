/** 
 * 
 * Copyright 2004 Protique Ltd
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
 * 
 **/
package org.activemq.transport.jabber;

import junit.framework.TestCase;
import org.activemq.io.WireFormat;
import org.activemq.message.ActiveMQTextMessage;
import org.activemq.message.ActiveMQTopic;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

/**
 * @version $Revision: 1.1 $
 */
public class JabberWireFormatTest extends TestCase {
    protected WireFormat format = new JabberWireFormat();

    public void testWrite() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setJMSType("id");
        message.setJMSReplyTo(new ActiveMQTopic("my.source"));
        message.setJMSDestination(new ActiveMQTopic("my.target"));
        message.setJMSCorrelationID("abc123");
        message.setText("<body>hello there </body>");

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        format.writePacket(message, out);
        out.close();

        byte[] data = buffer.toByteArray();
        System.out.println(new String(data));
    }
}
