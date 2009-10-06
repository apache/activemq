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
package org.apache.activemq.transport.xmpp;

import junit.framework.TestCase;
import junit.textui.TestRunner;
import org.jivesoftware.smack.Chat;
import org.jivesoftware.smack.ConnectionConfiguration;
import org.jivesoftware.smack.MessageListener;
import org.jivesoftware.smack.XMPPConnection;
import org.jivesoftware.smack.XMPPException;
import org.jivesoftware.smack.packet.Message;

/**
 * @version $Revision$
 */
public class XmppTest extends TestCase {

    protected static boolean block;

    private XmppBroker broker = new XmppBroker();

    public static void main(String[] args) {
        block = true;
        TestRunner.run(XmppTest.class);
    }

    public void testConnect() throws Exception {
        ConnectionConfiguration config = new
            ConnectionConfiguration("localhost", 61222);
        // config.setDebuggerEnabled(true);

        try {
            // SmackConfiguration.setPacketReplyTimeout(1000);
            XMPPConnection con = new XMPPConnection(config);
            con.connect(); 
            con.login("amq-user", "amq-pwd");
            Chat chat = con.getChatManager().createChat("test@localhost",
                new MessageListener() {
                    public void processMessage(Chat chat, Message message) {
                        //
                    }
                });
            for (int i = 0; i < 10; i++) {
                System.out.println("Sending message: " + i);
                chat.sendMessage("Hello from Message: " + i);
            }
            System.out.println("Sent all messages!");
            con.disconnect();
        } catch (XMPPException e) {
            if (block) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            } else {
                throw e;
            }
        }
        if (block) {
            Thread.sleep(20000);
            System.out.println("Press any key to quit!: ");
            System.in.read();
        }
        System.out.println("Done!");
    }

    @Override
    protected void setUp() throws Exception {
        broker.start();
    }

    @Override
    protected void tearDown() throws Exception {
        broker.stop();
    }
}
