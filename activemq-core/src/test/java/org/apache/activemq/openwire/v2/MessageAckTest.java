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
package org.apache.activemq.openwire.v2;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.activemq.openwire.*;
import org.apache.activemq.command.*;


/**
 * Test case for the OpenWire marshalling for MessageAck
 *
 *
 * NOTE!: This file is auto generated - do not modify!
 *        if you need to make a change, please see the modify the groovy scripts in the
 *        under src/gram/script and then use maven openwire:generate to regenerate 
 *        this file.
 *
 * @version $Revision: $
 */
public class MessageAckTest extends BaseCommandTestSupport {


    public static MessageAckTest SINGLETON = new MessageAckTest();

    public Object createObject() throws Exception {
        MessageAck info = new MessageAck();
        populateObject(info);
        return info;
    }

    protected void populateObject(Object object) throws Exception {
        super.populateObject(object);
        MessageAck info = (MessageAck) object;

        info.setDestination(createActiveMQDestination("Destination:1"));
        info.setTransactionId(createTransactionId("TransactionId:2"));
        info.setConsumerId(createConsumerId("ConsumerId:3"));
        info.setAckType((byte) 1);
        info.setFirstMessageId(createMessageId("FirstMessageId:4"));
        info.setLastMessageId(createMessageId("LastMessageId:5"));
        info.setMessageCount(1);
    }
}
