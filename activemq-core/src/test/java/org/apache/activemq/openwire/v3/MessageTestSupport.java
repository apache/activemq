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
package org.apache.activemq.openwire.v3;

import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.Message;

/**
 * Test case for the OpenWire marshalling for Message NOTE!: This file is auto
 * generated - do not modify! if you need to make a change, please see the
 * modify the groovy scripts in the under src/gram/script and then use maven
 * openwire:generate to regenerate this file.
 * 
 * @version $Revision: $
 */
public abstract class MessageTestSupport extends BaseCommandTestSupport {

    protected void populateObject(Object object) throws Exception {
        super.populateObject(object);
        Message info = (Message)object;

        info.setProducerId(createProducerId("ProducerId:1"));
        info.setDestination(createActiveMQDestination("Destination:2"));
        info.setTransactionId(createTransactionId("TransactionId:3"));
        info.setOriginalDestination(createActiveMQDestination("OriginalDestination:4"));
        info.setMessageId(createMessageId("MessageId:5"));
        info.setOriginalTransactionId(createTransactionId("OriginalTransactionId:6"));
        info.setGroupID("GroupID:7");
        info.setGroupSequence(1);
        info.setCorrelationId("CorrelationId:8");
        info.setPersistent(true);
        info.setExpiration(1);
        info.setPriority((byte)1);
        info.setReplyTo(createActiveMQDestination("ReplyTo:9"));
        info.setTimestamp(2);
        info.setType("Type:10");
        {
            byte data[] = "Content:11".getBytes();
            info.setContent(new org.apache.activemq.util.ByteSequence(data, 0, data.length));
        }
        {
            byte data[] = "MarshalledProperties:12".getBytes();
            info.setMarshalledProperties(new org.apache.activemq.util.ByteSequence(data, 0, data.length));
        }
        info.setDataStructure(createDataStructure("DataStructure:13"));
        info.setTargetConsumerId(createConsumerId("TargetConsumerId:14"));
        info.setCompressed(false);
        info.setRedeliveryCounter(2);
        {
            BrokerId value[] = new BrokerId[2];
            for (int i = 0; i < 2; i++) {
                value[i] = createBrokerId("BrokerPath:15");
            }
            info.setBrokerPath(value);
        }
        info.setArrival(3);
        info.setUserID("UserID:16");
        info.setRecievedByDFBridge(true);
        info.setDroppable(false);
        {
            BrokerId value[] = new BrokerId[2];
            for (int i = 0; i < 2; i++) {
                value[i] = createBrokerId("Cluster:17");
            }
            info.setCluster(value);
        }
        info.setBrokerInTime(4);
        info.setBrokerOutTime(5);
    }
}
