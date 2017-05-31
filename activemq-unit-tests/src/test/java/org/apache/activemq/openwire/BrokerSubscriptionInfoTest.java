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
package org.apache.activemq.openwire;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.activemq.command.BrokerSubscriptionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerSubscriptionInfoTest {

    static final Logger LOG = LoggerFactory.getLogger(BrokerSubscriptionInfoTest.class);



    @Test
    public void testMarshalClientProperties() throws IOException {
        // marshal object
        OpenWireFormatFactory factory = new OpenWireFormatFactory();
        factory.setCacheEnabled(true);
        OpenWireFormat wf = (OpenWireFormat)factory.createWireFormat();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream ds = new DataOutputStream(buffer);

        ConsumerInfo info = new ConsumerInfo();
        info.setClientId("clientId");
        info.setConsumerId(new ConsumerId());

        int size = 1000;


        ConsumerInfo infos[] = new ConsumerInfo[size];
        for (int i = 0; i < infos.length; i++) {
            infos[i] = info;
        }

        BrokerSubscriptionInfo bsi = new BrokerSubscriptionInfo();
        bsi.setSubscriptionInfos(infos);

        wf.marshal(bsi, ds);
        ds.close();

        // unmarshal object and check that the properties are present.
        ByteArrayInputStream in = new ByteArrayInputStream(buffer.toByteArray());
        DataInputStream dis = new DataInputStream(in);
        BrokerSubscriptionInfo actual = (BrokerSubscriptionInfo) wf.unmarshal(dis);

        //assertTrue(actual instanceof BrokerSubscriptionInfo);
        assertEquals(size, actual.getSubscriptionInfos().length);
    }


}
