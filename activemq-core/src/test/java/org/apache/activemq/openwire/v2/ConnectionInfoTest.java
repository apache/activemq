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

import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.ConnectionInfo;

/**
 * Test case for the OpenWire marshalling for ConnectionInfo NOTE!: This file is
 * auto generated - do not modify! if you need to make a change, please see the
 * modify the groovy scripts in the under src/gram/script and then use maven
 * openwire:generate to regenerate this file.
 * 
 * @version $Revision: $
 */
public class ConnectionInfoTest extends BaseCommandTestSupport {

    public static ConnectionInfoTest SINGLETON = new ConnectionInfoTest();

    public Object createObject() throws Exception {
        ConnectionInfo info = new ConnectionInfo();
        populateObject(info);
        return info;
    }

    protected void populateObject(Object object) throws Exception {
        super.populateObject(object);
        ConnectionInfo info = (ConnectionInfo)object;

        info.setConnectionId(createConnectionId("ConnectionId:1"));
        info.setClientId("ClientId:2");
        info.setPassword("Password:3");
        info.setUserName("UserName:4");
        {
            BrokerId value[] = new BrokerId[2];
            for (int i = 0; i < 2; i++) {
                value[i] = createBrokerId("BrokerPath:5");
            }
            info.setBrokerPath(value);
        }
        info.setBrokerMasterConnector(true);
        info.setManageable(false);
        info.setClientMaster(true);
    }
}
