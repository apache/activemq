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
package org.apache.activemq.network;

import javax.jms.JMSException;
import org.junit.Test;

public class UserIDBrokerPopulateFalseTest extends UserIDBrokerTest {

    @Override
    protected void doSetUp(boolean deleteAllMessages) throws Exception {
        doSetUp(deleteAllMessages, false, false);
    }

    @Test
    @Override
    public void testPopulateJMSXUserIdLocalAndNetwork() throws JMSException {

        sendTextMessage(localConnection, "exclude.test.foo","This local message is JMSXUserID=null");
        verifyTextMessage(localConnection, "exclude.test.foo", "This local message is JMSXUserID=null", "JMSXUserID", null, false);

        // Across the network
        sendTextMessage(localConnection, "include.test.foo", "This network message is JMSXUserID=null");
        verifyTextMessage(remoteConnection, "include.test.foo", "This network message is JMSXUserID=null", "JMSXUserID", null, false);
    }

}
