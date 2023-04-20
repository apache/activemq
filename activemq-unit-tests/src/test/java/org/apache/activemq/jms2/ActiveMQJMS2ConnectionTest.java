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
package org.apache.activemq.jms2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import javax.jms.JMSException;
import javax.jms.Session;
import org.junit.Test;

public class ActiveMQJMS2ConnectionTest extends ActiveMQJMS2TestBase {

    @Test
    public void testCreateSession() throws Exception {
        verifySession(connection.createSession(), Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    public void testCreateSessionAckModeAuto() throws Exception {
        verifySession(connection.createSession(Session.AUTO_ACKNOWLEDGE), Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    public void testCreateSessionAckModeClient() throws Exception {
        verifySession(connection.createSession(Session.CLIENT_ACKNOWLEDGE), Session.CLIENT_ACKNOWLEDGE);
    }

    @Test
    public void testCreateSessionAckModeDups() throws Exception {
        verifySession(connection.createSession(Session.DUPS_OK_ACKNOWLEDGE), Session.DUPS_OK_ACKNOWLEDGE);
    }

    @Test
    public void testCreateSessionAckModeTrans() throws Exception {
        verifySession(connection.createSession(Session.SESSION_TRANSACTED), Session.SESSION_TRANSACTED);
    }

    private void verifySession(Session session, int acknowledgeMode) throws JMSException {
        try {
            assertNotNull(session);
            assertEquals(acknowledgeMode, session.getAcknowledgeMode());
            assertEquals(acknowledgeMode == Session.SESSION_TRANSACTED, session.getTransacted());
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

}
