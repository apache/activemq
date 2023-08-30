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

import org.junit.Test;
import jakarta.jms.Session;

public class ActiveMQJMS2XAConnectionTest extends ActiveMQJMS2XATestBase {


    // XA connection always creates SESSION_TRANSACTED
    @Test
    public void testCreateSession() throws Exception {
        verifySession(connection.createSession(), Session.SESSION_TRANSACTED);
    }

    @Test
    public void testCreateSessionAckModeAuto() throws Exception {
        verifySession(connection.createSession(Session.AUTO_ACKNOWLEDGE), Session.SESSION_TRANSACTED);
    }

    @Test
    public void testCreateSessionAckModeClient() throws Exception {
        verifySession(connection.createSession(Session.CLIENT_ACKNOWLEDGE), Session.SESSION_TRANSACTED);
    }

    @Test
    public void testCreateSessionAckModeDups() throws Exception {
        verifySession(connection.createSession(Session.DUPS_OK_ACKNOWLEDGE), Session.SESSION_TRANSACTED);
    }

    @Test
    public void testCreateSessionAckModeTrans() throws Exception {
        verifySession(connection.createSession(Session.SESSION_TRANSACTED), Session.SESSION_TRANSACTED);
    }

    @Test
    public void testCreateXASession() throws Exception {
        verifySession(xaConnection.createSession(), Session.SESSION_TRANSACTED);
    }

    @Test
    public void testCreateXASessionAckModeAuto() throws Exception {
        verifySession(xaConnection.createSession(Session.AUTO_ACKNOWLEDGE), Session.SESSION_TRANSACTED);
    }

    @Test
    public void testCreateXASessionAckModeClient() throws Exception {
        verifySession(xaConnection.createSession(Session.CLIENT_ACKNOWLEDGE), Session.SESSION_TRANSACTED);
    }

    @Test
    public void testCreateXASessionAckModeDups() throws Exception {
        verifySession(xaConnection.createSession(Session.DUPS_OK_ACKNOWLEDGE), Session.SESSION_TRANSACTED);
    }

    @Test
    public void testCreateXASessionAckModeTrans() throws Exception {
        verifySession(xaConnection.createSession(Session.SESSION_TRANSACTED), Session.SESSION_TRANSACTED);
    }
}
