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
package org.apache.activemq.transport.tcp;

import java.net.URI;
import java.net.URISyntaxException;

import junit.framework.Test;
import junit.textui.TestRunner;

import org.apache.activemq.transport.TransportBrokerTestSupport;

public class Krb5TransportBrokerTest extends TransportBrokerTestSupport {

    protected String getBindLocation() {
        return "krb5://localhost:0?transport.soWriteTimeout=20000&krb5ConfigName=Server";
    }

    @Override
    protected URI getBindURI() throws URISyntaxException {
        return new URI("krb5://localhost:0?soWriteTimeout=20000&krb5ConfigName=Server");
    }

    protected void setUp() throws Exception {
        //FIXME
        //Neet setup apache DS
        System.setProperty("java.security.auth.login.config",
                "/home/pklimczak/workspace-new/krb5socket/src/main/java/com/redhat/test/kerberos/krb5socket/LoginModule.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");
//      System.setProperty("javax.net.debug", "ssl,handshake,data,trustmanager");
//      System.setProperty("sun.security.krb5.debug", "true");

        maxWait = 10000;
        super.setUp();
    }

    public static Test suite() {
        return suite(Krb5TransportBrokerTest.class);
    }

    public static void main(String[] args) {
        TestRunner.run(suite());
    }

}
