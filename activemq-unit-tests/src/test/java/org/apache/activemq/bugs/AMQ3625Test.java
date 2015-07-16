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
package org.apache.activemq.bugs;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */

public class AMQ3625Test {
    
    protected BrokerService broker1;
    protected BrokerService broker2;
    
    protected AtomicBoolean authenticationFailed = new AtomicBoolean(false);
    protected AtomicBoolean gotNPE = new AtomicBoolean(false);

    protected String java_security_auth_login_config = "java.security.auth.login.config";
    protected String xbean = "xbean:";
    protected String base = "src/test/resources/org/apache/activemq/bugs/amq3625";
    protected String conf = "conf";
    protected String keys = "keys";
    protected String JaasStompSSLBroker1_xml = "JaasStompSSLBroker1.xml";
    protected String JaasStompSSLBroker2_xml = "JaasStompSSLBroker2.xml";
    
    protected String oldLoginConf = null;

    @Before
    public void before() throws Exception {
        if (System.getProperty(java_security_auth_login_config) != null) {
            oldLoginConf = System.getProperty(java_security_auth_login_config);
        }
        System.setProperty(java_security_auth_login_config, base + "/" + conf + "/" + "login.config");
        broker1 = BrokerFactory.createBroker(xbean + base + "/" + conf + "/" + JaasStompSSLBroker1_xml);
        broker2 = BrokerFactory.createBroker(xbean + base + "/" + conf + "/" + JaasStompSSLBroker2_xml);
        
        broker1.start();
        broker1.waitUntilStarted();
        broker2.start();
        broker2.waitUntilStarted();
    }

    @After
    public void after() throws Exception {
        broker1.stop();
        broker2.stop();
        
        if (oldLoginConf != null) {
            System.setProperty(java_security_auth_login_config, oldLoginConf);
        }
    }
    
    @Test
    public void go() throws Exception {
        Appender appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getMessage().toString().contains("java.lang.SecurityException")) {
                    authenticationFailed.set(true);
                }
                if (event.getMessage().toString().contains("NullPointerException")) {
                    gotNPE.set(true);
                }
            }
        };
        Logger.getRootLogger().addAppender(appender);
        
        String connectURI = broker1.getConnectorByName("openwire").getConnectUri().toString();
        connectURI = connectURI.replace("?needClientAuth=true", "");
        broker2.addNetworkConnector("static:(" + connectURI + ")").start();
        
        Thread.sleep(10 * 1000);
        
        Logger.getRootLogger().removeAppender(appender);
        
        assertTrue(authenticationFailed.get());
        assertFalse(gotNPE.get());
    }
}
