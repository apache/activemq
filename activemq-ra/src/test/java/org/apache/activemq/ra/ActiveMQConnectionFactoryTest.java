/*
 *  Copyright 2008 hak8fe.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */

package org.apache.activemq.ra;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import javax.jms.Connection;
import junit.framework.TestCase;

/**
 *
 * @author hak8fe
 */
public class ActiveMQConnectionFactoryTest extends TestCase {
    
    ActiveMQManagedConnectionFactory mcf;
    ActiveMQConnectionRequestInfo info;
    String url = "vm://localhost";
    String user = "defaultUser";
    String pwd = "defaultPasswd";
    
    public ActiveMQConnectionFactoryTest(String testName) {
        super(testName);
    }            

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        mcf = new ActiveMQManagedConnectionFactory();
        info = new ActiveMQConnectionRequestInfo();
        info.setServerUrl(url);
        info.setUserName(user);
        info.setPassword(pwd);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testSerializability() throws Exception
    {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(mcf, new ConnectionManagerAdapter(), info);
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(factory);
        oos.close();
        byte[] byteArray = bos.toByteArray();
        
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(byteArray));
        ActiveMQConnectionFactory deserializedFactory = (ActiveMQConnectionFactory) ois.readObject();
        ois.close();
        
        Connection con = deserializedFactory.createConnection("defaultUser", "defaultPassword");
        assertNotNull("Connection object returned by ActiveMQConnectionFactory.createConnection() is null", con);
    }

}
