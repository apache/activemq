/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.panacya.platform.service.bus.sender;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.ejb.CreateException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.rmi.PortableRemoteObject;
import java.rmi.RemoteException;

/**
 * @author <a href="mailto:michael.gaffney@panacya.com">Michael Gaffney </a>
 */
public class SenderClient {
    private static final Log _log = LogFactory.getLog(SenderClient.class);

    private Sender sender;
    private String ejbName;
    
    public SenderClient(String ejbName) throws NamingException, RemoteException, CreateException {
        setEjbName(ejbName);
    }

    public void sendMessage(final String message) throws RemoteException, SenderException {
        if (_log.isInfoEnabled()) {
            _log.info("Sending message: " + message);
        }
        sender.sendMessage(message);
        if (_log.isInfoEnabled()) {
            _log.info("Message sent");
        }
    }

    public String getEjbName() {
        return ejbName;
    }

    private void setEjbName(final String ejbName) throws NamingException, RemoteException, CreateException {        
        this.ejbName = ejbName;
        lookupSender(ejbName);
    }

    private void lookupSender(final String ejbName) throws NamingException, RemoteException, CreateException {
        if (_log.isInfoEnabled()) {
            _log.info("Looking up Sender: " + ejbName);
        }
        Context context = new InitialContext();            
        Object objectRef = context.lookup(ejbName);
        SenderHome senderHome = (SenderHome) PortableRemoteObject.narrow(objectRef, SenderHome.class);
        sender = senderHome.create();
    }

    public static void main(String[] args) {

        try {
            SenderClient client = new SenderClient("SenderEJB");
            client.sendMessage("Hello ActiveMQ");
        } catch (Exception e) {
            e.printStackTrace();
        } 
    }

}
