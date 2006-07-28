/**
 *
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
package org.activemq.itest.ejb;

import java.rmi.RemoteException;

import javax.ejb.EJBObject;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.naming.NamingException;


/**
 * Provides the Remote interface of the MessengerBean.
 *
 * @version $Revision: 1.1 $
 */
public interface JMSTool extends EJBObject {
    
    public void sendTextMessage(String dest, String text) throws RemoteException, JMSException, NamingException;
    public void sendTextMessage(Destination dest, String text) throws RemoteException, JMSException, NamingException;
    public String receiveTextMessage(String dest, long timeout) throws RemoteException, JMSException, NamingException;
    public String receiveTextMessage(Destination dest, long timeout) throws RemoteException, JMSException, NamingException;
    public int drain(String dest) throws RemoteException, JMSException, NamingException;
    public int drain(Destination dest) throws RemoteException, JMSException, NamingException;
    
}

