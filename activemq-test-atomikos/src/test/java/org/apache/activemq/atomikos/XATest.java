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
package org.apache.activemq.atomikos;

import com.atomikos.datasource.xa.DefaultXidFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

import javax.jms.MessageConsumer;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import junit.framework.TestCase;


/**
 * @version $Revision$
 */
public class XATest extends TestCase {

    public void testXA() throws Exception {
		BrokerService broker = new BrokerService();
		broker.addConnector("tcp://localhost:61616");
		broker.start();
		
	
        String url = "tcp://localhost:61616";
        String qName = "MyQueue";
        int timeout = 5;
        DefaultXidFactory xidFactory = new DefaultXidFactory();
        ActiveMQXAConnectionFactory xacf = new ActiveMQXAConnectionFactory();
        xacf.setBrokerURL(url);
        ActiveMQQueue queue = new ActiveMQQueue();
        queue.setPhysicalName(qName);
        XAConnection xaconn = xacf.createXAConnection();
        xaconn.start();
        XASession session = xaconn.createXASession();
        XAResource xares = session.getXAResource();
        MessageConsumer receiver = session.getSession().createConsumer(queue);
        xares.recover(XAResource.TMSTARTRSCAN);
        xares.recover(XAResource.TMNOFLAGS);
        xares.setTransactionTimeout(timeout);
        xares.isSameRM(xares);
        Xid xid = xidFactory.createXid("part1", "part2");
        xares.start(xid, XAResource.TMNOFLAGS);
        receiver.receive(timeout);
        xares.end(xid, XAResource.TMSUCCESS);
        xares.rollback(xid);

        System.out.println("Done!");
    }

}
