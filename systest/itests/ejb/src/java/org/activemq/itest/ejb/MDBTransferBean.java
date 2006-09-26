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
package org.activemq.itest.ejb;

import javax.ejb.EJBException;
import javax.ejb.MessageDrivenBean;
import javax.ejb.MessageDrivenContext;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameClassPair;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 *
 * @version $Revision: 1.1 $ $Date: 2005/03/14 02:57:53 $
 *
 * */
public class MDBTransferBean implements MessageDrivenBean, MessageListener {
    private static final Log log = LogFactory.getLog(MDBTransferBean.class);
    private MessageDrivenContext messageDrivenContext;
    private Context envContext;

    public void ejbCreate() {

    }

    public void ejbRemove() throws EJBException {
    }

    public void setMessageDrivenContext(MessageDrivenContext messageDrivenContext) throws EJBException {
        try {
            this.messageDrivenContext = messageDrivenContext;
            envContext = (Context) new InitialContext().lookup("java:comp/env");
        }
        catch (NamingException e) {
            throw new EJBException(e);
        }
    }

    public void onMessage(Message message) {
        System.out.println("entering onMessage");
        try {
            ConnectionFactory cf = (ConnectionFactory) envContext.lookup("jms/Default");
            Connection con = cf.createConnection();
            try {
                
                Session session = con.createSession(true, 0);
                Destination dest = (Destination) envContext.lookup("MDBOut");
                MessageProducer producer = session.createProducer(dest);
                producer.setDeliveryMode(message.getJMSDeliveryMode());
                producer.send(message);
                
            } finally {
                con.close();
            }
        } catch (Throwable e) {
            log.info(e);
        }
        System.out.println("leaving onMessage");
    }

   /**
    *
    */
   private void printCompEnv() throws NamingException {
       log.warn("Printing java:comp/env/jms context: ");
       Context c = (Context) new InitialContext().lookup("java:comp/env/jms");
       NamingEnumeration iter = c.list("");
       while (iter.hasMoreElements()) {
           NameClassPair pair = (NameClassPair) iter.next();
           log.warn("'" + pair.getName() + "'");
           /*
            * Object obj = ctx.lookup(node.getName()); if ( obj instanceof
            * Context ){ node.type = Node.CONTEXT;
            * buildNode(node,(Context)obj); } else if (obj instanceof
            * java.rmi.Remote) { node.type = Node.BEAN; } else { node.type =
            * Node.OTHER; }
            */
       }
   }

}


