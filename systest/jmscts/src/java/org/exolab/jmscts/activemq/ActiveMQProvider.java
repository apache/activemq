/**
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "Exolab" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Exoffice Technologies.  For written permission,
 *    please contact jima@intalio.com.
 *
 * 4. Products derived from this Software may not be called "Exolab"
 *    nor may "Exolab" appear in their names without prior written
 *    permission of Exoffice Technologies. Exolab is a registered
 *    trademark of Exoffice Technologies.
 *
 * 5. Due credit should be given to the Exolab Project
 *    (http://www.exolab.org/).
 *
 * THIS SOFTWARE IS PROVIDED BY EXOFFICE TECHNOLOGIES AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * EXOFFICE TECHNOLOGIES OR ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2001, 2003 (C) Exoffice Technologies Inc. All Rights Reserved.
 * Copyright 2005 (C) Hiram Chirino
 *
 * $Id: ActiveMQProvider.java,v 1.1.1.1 2005/03/11 21:15:21 jstrachan Exp $
 */
package org.exolab.jmscts.activemq;

import java.util.HashMap;

import javax.jms.JMSException;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;

import org.apache.commons.logging.LogFactory;
import org.activemq.ActiveMQConnection;
import org.activemq.ActiveMQConnectionFactory;
import org.activemq.ActiveMQXAConnectionFactory;
import org.activemq.message.ActiveMQDestination;
import org.activemq.message.ActiveMQQueue;
import org.activemq.message.ActiveMQTopic;
import org.exolab.jmscts.provider.Administrator;
import org.exolab.jmscts.provider.Provider;


/**
 * This class provides methods for obtaining and manipulating administered
 * objects managed by the ActiveMQ implementation of JMS
 *
 * @author <a href="mailto:james@protique.com">James Strahcan</a>
 * @author <a href="mailto:hiram@protique.com">Hiram Chirino</a>
 * @version $Revision: 1.1.1.1 $ $Date: 2005/03/11 21:15:21 $
 */
public class ActiveMQProvider implements Provider, Administrator {

    private String brokerURL = System.getProperty("url", "vm://localhost");
    private HashMap directory = new HashMap();
    private ActiveMQConnection adminConnection = null;
    
    /**
     * Initialises the administation interface
     *
     * @throws JMSException if the administration interface can't be
     *                      initialised
     */
    public void initialise(boolean start) throws JMSException {
        
    	System.out.println("==================================");
    	System.out.println("ActiveMQ provider starting up.");
    	System.out.println("==================================");
    	
    	directory.put(getQueueConnectionFactory(), new ActiveMQConnectionFactory(brokerURL));
        directory.put(getTopicConnectionFactory(), new ActiveMQConnectionFactory(brokerURL));
        directory.put(getXAQueueConnectionFactory(), new ActiveMQXAConnectionFactory(brokerURL));
        directory.put(getXATopicConnectionFactory(), new ActiveMQXAConnectionFactory(brokerURL));

		if( adminConnection!=null )
			throw new JMSException("Admin connection allready established.");
		
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
        adminConnection = (ActiveMQConnection) factory.createConnection();
        adminConnection.setClientID("admin");	        
        adminConnection.start();
    }

    /**
     * This method cleans up the administrator
     * @throws JMSException 
     */
    public void cleanup(boolean stop) throws JMSException {
    	System.out.println("==================================");
    	System.out.println("ActiveMQ provider sutting down.");
    	System.out.println("==================================");
    	adminConnection.close();
    	adminConnection=null;
        directory.clear();
    }

    /**
     * Returns the administration interface
     */
    public Administrator getAdministrator() {
        return this;
    }

    /**
     * Look up the named administered object
     *
     * @param name the name that the administered object is bound to
     * @return the administered object bound to name
     * @throws javax.naming.NamingException if the object is not bound, or the lookup fails
     */
    public Object lookup(String name) throws NamingException {
        Object result = directory.get(name);
        if (result == null) {
            throw new NameNotFoundException("Name not found: " + name);
        }
        return result;
    }

    /**
     * Create an administered destination
     *
     * @param name  the destination name
     * @param queue if true, create a queue, else create a topic
     * @throws JMSException if the destination cannot be created
     */
    public void createDestination(String name, boolean queue)
            throws JMSException {
        if (queue) {
        	directory.put(name, new ActiveMQQueue(name));
        }
        else {
        	directory.put(name, new ActiveMQTopic(name));
        }
    }

    /**
     * Destroy an administered destination
     *
     * @param name the destination name
     * @throws JMSException if the destination cannot be destroyed
     */
    public void destroyDestination(String name) throws JMSException {
    	Object object = directory.remove(name);
    	if( object!=null && object instanceof ActiveMQDestination ) {
    		adminConnection.destroyDestination((ActiveMQDestination) object);
    	}
    }

    /**
     * Returns true if an administered destination exists
     *
     * @param name the destination name
     * @throws JMSException for any internal JMS provider error
     */
    public boolean destinationExists(String name)
            throws JMSException {
    	return directory.containsKey(name);
    }
    
    public String getQueueConnectionFactory() {
        return "QueueConnectionFactory";
    }
    public String getTopicConnectionFactory() {
        return "TopicConnectionFactory";
    }
    public String getXAQueueConnectionFactory() {
        return "XAQueueConnectionFactory";
    }
    public String getXATopicConnectionFactory() {
        return "XATopicConnectionFactory";
    }

    public String getBrokerURL() {
        return brokerURL;
    }

    public void setBrokerURL(String brokerURL) {
    	LogFactory.getLog("TEST").info("!!!!!!!!!!!!!! setting url to "+brokerURL);
        this.brokerURL = brokerURL;
    }
}
