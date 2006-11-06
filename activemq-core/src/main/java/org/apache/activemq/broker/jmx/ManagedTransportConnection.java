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
package org.apache.activemq.broker.jmx;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.JMXSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.IOException;
import java.util.Hashtable;

/**
 * A managed transport connection
 * 
 * @version $Revision: 1.1 $
 */
public class ManagedTransportConnection extends TransportConnection {
    private static final Log log = LogFactory.getLog(ManagedTransportConnection.class);

    private final MBeanServer server;
    private final ObjectName connectorName;
    private ConnectionViewMBean mbean;

    private ObjectName byClientIdName;
	private ObjectName byAddressName;

    public ManagedTransportConnection(TransportConnector connector, Transport transport, Broker broker, TaskRunnerFactory factory, MBeanServer server,
            ObjectName connectorName) throws IOException {
        super(connector, transport, broker, factory);
        this.server = server;
        this.connectorName = connectorName;
        this.mbean = new ConnectionView(this);
        byAddressName = createByAddressObjectName("address", transport.getRemoteAddress());
        registerMBean(byAddressName);
    }

    public synchronized void stop() throws Exception {
        if (isStarting()) {
            setPendingStop(true);
            return;
        }
    	synchronized(this) {
	        unregisterMBean(byClientIdName);
	        unregisterMBean(byAddressName);
	        byClientIdName=null;
	        byAddressName=null;
    	}
        super.stop();
    }

    /**
     * Sets the connection ID of this connection. On startup this connection ID
     * is set to an incrementing counter; once the client registers it is set to
     * the clientID of the JMS client.
     */
    public void setConnectionId(String connectionId) throws IOException {
    }

    public Response processAddConnection(ConnectionInfo info) throws Exception {
        Response answer = super.processAddConnection(info);
        String clientId = info.getClientId();
        if (clientId != null) {
            if(byClientIdName==null) {
    	        byClientIdName = createByClientIdObjectName(clientId);
    	        registerMBean(byClientIdName);
            }
        }
        return answer;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected void registerMBean(ObjectName name) {
    	if( name!=null ) {
	        try {
	            server.registerMBean(mbean, name);
	        } catch (Throwable e) {
	            log.warn("Failed to register MBean: "+name);
	            log.debug("Failure reason: "+e,e);
	        }
    	}
    }

    protected void unregisterMBean(ObjectName name) {
        if (name != null) {
            try {
                server.unregisterMBean(name);
            }
            catch (Throwable e) {
                log.warn("Failed to unregister mbean: " + name);
                log.debug("Failure reason: "+e,e);
            }
        }
    }

    protected ObjectName createByAddressObjectName(String type, String value) throws IOException {
        // Build the object name for the destination
        Hashtable map = connectorName.getKeyPropertyList();
        try {
            return new ObjectName(
            		connectorName.getDomain()+":"+
            		"BrokerName="+JMXSupport.encodeObjectNamePart((String) map.get("BrokerName"))+","+
            		"Type=Connection,"+
                    "ConnectorName="+JMXSupport.encodeObjectNamePart((String) map.get("ConnectorName"))+","+
            		"ViewType="+JMXSupport.encodeObjectNamePart(type)+","+
            		"Name="+JMXSupport.encodeObjectNamePart(value)
            		);
        }
        catch (Throwable e) {
            throw IOExceptionSupport.create(e);
        }
    }
    
    protected ObjectName createByClientIdObjectName(String value) throws IOException {
        // Build the object name for the destination
        Hashtable map = connectorName.getKeyPropertyList();
        try {
            return new ObjectName(
            		connectorName.getDomain()+":"+
            		"BrokerName="+JMXSupport.encodeObjectNamePart((String) map.get("BrokerName"))+","+
            		"Type=Connection,"+
                    "ConnectorName="+JMXSupport.encodeObjectNamePart((String) map.get("ConnectorName"))+","+
                	"Connection="+JMXSupport.encodeObjectNamePart(value)
            		);
        }
        catch (Throwable e) {
            throw IOExceptionSupport.create(e);
        }
    }


}
