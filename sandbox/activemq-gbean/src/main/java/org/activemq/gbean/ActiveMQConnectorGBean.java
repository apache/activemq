/**
 *
 * Copyright 2004 Protique Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/
package org.activemq.gbean;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import javax.jms.JMSException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.geronimo.gbean.GBeanInfo;
import org.apache.geronimo.gbean.GBeanInfoBuilder;
import org.apache.geronimo.gbean.GBeanLifecycle;
import org.apache.geronimo.gbean.GConstructorInfo;
import org.apache.geronimo.kernel.Kernel;
import org.activemq.ActiveMQConnectionFactory;
import org.activemq.broker.BrokerConnector;
import org.activemq.broker.impl.BrokerConnectorImpl;
import org.activemq.io.WireFormat;
import org.activemq.io.impl.DefaultWireFormat;

/**
 * Default implementation of the ActiveMQ connector
 *
 * @version $Revision: 1.1.1.1 $
 */
public class ActiveMQConnectorGBean implements GBeanLifecycle, ActiveMQConnector {
    private Log log = LogFactory.getLog(getClass().getName());

    private BrokerConnector brokerConnector;
    private ActiveMQContainer container;
    private WireFormat wireFormat = new DefaultWireFormat();
    private String protocol;
    private String host;
    private int port;
    private String path;
    private String query;
    private String urlAsStarted;

    public ActiveMQConnectorGBean(ActiveMQContainer container, String protocol, String host, int port) {
        this.container = container;
        this.protocol = protocol;
        this.host = host;
        this.port = port;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getUrl() {
        try {
            return new URI(protocol, null, host, port, path, query, null).toString();
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Attributes don't form a valid URI: "+protocol+"://"+host+":"+port+"/"+path+"?"+query);
        }
    }

    public WireFormat getWireFormat() {
        return wireFormat;
    }

    public void setWireFormat(WireFormat wireFormat) {
        this.wireFormat = wireFormat;
    }

    public InetSocketAddress getListenAddress() {
	return brokerConnector == null ? null : brokerConnector.getServerChannel().getSocketAddress();
    }

    public synchronized void doStart() throws Exception {
    	ClassLoader old = Thread.currentThread().getContextClassLoader();
    	Thread.currentThread().setContextClassLoader(ActiveMQContainerGBean.class.getClassLoader());
    	try {
	        if (brokerConnector == null) {
                urlAsStarted = getUrl();
	            brokerConnector = createBrokerConnector(urlAsStarted);
	            brokerConnector.start();
	            ActiveMQConnectionFactory.registerBroker(urlAsStarted, brokerConnector);
	        }
    	} finally {
        	Thread.currentThread().setContextClassLoader(old);
    	}
    }

    public synchronized void doStop() throws Exception {
        if (brokerConnector != null) {
            ActiveMQConnectionFactory.unregisterBroker(urlAsStarted);
            BrokerConnector temp = brokerConnector;
            brokerConnector = null;
            temp.stop();
        }
    }

    public synchronized void doFail() {
        if (brokerConnector != null) {
            BrokerConnector temp = brokerConnector;
            brokerConnector = null;
            try {
                temp.stop();
            }
            catch (JMSException e) {
                log.info("Caught while closing due to failure: " + e, e);
            }
        }
    }

    protected BrokerConnector createBrokerConnector(String url) throws Exception {
        return new BrokerConnectorImpl(container.getBrokerContainer(), url, wireFormat);
    }

    public static final GBeanInfo GBEAN_INFO;

    static {
        GBeanInfoBuilder infoFactory = new GBeanInfoBuilder("ActiveMQ Message Broker Connector", ActiveMQConnectorGBean.class, CONNECTOR_J2EE_TYPE);
        infoFactory.addAttribute("url", String.class.getName(), false);
        infoFactory.addAttribute("wireFormat", WireFormat.class.getName(), false);
        infoFactory.addReference("activeMQContainer", ActiveMQContainer.class);
        infoFactory.addInterface(ActiveMQConnector.class, new String[]{"host","port","protocol","path","query"});
        infoFactory.setConstructor(new GConstructorInfo(new String[]{"activeMQContainer", "protocol", "host", "port"}));
        GBEAN_INFO = infoFactory.getBeanInfo();
    }

    public static GBeanInfo getGBeanInfo() {
        return GBEAN_INFO;
    }
}
