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
package org.apache.activemq.broker.jmx;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMIServerSocketFactory;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.management.Attribute;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.activemq.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An abstraction over JMX mbean registration
 * 
 * @org.apache.xbean.XBean
 * @version $Revision$
 */
public class ManagementContext implements Service {
    /**
     * Default activemq domain
     */
    public static final String DEFAULT_DOMAIN = "org.apache.activemq";
    private static final Log LOG = LogFactory.getLog(ManagementContext.class);
    private MBeanServer beanServer;
    private String jmxDomainName = DEFAULT_DOMAIN;
    private boolean useMBeanServer = true;
    private boolean createMBeanServer = true;
    private boolean locallyCreateMBeanServer;
    private boolean createConnector = true;
    private boolean findTigerMbeanServer = true;
    private String connectorHost = "localhost";
    private int connectorPort = 1099;
    private int rmiServerPort;
    private String connectorPath = "/jmxrmi";
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean connectorStarting = new AtomicBoolean(false);
    private JMXConnectorServer connectorServer;
    private ObjectName namingServiceObjectName;
    private Registry registry;
    private final List<ObjectName> registeredMBeanNames = new CopyOnWriteArrayList<ObjectName>();

    public ManagementContext() {
        this(null);
    }

    public ManagementContext(MBeanServer server) {
        this.beanServer = server;
    }

    public void start() throws IOException {
        // lets force the MBeanServer to be created if needed
        if (started.compareAndSet(false, true)) {
            getMBeanServer();
            if (connectorServer != null) {
                try {
                    getMBeanServer().invoke(namingServiceObjectName, "start", null, null);
                } catch (Throwable ignore) {
                }
                Thread t = new Thread("JMX connector") {
                    @Override
                    public void run() {
                        try {
                            JMXConnectorServer server = connectorServer;
                            if (started.get() && server != null) {
                                LOG.debug("Starting JMXConnectorServer...");
                                connectorStarting.set(true);
                                try {
                                	server.start();
                                } finally {
                                	connectorStarting.set(false);
                                }
                                LOG.info("JMX consoles can connect to " + server.getAddress());
                            }
                        } catch (IOException e) {
                            LOG.warn("Failed to start jmx connector: " + e.getMessage());
                            LOG.debug("Reason for failed jms connector start", e);
                        }
                    }
                };
                t.setDaemon(true);
                t.start();
            }
        }
    }

    public void stop() throws Exception {
        if (started.compareAndSet(true, false)) {
            MBeanServer mbeanServer = getMBeanServer();
            if (mbeanServer != null) {
                for (Iterator<ObjectName> iter = registeredMBeanNames.iterator(); iter.hasNext();) {
                    ObjectName name = iter.next();
                    
                        mbeanServer.unregisterMBean(name);
                    
                }
            }
            registeredMBeanNames.clear();
            JMXConnectorServer server = connectorServer;
            connectorServer = null;
            if (server != null) {
                try {
                	if (!connectorStarting.get()) {
                		server.stop();
                	}
                } catch (IOException e) {
                    LOG.warn("Failed to stop jmx connector: " + e.getMessage());
                }
                try {
                    getMBeanServer().invoke(namingServiceObjectName, "stop", null, null);
                } catch (Throwable ignore) {
                }
            }
            if (locallyCreateMBeanServer && beanServer != null) {
                // check to see if the factory knows about this server
                List list = MBeanServerFactory.findMBeanServer(null);
                if (list != null && !list.isEmpty() && list.contains(beanServer)) {
                    MBeanServerFactory.releaseMBeanServer(beanServer);
                }
            }
            beanServer = null;
        }
    }

    /**
     * @return Returns the jmxDomainName.
     */
    public String getJmxDomainName() {
        return jmxDomainName;
    }

    /**
     * @param jmxDomainName The jmxDomainName to set.
     */
    public void setJmxDomainName(String jmxDomainName) {
        this.jmxDomainName = jmxDomainName;
    }

    /**
     * Get the MBeanServer
     * 
     * @return the MBeanServer
     */
    protected MBeanServer getMBeanServer() {
        if (this.beanServer == null) {
            this.beanServer = findMBeanServer();
        }
        return beanServer;
    }

    /**
     * Set the MBeanServer
     * 
     * @param beanServer
     */
    public void setMBeanServer(MBeanServer beanServer) {
        this.beanServer = beanServer;
    }

    /**
     * @return Returns the useMBeanServer.
     */
    public boolean isUseMBeanServer() {
        return useMBeanServer;
    }

    /**
     * @param useMBeanServer The useMBeanServer to set.
     */
    public void setUseMBeanServer(boolean useMBeanServer) {
        this.useMBeanServer = useMBeanServer;
    }

    /**
     * @return Returns the createMBeanServer flag.
     */
    public boolean isCreateMBeanServer() {
        return createMBeanServer;
    }

    /**
     * @param enableJMX Set createMBeanServer.
     */
    public void setCreateMBeanServer(boolean enableJMX) {
        this.createMBeanServer = enableJMX;
    }

    public boolean isFindTigerMbeanServer() {
        return findTigerMbeanServer;
    }

    public boolean isConnectorStarted() {
		return connectorStarting.get() || (connectorServer != null && connectorServer.isActive());
	}

	/**
     * Enables/disables the searching for the Java 5 platform MBeanServer
     */
    public void setFindTigerMbeanServer(boolean findTigerMbeanServer) {
        this.findTigerMbeanServer = findTigerMbeanServer;
    }

    /**
     * Formulate and return the MBean ObjectName of a custom control MBean
     * 
     * @param type
     * @param name
     * @return the JMX ObjectName of the MBean, or <code>null</code> if
     *         <code>customName</code> is invalid.
     */
    public ObjectName createCustomComponentMBeanName(String type, String name) {
        ObjectName result = null;
        String tmp = jmxDomainName + ":" + "type=" + sanitizeString(type) + ",name=" + sanitizeString(name);
        try {
            result = new ObjectName(tmp);
        } catch (MalformedObjectNameException e) {
            LOG.error("Couldn't create ObjectName from: " + type + " , " + name);
        }
        return result;
    }

    /**
     * The ':' and '/' characters are reserved in ObjectNames
     * 
     * @param in
     * @return sanitized String
     */
    private static String sanitizeString(String in) {
        String result = null;
        if (in != null) {
            result = in.replace(':', '_');
            result = result.replace('/', '_');
            result = result.replace('\\', '_');
        }
        return result;
    }

    /**
     * Retrive an System ObjectName
     * 
     * @param domainName
     * @param containerName
     * @param theClass
     * @return the ObjectName
     * @throws MalformedObjectNameException
     */
    public static ObjectName getSystemObjectName(String domainName, String containerName, Class theClass) throws MalformedObjectNameException, NullPointerException {
        String tmp = domainName + ":" + "type=" + theClass.getName() + ",name=" + getRelativeName(containerName, theClass);
        return new ObjectName(tmp);
    }

    private static String getRelativeName(String containerName, Class theClass) {
        String name = theClass.getName();
        int index = name.lastIndexOf(".");
        if (index >= 0 && (index + 1) < name.length()) {
            name = name.substring(index + 1);
        }
        return containerName + "." + name;
    }
    
    public Object newProxyInstance( ObjectName objectName,
                      Class interfaceClass,
                      boolean notificationBroadcaster){
        return MBeanServerInvocationHandler.newProxyInstance(getMBeanServer(), objectName, interfaceClass, notificationBroadcaster);
        
    }
    
    public Object getAttribute(ObjectName name, String attribute) throws Exception{
        return getMBeanServer().getAttribute(name, attribute);
    }
    
    public ObjectInstance registerMBean(Object bean, ObjectName name) throws Exception{
        ObjectInstance result = getMBeanServer().registerMBean(bean, name);
        this.registeredMBeanNames.add(name);
        return result;
    }
    
    public Set queryNames(ObjectName name, QueryExp query) throws Exception{
        return getMBeanServer().queryNames(name, query);
    }
    
    public ObjectInstance getObjectInstance(ObjectName name) throws Exception {
        return getMBeanServer().getObjectInstance(name);
    }
    
    /**
     * Unregister an MBean
     * 
     * @param name
     * @throws JMException
     */
    public void unregisterMBean(ObjectName name) throws JMException {
        if (beanServer != null && beanServer.isRegistered(name) && this.registeredMBeanNames.remove(name)) {
            beanServer.unregisterMBean(name);
        }
    }

    protected synchronized MBeanServer findMBeanServer() {
        MBeanServer result = null;
        // create the mbean server
        try {
            if (useMBeanServer) {
                if (findTigerMbeanServer) {
                    result = findTigerMBeanServer();
                }
                if (result == null) {
                    // lets piggy back on another MBeanServer -
                    // we could be in an appserver!
                    List list = MBeanServerFactory.findMBeanServer(null);
                    if (list != null && list.size() > 0) {
                        result = (MBeanServer)list.get(0);
                    }
                }
            }
            if (result == null && createMBeanServer) {
                result = createMBeanServer();
            }
        } catch (NoClassDefFoundError e) {
            LOG.error("Could not load MBeanServer", e);
        } catch (Throwable e) {
            // probably don't have access to system properties
            LOG.error("Failed to initialize MBeanServer", e);
        }
        return result;
    }

    public MBeanServer findTigerMBeanServer() {
        String name = "java.lang.management.ManagementFactory";
        Class type = loadClass(name, ManagementContext.class.getClassLoader());
        if (type != null) {
            try {
                Method method = type.getMethod("getPlatformMBeanServer", new Class[0]);
                if (method != null) {
                    Object answer = method.invoke(null, new Object[0]);
                    if (answer instanceof MBeanServer) {
                    	if (createConnector) {
                    		createConnector((MBeanServer)answer);
                    	}
                        return (MBeanServer)answer;
                    } else {
                        LOG.warn("Could not cast: " + answer + " into an MBeanServer. There must be some classloader strangeness in town");
                    }
                } else {
                    LOG.warn("Method getPlatformMBeanServer() does not appear visible on type: " + type.getName());
                }
            } catch (Exception e) {
                LOG.warn("Failed to call getPlatformMBeanServer() due to: " + e, e);
            }
        } else {
            LOG.trace("Class not found: " + name + " so probably running on Java 1.4");
        }
        return null;
    }

    private static Class loadClass(String name, ClassLoader loader) {
        try {
            return loader.loadClass(name);
        } catch (ClassNotFoundException e) {
            try {
                return Thread.currentThread().getContextClassLoader().loadClass(name);
            } catch (ClassNotFoundException e1) {
                return null;
            }
        }
    }

    /**
     * @return
     * @throws NullPointerException
     * @throws MalformedObjectNameException
     * @throws IOException
     */
    protected MBeanServer createMBeanServer() throws MalformedObjectNameException, IOException {
        MBeanServer mbeanServer = MBeanServerFactory.createMBeanServer(jmxDomainName);
        locallyCreateMBeanServer = true;
        if (createConnector) {
            createConnector(mbeanServer);
        }
        return mbeanServer;
    }

    /**
     * @param mbeanServer
     * @throws MalformedObjectNameException
     * @throws MalformedURLException
     * @throws IOException
     */
    private void createConnector(MBeanServer mbeanServer) throws MalformedObjectNameException, MalformedURLException, IOException {
        // Create the NamingService, needed by JSR 160
        try {
            if (registry == null) {
                registry = LocateRegistry.createRegistry(connectorPort, null, new RMIServerSocketFactory() {
                    public ServerSocket createServerSocket(int port)
                            throws IOException {
                        ServerSocket result = new ServerSocket(port);
                        result.setReuseAddress(true);
                        return result;
                    }});
            }
            namingServiceObjectName = ObjectName.getInstance("naming:type=rmiregistry");
            // Do not use the createMBean as the mx4j jar may not be in the
            // same class loader than the server
            Class cl = Class.forName("mx4j.tools.naming.NamingService");
            mbeanServer.registerMBean(cl.newInstance(), namingServiceObjectName);
            // mbeanServer.createMBean("mx4j.tools.naming.NamingService",
            // namingServiceObjectName, null);
            // set the naming port
            Attribute attr = new Attribute("Port", Integer.valueOf(connectorPort));
            mbeanServer.setAttribute(namingServiceObjectName, attr);
        } catch(ClassNotFoundException e) {
            LOG.debug("Probably not using JRE 1.4: " + e.getLocalizedMessage());
        }
        catch (Throwable e) {
            LOG.debug("Failed to create local registry", e);
        }
        // Create the JMXConnectorServer
        String rmiServer = "";
        if (rmiServerPort != 0) {
            // This is handy to use if you have a firewall and need to
            // force JMX to use fixed ports.
            rmiServer = ""+getConnectorHost()+":" + rmiServerPort;
        }
        String serviceURL = "service:jmx:rmi://" + rmiServer + "/jndi/rmi://" +getConnectorHost()+":" + connectorPort + connectorPath;
        JMXServiceURL url = new JMXServiceURL(serviceURL);
        connectorServer = JMXConnectorServerFactory.newJMXConnectorServer(url, null, mbeanServer);
    }

    public String getConnectorPath() {
        return connectorPath;
    }

    public void setConnectorPath(String connectorPath) {
        this.connectorPath = connectorPath;
    }

    public int getConnectorPort() {
        return connectorPort;
    }

    public void setConnectorPort(int connectorPort) {
        this.connectorPort = connectorPort;
    }

    public int getRmiServerPort() {
        return rmiServerPort;
    }

    public void setRmiServerPort(int rmiServerPort) {
        this.rmiServerPort = rmiServerPort;
    }

    public boolean isCreateConnector() {
        return createConnector;
    }

    public void setCreateConnector(boolean createConnector) {
        this.createConnector = createConnector;
    }

    /**
     * Get the connectorHost
     * @return the connectorHost
     */
    public String getConnectorHost() {
        return this.connectorHost;
    }

    /**
     * Set the connectorHost
     * @param connectorHost the connectorHost to set
     */
    public void setConnectorHost(String connectorHost) {
        this.connectorHost = connectorHost;
    }
}
