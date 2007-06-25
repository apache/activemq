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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.rmi.registry.LocateRegistry;
import java.util.List;
import javax.management.Attribute;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.activemq.Service;
import org.apache.activemq.util.ClassLoading;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.concurrent.atomic.AtomicBoolean;
/**
 * A Flow provides different dispatch policies within the NMR
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision$
 */
public class ManagementContext implements Service{
    /**
     * Default activemq domain
     */
    public static final String DEFAULT_DOMAIN="org.apache.activemq";
    private final static Log log=LogFactory.getLog(ManagementContext.class);
    private MBeanServer beanServer;
    private String jmxDomainName=DEFAULT_DOMAIN;
    private boolean useMBeanServer=true;
    private boolean createMBeanServer=true;
    private boolean locallyCreateMBeanServer=false;
    private boolean createConnector=true;
    private boolean findTigerMbeanServer=false;
    private int connectorPort=1099;
    private int rmiServerPort;
    private String connectorPath="/jmxrmi";
    private AtomicBoolean started=new AtomicBoolean(false);
    private JMXConnectorServer connectorServer;
    private ObjectName namingServiceObjectName;

    public ManagementContext(){
        this(null);
    }

    public ManagementContext(MBeanServer server){
        this.beanServer=server;
    }

    public void start() throws IOException {
        // lets force the MBeanServer to be created if needed
        if (started.compareAndSet(false, true)) {
            getMBeanServer();
            if (connectorServer != null) {
                try {
                    getMBeanServer().invoke(namingServiceObjectName, "start", null, null);
                }
                catch (Throwable ignore) {
                }
                Thread t = new Thread("JMX connector") {
                    public void run() {
                        try {
                            JMXConnectorServer server = connectorServer;
                            if (started.get() && server != null) {
                                server.start();
                                log.info("JMX consoles can connect to " + server.getAddress());
                            }
                        }
                        catch (IOException e) {
                            log.warn("Failed to start jmx connector: " + e.getMessage());
                        }
                    }
                };
                t.setDaemon(true);
                t.start();
            }
        }
    }

    public void stop() throws IOException {
        if (started.compareAndSet(true, false)) {
            JMXConnectorServer server = connectorServer;
            connectorServer = null;
            if (server != null) {
                try {
                    server.stop();
                }
                catch (IOException e) {
                    log.warn("Failed to stop jmx connector: " + e.getMessage());
                }
                try {
                    getMBeanServer().invoke(namingServiceObjectName, "stop", null, null);
                }
                catch (Throwable ignore) {
                }
            }
            if (locallyCreateMBeanServer && beanServer != null) {
                // check to see if the factory knows about this server
                List list = MBeanServerFactory.findMBeanServer(null);
                if (list != null && !list.isEmpty() && list.contains(beanServer)) {
                    MBeanServerFactory.releaseMBeanServer(beanServer);
                }
            }
        }
    }

    /**
     * @return Returns the jmxDomainName.
     */
    public String getJmxDomainName(){
        return jmxDomainName;
    }

    /**
     * @param jmxDomainName
     *            The jmxDomainName to set.
     */
    public void setJmxDomainName(String jmxDomainName){
        this.jmxDomainName=jmxDomainName;
    }

    /**
     * Get the MBeanServer
     * 
     * @return the MBeanServer
     */
    public MBeanServer getMBeanServer(){
        if(this.beanServer==null){
            this.beanServer=findMBeanServer();
        }
        return beanServer;
    }

    /**
     * Set the MBeanServer
     * 
     * @param beanServer
     */
    public void setMBeanServer(MBeanServer beanServer){
        this.beanServer=beanServer;
    }

    /**
     * @return Returns the useMBeanServer.
     */
    public boolean isUseMBeanServer(){
        return useMBeanServer;
    }

    /**
     * @param useMBeanServer
     *            The useMBeanServer to set.
     */
    public void setUseMBeanServer(boolean useMBeanServer){
        this.useMBeanServer=useMBeanServer;
    }

    /**
     * @return Returns the createMBeanServer flag.
     */
    public boolean isCreateMBeanServer(){
        return createMBeanServer;
    }

    /**
     * @param enableJMX
     *            Set createMBeanServer.
     */
    public void setCreateMBeanServer(boolean enableJMX){
        this.createMBeanServer=enableJMX;
    }

    public boolean isFindTigerMbeanServer() {
        return findTigerMbeanServer;
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
     * @return the JMX ObjectName of the MBean, or <code>null</code> if <code>customName</code> is invalid.
     */
    public ObjectName createCustomComponentMBeanName(String type,String name){
        ObjectName result=null;
        String tmp=jmxDomainName+":"+"type="+sanitizeString(type)+",name="+sanitizeString(name);
        try{
            result=new ObjectName(tmp);
        }catch(MalformedObjectNameException e){
            log.error("Couldn't create ObjectName from: "+type+" , "+name);
        }
        return result;
    }

    /**
     * The ':' and '/' characters are reserved in ObjectNames
     * 
     * @param in
     * @return sanitized String
     */
    private static String sanitizeString(String in){
        String result=null;
        if(in!=null){
            result=in.replace(':','_');
            result=result.replace('/','_');
            result=result.replace('\\','_');
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
    public static ObjectName getSystemObjectName(String domainName,String containerName,Class theClass)
                    throws MalformedObjectNameException,NullPointerException{
        String tmp=domainName+":"+"type="+theClass.getName()+",name="+getRelativeName(containerName,theClass);
        return new ObjectName(tmp);
    }

    private static String getRelativeName(String containerName,Class theClass){
        String name=theClass.getName();
        int index=name.lastIndexOf(".");
        if(index>=0&&(index+1)<name.length()){
            name=name.substring(index+1);
        }
        return containerName+"."+name;
    }

    /**
     * Unregister an MBean
     * 
     * @param name
     * @throws JMException
     */
    public void unregisterMBean(ObjectName name) throws JMException{
        if(beanServer!=null&&beanServer.isRegistered(name)){
            beanServer.unregisterMBean(name);
        }
    }

    protected synchronized MBeanServer findMBeanServer(){
        MBeanServer result=null;
        // create the mbean server
        try{
            if(useMBeanServer){
                if (findTigerMbeanServer) {
                    result = findTigerMBeanServer();
                }
                if (result == null) {
                    // lets piggy back on another MBeanServer -
                    // we could be in an appserver!
                    List list=MBeanServerFactory.findMBeanServer(null);
                    if(list!=null&&list.size()>0){
                        result=(MBeanServer) list.get(0);
                    }
                }
            }
            if (result == null && createMBeanServer) {
                result = createMBeanServer();
            }

            if (result != null && createConnector) {
                createConnector(result);
            }
        }
        catch (NoClassDefFoundError e) {
            log.error("Could not load MBeanServer", e);
        }
        catch (Throwable e) {
            // probably don't have access to system properties
            log.error("Failed to initialize MBeanServer", e);
        }
        return result;
    }

    public static MBeanServer findTigerMBeanServer() {
        String name = "java.lang.management.ManagementFactory";
        Class type = loadClass(name, ManagementContext.class.getClassLoader());
        if (type != null) {
            try {
                Method method = type.getMethod("getPlatformMBeanServer", new Class[0]);
                if (method != null) {
                    Object answer = method.invoke(null, new Object[0]);
                    if (answer instanceof MBeanServer) {
                        return (MBeanServer) answer;
                    }
                    else {
                        log.warn("Could not cast: " + answer + " into an MBeanServer. There must be some classloader strangeness in town");
                    }
                }
                else {
                    log.warn("Method getPlatformMBeanServer() does not appear visible on type: " + type.getName());
                }
            }
            catch (Exception e) {
                log.warn("Failed to call getPlatformMBeanServer() due to: " + e, e);
            }
        }
        else {
            log.trace("Class not found: " + name + " so probably running on Java 1.4");
        }
        return null;
    }

    private static Class loadClass(String name, ClassLoader loader) {
        try {
            return loader.loadClass(name);
        }
        catch (ClassNotFoundException e) {
            try {
                return Thread.currentThread().getContextClassLoader().loadClass(name);
            }
            catch (ClassNotFoundException e1) {
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
    protected MBeanServer createMBeanServer() throws MalformedObjectNameException,IOException{
        MBeanServer mbeanServer=MBeanServerFactory.createMBeanServer(jmxDomainName);
        locallyCreateMBeanServer=true;
        if(createConnector){
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
    private void createConnector(MBeanServer mbeanServer) throws MalformedObjectNameException,MalformedURLException,
                    IOException{
        // Create the NamingService, needed by JSR 160
        try{
            LocateRegistry.createRegistry(connectorPort);
            namingServiceObjectName=ObjectName.getInstance("naming:type=rmiregistry");
            // Do not use the createMBean as the mx4j jar may not be in the
            // same class loader than the server
            Class cl=Class.forName("mx4j.tools.naming.NamingService");
            mbeanServer.registerMBean(cl.newInstance(),namingServiceObjectName);
            // mbeanServer.createMBean("mx4j.tools.naming.NamingService", namingServiceObjectName, null);
            // set the naming port
            Attribute attr=new Attribute("Port",Integer.valueOf(connectorPort));
            mbeanServer.setAttribute(namingServiceObjectName,attr);
        }catch(Throwable e){
            log.debug("Failed to create local registry",e);
        }
        // Create the JMXConnectorServer
	String rmiServer = "";
	if (rmiServerPort != 0) {
	    // This is handy to use if you have a firewall and need to
	    // force JMX to use fixed ports.
	    rmiServer = "localhost:" + rmiServerPort;
	}
	String serviceURL = "service:jmx:rmi://"+rmiServer+"/jndi/rmi://localhost:"+connectorPort+connectorPath;
        JMXServiceURL url=new JMXServiceURL(serviceURL);
        connectorServer=JMXConnectorServerFactory.newJMXConnectorServer(url,null,mbeanServer);
    }

    public String getConnectorPath(){
        return connectorPath;
    }

    public void setConnectorPath(String connectorPath){
        this.connectorPath=connectorPath;
    }

    public int getConnectorPort(){
        return connectorPort;
    }

    public void setConnectorPort(int connectorPort){
        this.connectorPort=connectorPort;
    }

    public int getRmiServerPort(){
        return rmiServerPort;
    }

    public void setRmiServerPort(int rmiServerPort){
        this.rmiServerPort=rmiServerPort;
    }

    public boolean isCreateConnector(){
        return createConnector;
    }

    public void setCreateConnector(boolean createConnector){
        this.createConnector=createConnector;
    }
}
