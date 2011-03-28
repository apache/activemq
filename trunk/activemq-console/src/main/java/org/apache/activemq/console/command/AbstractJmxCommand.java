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
package org.apache.activemq.console.command;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import sun.management.ConnectorAddressLink;

public abstract class AbstractJmxCommand extends AbstractCommand {
    public static final String DEFAULT_JMX_URL    = "service:jmx:rmi:///jndi/rmi://localhost:1099/jmxrmi";

    private JMXServiceURL jmxServiceUrl;
    private String jmxUser;
    private String jmxPassword;
    private boolean jmxUseLocal;
    private JMXConnector jmxConnector;
    private MBeanServerConnection jmxConnection;

    /**
     * Get the current specified JMX service url.
     * @return JMX service url
     */
    protected JMXServiceURL getJmxServiceUrl() {
        return jmxServiceUrl;
    }
    
    public static String getJVM() { 
        return System.getProperty("java.vm.specification.vendor"); 
    } 

    public static boolean isSunJVM() { 
        return getJVM().equals("Sun Microsystems Inc."); 
    }

    /**
     * Get the current JMX service url being used, or create a default one if no JMX service url has been specified.
     * @return JMX service url
     * @throws MalformedURLException
     */
    protected JMXServiceURL useJmxServiceUrl() throws MalformedURLException {
        if (getJmxServiceUrl() == null) {
            String jmxUrl = DEFAULT_JMX_URL;
            int connectingPid = -1;
            if (isSunJVM()) {
                try {
                    // Try to attach to the local process
                    // Classes are all dynamically loaded, since they are specific to Sun VM
                    // if it fails for any reason default jmx url will be used
                	
                	
                    // tools.jar are not always included used by default 
                    // class loader, so we will try to use custom loader that will
                    // try to load tools.jar
                    String javaHome = System.getProperty("java.home");
                    String tools = javaHome + File.separator +
                            ".." + File.separator + "lib" + File.separator + "tools.jar";
                    URLClassLoader loader = new URLClassLoader(new URL[]{new File(tools).toURI().toURL()});
                    
                    // load all classes dynamically so we can compile on non-Sun VMs
                    
                    //MonitoredHost host = MonitoredHost.getMonitoredHost(new HostIdentifier((String)null));
                	Class monitoredHostClass = Class.forName("sun.jvmstat.monitor.MonitoredHost", true, loader);
                	Method getMonitoredHostMethod = monitoredHostClass.getMethod("getMonitoredHost", String.class);
                	Object host = getMonitoredHostMethod.invoke(null, (String)null);
                	//Set vms = host.activeVms();
                	Method activeVmsMethod = host.getClass().getMethod("activeVms", null);
                	Set vms = (Set)activeVmsMethod.invoke(host, null);
                    for (Object vmid: vms) {
                        int pid = ((Integer) vmid).intValue();
                        //MonitoredVm mvm = host.getMonitoredVm(new VmIdentifier(vmid.toString()));
                        Class vmIdentifierClass = Class.forName("sun.jvmstat.monitor.VmIdentifier", true, loader);
                        Constructor vmIdentifierConstructor = vmIdentifierClass.getConstructor(String.class);
                        Object vmIdentifier = vmIdentifierConstructor.newInstance(vmid.toString());
                        Method getMonitoredVmMethod = host.getClass().getMethod("getMonitoredVm", vmIdentifierClass);
                        Object mvm = getMonitoredVmMethod.invoke(host, vmIdentifier);
                        //String name =  MonitoredVmUtil.commandLine(mvm);
                        Class monitoredVmUtilClass = Class.forName("sun.jvmstat.monitor.MonitoredVmUtil", true, loader);
                        Method commandLineMethod = monitoredVmUtilClass.getMethod("commandLine", Class.forName("sun.jvmstat.monitor.MonitoredVm", true, loader));
                        String name = (String)commandLineMethod.invoke(null, mvm);
                        if (name.contains("run.jar start")) {
                        	connectingPid = pid;
                            jmxUrl = ConnectorAddressLink.importFrom(pid);
                            break;
                        }
                    }
                } catch (Exception ignore) {}
            }
            
            if (connectingPid != -1) {
            	context.print("Connecting to pid: " + connectingPid);
            } else {
            	context.print("Connecting to JMX URL: " + jmxUrl);
            }
            setJmxServiceUrl(jmxUrl);
        }

        return getJmxServiceUrl();
    }

    /**
     * Sets the JMX service url to use.
     * @param jmxServiceUrl - new JMX service url to use
     */
    protected void setJmxServiceUrl(JMXServiceURL jmxServiceUrl) {
        this.jmxServiceUrl = jmxServiceUrl;
    }

    /**
     * Sets the JMX service url to use.
     * @param jmxServiceUrl - new JMX service url to use
     * @throws MalformedURLException
     */
    protected void setJmxServiceUrl(String jmxServiceUrl) throws MalformedURLException {
        setJmxServiceUrl(new JMXServiceURL(jmxServiceUrl));
    }

    /**
     * Get the JMX user name to be used when authenticating.
     * @return the JMX user name
     */
    public String getJmxUser() {
        return jmxUser;
    }

    /**
     * Sets the JMS user name to use
     * @param jmxUser - the jmx 
     */
    public void setJmxUser(String jmxUser) {
        this.jmxUser = jmxUser;
    }

    /**
     * Get the password used when authenticating
     * @return the password used for JMX authentication
     */
    public String getJmxPassword() {
        return jmxPassword;
    }

    /**
     * Sets the password to use when authenticating
     * @param jmxPassword - the password used for JMX authentication
     */
    public void setJmxPassword(String jmxPassword) {
        this.jmxPassword = jmxPassword;
    }

    /**
     * Get whether the default mbean server for this JVM should be used instead of the jmx url
     * @return <code>true</code> if the mbean server from this JVM should be used, <code>false<code> if the jmx url should be used
     */
    public boolean isJmxUseLocal() {
        return jmxUseLocal;
    }

    /**
     * Sets whether the the default mbean server for this JVM should be used instead of the jmx url
     * @param jmxUseLocal - <code>true</code> if the mbean server from this JVM should be used, <code>false<code> if the jmx url should be used
     */
    public void setJmxUseLocal(boolean jmxUseLocal) {
        this.jmxUseLocal = jmxUseLocal;
    }

    /**
     * Create a JMX connector using the current specified JMX service url. If there is an existing connection,
     * it tries to reuse this connection.
     * @return created JMX connector
     * @throws IOException
     */
    private JMXConnector createJmxConnector() throws IOException {
        // Reuse the previous connection
        if (jmxConnector != null) {
            jmxConnector.connect();
            return jmxConnector;
        }

        // Create a new JMX connector
        if (jmxUser != null && jmxPassword != null) {
            Map<String,Object> props = new HashMap<String,Object>();
            props.put(JMXConnector.CREDENTIALS, new String[] { jmxUser, jmxPassword });
            jmxConnector = JMXConnectorFactory.connect(useJmxServiceUrl(), props);
        } else {
            jmxConnector = JMXConnectorFactory.connect(useJmxServiceUrl());
        }
        return jmxConnector;
    }

    /**
     * Close the current JMX connector
     */
    protected void closeJmxConnection() {
        try {
            if (jmxConnector != null) {
                jmxConnector.close();
                jmxConnector = null;
            }
        } catch (IOException e) {
        }
    }

    protected MBeanServerConnection createJmxConnection() throws IOException {
        if (jmxConnection == null) {
            if (isJmxUseLocal()) {
                jmxConnection = ManagementFactory.getPlatformMBeanServer();
            } else {
                jmxConnection = createJmxConnector().getMBeanServerConnection();
            }
        }
        return jmxConnection;
    }

    /**
     * Handle the --jmxurl option.
     * @param token - option token to handle
     * @param tokens - succeeding command arguments
     * @throws Exception
     */
    protected void handleOption(String token, List<String> tokens) throws Exception {
        // Try to handle the options first
        if (token.equals("--jmxurl")) {
            // If no jmx url specified, or next token is a new option
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                context.printException(new IllegalArgumentException("JMX URL not specified."));
            }

            // If jmx url already specified
            if (getJmxServiceUrl() != null) {
                context.printException(new IllegalArgumentException("Multiple JMX URL cannot be specified."));
                tokens.clear();
            }

            String strJmxUrl = (String)tokens.remove(0);
            try {
                this.setJmxServiceUrl(new JMXServiceURL(strJmxUrl));
            } catch (MalformedURLException e) {
                context.printException(e);
                tokens.clear();
            }
        } else if(token.equals("--pid")) {
           if (isSunJVM()) {
               if (tokens.isEmpty() || ((String) tokens.get(0)).startsWith("-")) {
                   context.printException(new IllegalArgumentException("pid not specified"));
                   return;
               }
               int pid = Integer.parseInt(tokens.remove(0));
               context.print("Connecting to pid: " + pid);

               String jmxUrl = ConnectorAddressLink.importFrom(pid);
               // If jmx url already specified
               if (getJmxServiceUrl() != null) {
                   context.printException(new IllegalArgumentException("JMX URL already specified."));
                   tokens.clear();
               }
               try {
                   this.setJmxServiceUrl(new JMXServiceURL(jmxUrl));
               } catch (MalformedURLException e) {
                   context.printException(e);
                   tokens.clear();
               }
           }  else {
              context.printInfo("--pid option is not available for this VM, using default JMX url");
           }
        } else if (token.equals("--jmxuser")) {
            // If no jmx user specified, or next token is a new option
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                context.printException(new IllegalArgumentException("JMX user not specified."));
            }
            this.setJmxUser((String) tokens.remove(0));
        } else if (token.equals("--jmxpassword")) {
            // If no jmx password specified, or next token is a new option
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                context.printException(new IllegalArgumentException("JMX password not specified."));
            }
            this.setJmxPassword((String) tokens.remove(0));
        } else if (token.equals("--jmxlocal")) {
            this.setJmxUseLocal(true);
        } else {
            // Let the super class handle the option
            super.handleOption(token, tokens);
        }
    }

    public void execute(List<String> tokens) throws Exception {
        try {
            super.execute(tokens);
        } finally {
            closeJmxConnection();
        }
    }
}
