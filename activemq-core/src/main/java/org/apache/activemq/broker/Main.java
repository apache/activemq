/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */
package org.apache.activemq.broker;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.regex.Pattern;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * Main class that can bootstrap a ActiveMQ Broker. Handles command line
 * argument parsing to set up the broker classpath and System properties.
 *
 * @version $Revision$
 */
public class Main {
    public static final int HELP_MAIN       = 0;
    public static final int HELP_START_TASK = 1;
    public static final int HELP_STOP_TASK  = 2;
    public static final int HELP_LIST_TASK  = 3;
    public static final int HELP_QUERY_TASK = 4;

    public static final int TASK_NONE  = 0;
    public static final int TASK_START = 1;
    public static final int TASK_STOP  = 2;
    public static final int TASK_LIST  = 3;
    public static final int TASK_QUERY = 4;

    public static final String BROKER_FACTORY_CLASS = "org.apache.activemq.broker.BrokerFactory";
    public static final String DEFAULT_CONFIG_URI   = "xbean:activemq.xml";
    public static final String DEFAULT_JMX_URL      = "service:jmx:rmi:///jndi/rmi://localhost:1099/jmxconnector";
    public static final String DEFAULT_JMX_DOMAIN   = "org.apache.activemq";

    private static final String DEFAULT_KEY_BROKER_NAME = "BrokerName";

    // Predefined type=identifier query
    private static final Properties PREDEFINED_OBJNAME_QUERY = new Properties();

    static
    {
        PREDEFINED_OBJNAME_QUERY.setProperty("Broker",           "Type=Broker,BrokerName=%1,*");
        PREDEFINED_OBJNAME_QUERY.setProperty("Connection",       "Type=Connection,Connection=%1,*");
        PREDEFINED_OBJNAME_QUERY.setProperty("Connector",        "Type=Connector,ConnectorName=%1,*");
        PREDEFINED_OBJNAME_QUERY.setProperty("NetworkConnector", "Type=NetworkConnector,BrokerName=%1,*");
        PREDEFINED_OBJNAME_QUERY.setProperty("Queue",            "Type=Queue,Destination=%1,*");
        PREDEFINED_OBJNAME_QUERY.setProperty("Topic",            "Type=Topic,Destination=%1,*");
    };

    private final List brokers         = new ArrayList(5);
    private final List extensions      = new ArrayList(5);
    private final List queryAddObjects = new ArrayList(10);
    private final List querySubObjects = new ArrayList(10);
    private final List queryViews      = new ArrayList(10);

    private int     taskType  = TASK_NONE;
    private boolean stopAll   = false;
    private boolean printHelp = false;
    private boolean printVer  = false;

    private JMXServiceURL jmxUrl;
    private URI           configURI;
    private File          activeMQHome;
    private ClassLoader   classLoader;

    public static void main(String[] args) {
        Main app = new Main();

        // Convert arguments to collection for easier management
        List tokens =  new LinkedList(Arrays.asList(args));

        // First token should be task type (start|stop|list|query)
        app.setTaskType(app.parseTask(tokens));

        // Succeeding tokens should be task specific options identified by "-" at the start
        app.parseOptions(tokens);

        // If display version is set, display and quit no matter the task
        if (app.isPrintVersion()) {
            app.printVersion();
            return;
        }

        // Display the main help, if there is no selected task and help flag is set
        if (app.getTaskType()==TASK_NONE && app.isPrintHelp()) {
            app.printHelp(HELP_MAIN);
            return;
        }

        // Succeeding tokens should be the task data
        switch (app.getTaskType()) {
            case  TASK_START:
                // Print start task help
                if (app.isPrintHelp()) {
                    app.printHelp(HELP_START_TASK);

                // Run start broker task
                } else {
                    try {
                        app.taskStart(tokens);
                    } catch (Throwable e) {
                        System.out.println("Failed to execute start task. Reason: " + e);
                    }
                }
                break;

            case  TASK_STOP:
                // Print stop task help
                if (app.isPrintHelp()) {
                    app.printHelp(HELP_STOP_TASK);

                // Run stop broker task
                } else {
                    try {
                        app.taskStop(tokens);
                    } catch (Throwable e) {
                        System.out.println("Failed to execute stop task. Reason: " + e);
                    }
                }
                break;

            case  TASK_LIST:
                // Print list broker help
                if (app.isPrintHelp()) {
                    app.printHelp(HELP_LIST_TASK);

                // Run list task
                } else {
                    try {
                        app.taskList();
                    } catch (Throwable e) {
                        e.printStackTrace();
                        System.out.println("Failed to execute list task. Reason: " + e);
                    }
                }
                break;

            case  TASK_QUERY:
                // Print query broker help
                if (app.isPrintHelp()) {
                    app.printHelp(HELP_QUERY_TASK);

                // Run query task
                } else {
                    try {
                        app.taskQuery();
                    } catch (Throwable e) {
                        System.out.println("Failed to execute query task. Reason: " + e);
                    }
                }
                break;

            case TASK_NONE:
                break;
            default:
                app.printHelp(HELP_MAIN);
                break;
        }
    }

    public int parseTask(List tokens) {
        if (tokens.isEmpty()) {
            // If no defined arguments, assume start task and default uri
            return TASK_START;
        }

        // Process task token
        String taskToken = (String)tokens.remove(0);

        if (taskToken.equals("start")) {
            return TASK_START;
        } else if (taskToken.equals("stop")) {
            return TASK_STOP;
        } else if (taskToken.equals("list")) {
            return TASK_LIST;
        } else if (taskToken.equals("query")) {
            return TASK_QUERY;
        } else {
            // If not valid task, push back to list
            tokens.add(0, taskToken);
            return TASK_NONE;
        }
    }

    public void parseOptions(List tokens) {
        String token;

        while (!tokens.isEmpty()) {
            token = (String)tokens.remove(0);

            // If token is an option
            if (token.startsWith("-")) {

                // If token is a help option
                if (token.equals("-h") || token.equals("-?") || token.equals("--help")) {
                    printHelp = true;
                    tokens.clear();
                    return;

                // If token is a version option
                } else if (token.equals("--version")) {
                    printVer = true;
                    tokens.clear();
                    return;

                // If token is an extension dir option
                } else if (token.equals("--extdir")) {
                    if(!canUseExtdir()) {
                        printError("Extension directory feature not available due to the system classpath being able to load: " + BROKER_FACTORY_CLASS);
                        tokens.clear();
                        return;
                    }

                    // If no extension directory is specified, or next token is another option
                    if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                        printError("Extension directory not specified.");
                        return;
                    }

                    // Process extension dir token
                    File extDir = new File((String)tokens.remove(0));
                    if (!extDir.isDirectory()) {
                        printError("Extension directory specified is not valid directory: " + extDir);
                        return;
                    }

                    addExtensionDirectory(extDir);
                }

                // If token is a system property define option
                else if (token.startsWith("-D")) {
                    String key = token.substring(2);
                    String value = "";
                    int pos = key.indexOf("=");
                    if (pos >= 0) {
                        value = key.substring(pos + 1);
                        key = key.substring(0, pos);
                    }
                    System.setProperty(key, value);
                }

                // If token is a predefined query define option
                else if (token.startsWith("-Q")) {
                    String key = token.substring(2);
                    String value = "";
                    int pos = key.indexOf("=");
                    if (pos >= 0) {
                        value = key.substring(pos + 1);
                        key = key.substring(0, pos);
                    }

                    // If subtractive query
                    if (key.startsWith("!")) {
                        // Transform predefined query to object name query
                        String predefQuery = PREDEFINED_OBJNAME_QUERY.getProperty(key.substring(1));
                        if (predefQuery == null) {
                            printError("Unknown query object type: " + key.substring(1));
                            return;
                        }
                        String queryStr = createQueryString(predefQuery, value);
                        querySubObjects.add(queryStr);
                    }

                    // If additive query
                    else {
                        // Transform predefined query to object name query
                        String predefQuery = PREDEFINED_OBJNAME_QUERY.getProperty(key);
                        if (predefQuery == null) {
                            printError("Unknown query object type: " + key);
                            return;
                        }
                        String queryStr = createQueryString(predefQuery, value);
                        queryAddObjects.add(queryStr);
                    }
                }

                // If token is an object name query option
                else if (token.startsWith("--objname")) {

                    // If no object name query is specified, or next token is a new option
                    if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                        printError("Object name query not specified");
                        return;
                    }

                    String queryString = (String)tokens.remove(0);

                    // If subtractive query
                    if (queryString.startsWith("!")) {
                        querySubObjects.add(queryString.substring(1));

                    // If additive query
                    } else {
                        queryAddObjects.add(queryString);
                    }
                }

                // If token is a view option
                else if (token.startsWith("--view")) {

                    // If no view specified, or next token is a new option
                    if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                        printError("Attributes to view not specified");
                        return;
                    }

                    // Add the attributes to view
                    Enumeration viewTokens = new StringTokenizer((String)tokens.remove(0), ",", false);
                    while (viewTokens.hasMoreElements()) {
                        queryViews.add(viewTokens.nextElement());
                    }
                }

                // If token is a JMX URL option
                else if (token.startsWith("--jmxurl")) {

                    // If no jmx url specified, or next token is a new option
                    if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                        printError("JMX URL not specified.");
                        return;
                    }

                    // If jmx url already specified
                    if (getJmxUrl() != null) {
                        printError("Multiple JMX URL cannot be specified.");
                        tokens.clear();
                        return;
                    }

                    String strJmxUrl = (String)tokens.remove(0);
                    try {
                        this.setJmxUrl(new JMXServiceURL(strJmxUrl));
                    } catch (MalformedURLException e) {
                        printError("Invalid JMX URL format: " + strJmxUrl);
                        tokens.clear();
                        return;
                    }

                // If token is stop all broker option
                } else if (token.equals("--all")) {
                    stopAll = true;

                } else {
                    System.out.println("Ignoring unrecognized option: " + token);
                }

            // Finish parsing options
            } else {
                // If not valid option, push back to list
                tokens.add(0, token);
                return;
            }
        }
    }

    protected void taskStart(List brokerURIs) throws Throwable {

        // Flag an error if there are multiple configuration uris
//        if (brokerURIs.size() > 1) {
//            printError("Multiple configuration uris or broker names cannot be specified.");
//            brokerURIs.clear();
//            return;
//        }

        // Add the default directories.
        if(canUseExtdir()) {
            addExtensionDirectory(new File(getActiveMQHome(), "conf"));
            addExtensionDirectory(new File(getActiveMQHome(), "lib"));
            addExtensionDirectory(new File(new File(getActiveMQHome(), "lib"), "optional"));
        }

        // If no config uri, use default setting
        if (brokerURIs.isEmpty()) {
            setConfigUri(getDefaultUri());
            startBroker(getConfigUri());

        // Set configuration data, if available, which in this case would be the config URI
        } else {
            String strConfigURI;

            while (!brokerURIs.isEmpty()) {
                strConfigURI = (String)brokerURIs.remove(0);

                try {
                    setConfigUri(new URI(strConfigURI));
                } catch (URISyntaxException e) {
                    printError("Invalid broker configuration URI: " + strConfigURI + ", reason: " + e.getMessage());
                    return;
                }

                startBroker(getConfigUri());
            }
        }

        // Prevent the main thread from exiting unless it is terminated elsewhere
        waitForShutdown();
    }

    protected void taskStop(List brokerNames) throws Throwable {
        // Check if there is a user-specified JMX URL
        if (getJmxUrl() == null) {
            setJmxUrl(getDefaultJmxUrl());
        }

        // Stop all brokers
        if (isStopAllBrokers()) {
            JMXConnector jmxConnector = JMXConnectorFactory.connect(getJmxUrl());
            MBeanServerConnection server = jmxConnector.getMBeanServerConnection();

            ObjectName brokerObjName = new ObjectName(DEFAULT_JMX_DOMAIN + ":Type=Broker,*");

            stopBroker(server, brokerObjName);

            brokerNames.clear();

            // Maybe no need to close, since context is already closed by broker
            //jmxConnector.close();

            return;
        }

        // Stop the default broker
        if (brokerNames.isEmpty()) {
            Set brokerList = getBrokerList(getJmxUrl());

            // If there is no broker to stop
            if (brokerList.isEmpty()) {
                System.out.println("There are no brokers to stop.");
                return;

            // There should only be one broker to stop
            } else if (brokerList.size() > 1) {
                System.out.println("There are multiple brokers to stop. Please select the broker(s) to stop or use --all to stop all brokers.");
                System.out.println();
                printBrokerList(brokerList);
                return;

            // Stop the only running broker
            } else {
                Iterator brokerIter = brokerList.iterator();

                JMXConnector jmxConnector = JMXConnectorFactory.connect(getJmxUrl());
                MBeanServerConnection server = jmxConnector.getMBeanServerConnection();

                this.stopBroker(server, ((ObjectInstance)brokerIter.next()).getObjectName());

                // Maybe no need to close, since context is already closed by broker
                //jmxConnector.close();
                return;
            }
        }

        // Stop each specified broker
        String brokerName;

        JMXConnector jmxConnector = JMXConnectorFactory.connect(getJmxUrl());
        MBeanServerConnection server = jmxConnector.getMBeanServerConnection();

        while (!brokerNames.isEmpty()) {
            brokerName = (String)brokerNames.remove(0);
            stopBroker(server, brokerName);
        }

        // Maybe be no need to close, since context is already closed by broker
        //jmxConnector.close();
    }

    protected void taskList() throws Throwable {
        // Check if there is a user-specified JMX URL
        if (getJmxUrl() == null) {
            setJmxUrl(getDefaultJmxUrl());
        }

        printBrokerList(getBrokerList(getJmxUrl()));
    }

    protected void taskQuery() throws Throwable {
        // Check if there is a user-specified JMX URL
        if (getJmxUrl() == null) {
            setJmxUrl(getDefaultJmxUrl());
        }

        // Connect to jmx server
        JMXConnector jmxConnector = JMXConnectorFactory.connect(getJmxUrl());
        MBeanServerConnection server = jmxConnector.getMBeanServerConnection();

        // Query for the mbeans to add
        Set addMBeans = queryMBeans(server, getAddQueryObjects());

        // Query for the mbeans to sub
        if (getSubQueryObjects().size() > 0) {
            Set subMBeans = queryMBeans(server, getSubQueryObjects());
            addMBeans.removeAll(subMBeans);
        }

        for (Iterator i=addMBeans.iterator(); i.hasNext();) {
            ObjectInstance mbean = (ObjectInstance)i.next();
            printMBeanProp(mbean, null);
            printMBeanAttr(server, mbean, getQueryViews());
        }

        jmxConnector.close();
    }

    public void startBroker(URI configURI) throws Throwable {
        System.out.println("Loading Message Broker from: " + configURI);
        System.out.println("ACTIVEMQ_HOME: "+ getActiveMQHome());

        ClassLoader cl = getClassLoader();

        // Use reflection to start the broker up.
        Object broker;
        try {
            Class brokerFactory = cl.loadClass(BROKER_FACTORY_CLASS);
            Method createBroker = brokerFactory.getMethod("createBroker", new Class[] { URI.class });
            broker = createBroker.invoke(null, new Object[] { configURI });
            brokers.add(broker);

            Method start = broker.getClass().getMethod("start", new Class[]{});
            start.invoke(broker, new Object[]{});

        } catch (InvocationTargetException e) {
            throw e.getCause();
        } catch (Throwable e) {
            throw e;
        }
    }

    public void addExtensionDirectory(File directory) {
        extensions.add(directory);
    }

    protected void waitForShutdown() throws Throwable {
        // Prevent the main thread from exiting, in case this is the last user thread
        final boolean[] shutdown = new boolean[] {false};
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                synchronized(shutdown) {
                    shutdown[0]=true;
                    shutdown.notify();
                }
            }
        });

        synchronized(shutdown) {
            while( !shutdown[0] ) {
                shutdown.wait();
            }
        }

        // Use reflection to stop the broker in case, the vm was exited via unconventional means
        try {
            for (Iterator i=brokers.iterator(); i.hasNext();) {
                Object broker = i.next();
                Method stop = broker.getClass().getMethod("stop", new Class[] {});
                stop.invoke(broker, new Object[] {});
            }
        } catch (InvocationTargetException e) {
            throw e.getCause();
        } catch (Throwable e) {
            throw e;
        }
    }

    public void stopBroker(MBeanServerConnection server, String brokerName) {
        ObjectName brokerObjName = null;
        try {
            brokerObjName = new ObjectName(DEFAULT_JMX_DOMAIN + ":Type=Broker," + DEFAULT_KEY_BROKER_NAME + "=" + brokerName);
        } catch (Exception e) {
            System.out.println("Invalid broker name: " + brokerName);
            return;
        }
        stopBroker(server, brokerObjName);
    }

    public void stopBroker(MBeanServerConnection server, ObjectName brokerObjName) {
        try {
            Set brokerList = server.queryMBeans(brokerObjName, null);

            // Stop each broker that matches the object name
            for (Iterator i=brokerList.iterator(); i.hasNext();) {
                ObjectName broker = ((ObjectInstance)i.next()).getObjectName();

                String brokerName = broker.getKeyProperty(DEFAULT_KEY_BROKER_NAME);
                System.out.println("Stopping broker: " + brokerName);
                try {
                    server.invoke(broker, "terminateJVM", new Object[] {new Integer(0)}, new String[] {"int"});
                    System.out.println("Succesfully stopped broker: " + brokerName);
                } catch (Exception e) {
                    // TODO: Check exceptions throwned
                    //System.out.println("Failed to stop broker: [ " + brokerName + " ]. Reason: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.out.println("Failed to execute stop task. Reason: " + e);
            return;
        }
    }

    public Set queryMBeans(MBeanServerConnection server, List queryList) throws Exception {
        Set mbeans;

        // If there is no query defined get all mbeans
        if (queryList==null || queryList.size()==0) {
            ObjectName queryName = new ObjectName(DEFAULT_JMX_DOMAIN + ":*");

            mbeans = server.queryMBeans(queryName, null);

        // Construct the object name based on the query
        } else {
            mbeans = new HashSet();

            for (Iterator i=queryList.iterator(); i.hasNext();) {
                String queryStr = (String)i.next();

                // Transform string to support regex filtering
                List regexProp = new ArrayList();
                queryStr = createSimpleRegExQuery(queryStr, regexProp);

                ObjectName queryName = new ObjectName(DEFAULT_JMX_DOMAIN + ":" + queryStr);
                mbeans.addAll(filterUsingRegEx(server.queryMBeans(queryName, null), regexProp));
            }
        }

        return mbeans;
    }

    public Map queryMBeanAttrs(MBeanServerConnection server, ObjectName mbeanObjName, List attrView) throws Exception {
        Map attr = new HashMap();
        MBeanAttributeInfo[] attrs = server.getMBeanInfo(mbeanObjName).getAttributes();

        // If the mbean has no attribute, print a no attribute message
        if (attrs.length == 0) {
            return null;
        }

        // If there is no view specified, get all attributes
        if (attrView == null || attrView.isEmpty()) {
            for (int i=0; i<attrs.length; i++) {
                Object attrVal = server.getAttribute(mbeanObjName, attrs[i].getName());
                attr.put(attrs[i].getName(), attrVal);
            }
            return attr;
        }

        // Get attributes specified by view
        for (int i=0; i<attrs.length; i++) {
            if (attrView.contains(attrs[i].getName())) {
                Object attrVal = server.getAttribute(mbeanObjName, attrs[i].getName());
                attr.put(attrs[i].getName(), attrVal);
            }
        }

        return attr;
    }

    protected String createQueryString(String query, String param) {
        return query.replaceAll("%1", param);
    }

    protected String createQueryString(String query, List params) {

        int count = 1;
        for (Iterator i=params.iterator();i.hasNext();) {
            query.replaceAll("%" + count++, i.next().toString());
        }

        return query;
    }

    protected String createSimpleRegExQuery(String query, List regExMap) throws Exception {
        if (regExMap==null) {
            regExMap = new ArrayList();
        }

        StringBuffer newQueryStr = new StringBuffer();

        for (StringTokenizer tokenizer = new StringTokenizer(query, ","); tokenizer.hasMoreTokens();) {
            String token = tokenizer.nextToken();

            // Get key value pair
            String key = token;
            String value = "";
            int pos = key.indexOf("=");
            if (pos >= 0) {
                value = key.substring(pos + 1);
                key = key.substring(0, pos);
            }

            // Check if value is a wildcard query
            if ((value.indexOf("*") >= 0) || (value.indexOf("?") >= 0)) {
                // If value is a wildcard query, convert to regex
                // and remove the object name query to ensure it selects all
                regExMap.add(Pattern.compile("(.*)(" + key + "=)(" + transformWildcardQueryToRegEx(value) + ")(,)(.*)"));

            // Re-add valid key value pair. Remove all * property and just add one at the end.
            } else if ((key != "") && (value != "")) {
                newQueryStr.append(key + "=" + value + ",");
            }
        }

        newQueryStr.append("*");
        return newQueryStr.toString();
    }

    protected String transformWildcardQueryToRegEx(String query) {
        query = query.replaceAll("[.]", "\\\\."); // Escape all dot characters. From (.) to (\.)
        query = query.replaceAll("[?]", ".");
        query = query.replaceAll("[*]", ".*?"); // Use reluctant quantifier

        return query;
    }

    protected Set filterUsingRegEx(Set mbeans, List regexProp) {
        // No regular expressions filtering needed
        if (regexProp==null || regexProp.isEmpty()) {
            return mbeans;
        }

        Set filteredMbeans = new HashSet();

        // Get each bean to filter
        for (Iterator i=mbeans.iterator(); i.hasNext();) {
            ObjectInstance mbeanInstance = (ObjectInstance)i.next();
            String mbeanName = mbeanInstance.getObjectName().getKeyPropertyListString();

            // Ensure name ends with ,* to guarantee correct parsing behavior
            if (!mbeanName.endsWith(",*")) {
                mbeanName = mbeanName + ",*";
            }
            boolean match = true;

            // Match the object name to each regex
            for (Iterator j=regexProp.iterator(); j.hasNext();) {
                Pattern p = (Pattern)j.next();

                if (!p.matcher(mbeanName).matches()) {
                    match = false;
                    break;
                }
            }

            // If name of mbean matches all regex pattern, add it
            if (match) {
                filteredMbeans.add(mbeanInstance);
            }
        }

        return filteredMbeans;
    }

    /**
     * The extension directory feature will not work if the broker factory is already in the classpath
     * since we have to load him from a child ClassLoader we build for it to work correctly.
     *
     * @return
     */
    public boolean canUseExtdir() {
        try {
            Main.class.getClassLoader().loadClass(BROKER_FACTORY_CLASS);
            return false;
        } catch (ClassNotFoundException e) {
            return true;
        }
    }

    public void printHelp(int helpIndex) {
        for (int i=0; i<taskHelp[helpIndex].length; i++) {
            System.out.println(taskHelp[helpIndex][i]);
        }
    }

    public void printAllHelp() {
        for (int i=0; i<taskHelp.length; i++) {
            printHelp(i);
        }
    }

    public void printError(String message) {
        System.out.println(message);
        System.out.println();
        setTaskType(TASK_NONE);
    }

    public void printVersion() {
        System.out.println();
        try {
            System.out.println("ActiveMQ " + getVersion());
        } catch (Throwable e) {
            System.out.println("ActiveMQ <unknown version>");
        }
        System.out.println("For help or more information please see: http://www.logicblaze.com");
        System.out.println();
    }

    public void printBrokerList(Set brokerList) {
        Object[] brokerArray = brokerList.toArray();

        System.out.println("List of available brokers:");
        for (int i=0; i<brokerArray.length; i++) {
            String brokerName = ((ObjectInstance)brokerArray[i]).getObjectName().getKeyProperty("BrokerName");
            System.out.println("    " + (i+1) + ".) " + brokerName);
        }
    }

    public void printMBeanProp(ObjectInstance mbean, List propView) {
        // Filter properties to print
        if (propView != null && !propView.isEmpty()) {
            Map mbeanProps = mbean.getObjectName().getKeyPropertyList();
            for (Iterator i=propView.iterator(); i.hasNext();) {
                Object key = i.next();
                Object val = mbeanProps.get(key);

                if (val != null) {
                    System.out.println("MBean " + key + ": " + val);
                }
            }

        // Print all properties
        } else {
            Map mbeanProps = mbean.getObjectName().getKeyPropertyList();
            for (Iterator i=mbeanProps.keySet().iterator(); i.hasNext();) {
                Object key = i.next();
                Object val = mbeanProps.get(key);

                System.out.println("MBean " + key + ": " + val);
            }
        }
    }

    public void printMBeanAttr(MBeanServerConnection server, ObjectInstance mbean, List attrView) {

        try {
            Map attrList = queryMBeanAttrs(server, mbean.getObjectName(), attrView);

            // If the mbean has no attribute, print a no attribute message
            if (attrList == null) {
                System.out.println("    MBean has no attributes.");
                System.out.println();
                return;
            }

            // If the mbean's attributes did not match any of the view, display a message
            if (attrList.isEmpty()) {
                System.out.println("    View did not match any of the mbean's attributes.");
                System.out.println("");
                return;
            }

            // Display mbean attributes

            // If attrView is available, use it. This allows control over the display order
            if (attrView != null && !attrView.isEmpty()) {
                for (Iterator i=attrView.iterator(); i.hasNext();) {
                    Object key = i.next();
                    Object val = attrList.get(key);

                    if (val != null) {
                        System.out.println("    " + key + " = " + attrList.get(key));
                    }
                }

            // If attrView is not available, print all attributes
            } else {
                for (Iterator i=attrList.keySet().iterator(); i.hasNext();) {
                    Object key = i.next();
                    System.out.println("    " + key + " = " + attrList.get(key));
                }
            }
            System.out.println("");
            
        } catch (Exception e) {
            System.out.println("Failed to print mbean attributes. Reason: " + e.getMessage());
        }
    }

    // Property setters and getters
    public void setTaskType(int taskType) {
        this.taskType = taskType;
    }

    public int getTaskType() {
        return taskType;
    }

    public void setJmxUrl(JMXServiceURL url) {
        jmxUrl = url;
    }

    public JMXServiceURL getJmxUrl() {
        return jmxUrl;
    }

    public JMXServiceURL getDefaultJmxUrl() throws MalformedURLException {
        return new JMXServiceURL(DEFAULT_JMX_URL);
    }

    public String getVersion() throws Throwable {
        ClassLoader cl = getClassLoader();
        // Use reflection to get the version
        try {
            Class activeMQConnectionMetaData = cl.loadClass("org.apache.activemq.ActiveMQConnectionMetaData");
            Field field = activeMQConnectionMetaData.getField("PROVIDER_VERSION");
            return (String)field.get(null);
        } catch (Throwable e) {
            throw e;
        }
    }

    public URI getDefaultUri() throws URISyntaxException{
        return new URI(DEFAULT_CONFIG_URI);
    }

    public void setConfigUri(URI uri) {
        configURI = uri;
    }

    public URI getConfigUri() {
        return configURI;
    }

    public boolean isStopAllBrokers() {
        return stopAll;
    }

    public boolean isPrintVersion() {
        return printVer;
    }

    public boolean isPrintHelp() {
        return printHelp;
    }

    public List getAddQueryObjects() {
        return queryAddObjects;
    }

    public List getSubQueryObjects() {
        return querySubObjects;
    }

    public List getQueryViews() {
        return queryViews;
    }

    public ClassLoader getClassLoader() throws MalformedURLException {
        if(classLoader==null) {
            //
            // Setup the ClassLoader
            //
            classLoader = Main.class.getClassLoader();
            if (!extensions.isEmpty()) {

                ArrayList urls = new ArrayList();
                for (Iterator iter = extensions.iterator(); iter.hasNext();) {
                    File dir = (File) iter.next();
                    urls.add(dir.toURL());
                    File[] files = dir.listFiles();
                    if( files!=null ) {
                        for (int j = 0; j < files.length; j++) {
                            if( files[j].getName().endsWith(".zip") || files[j].getName().endsWith(".jar") ) {
                                urls.add(files[j].toURL());
                            }
                        }
                    }
                }

                URL u[] = new URL[urls.size()];
                urls.toArray(u);
                classLoader = new URLClassLoader(u, classLoader);
            }
            Thread.currentThread().setContextClassLoader(classLoader);
        }
        return classLoader;
    }


    public void setActiveMQHome(File activeMQHome) {
        this.activeMQHome = activeMQHome;
    }

    public File getActiveMQHome() {
        if(activeMQHome==null) {
            if(System.getProperty("activemq.home") != null) {
                activeMQHome = new File(System.getProperty("activemq.home"));
            }

            if(activeMQHome==null){
                // guess from the location of the jar
                URL url = Main.class.getClassLoader().getResource("org/apache/activemq/broker/Main.class");
                if (url != null) {
                    try {
                        JarURLConnection jarConnection = (JarURLConnection) url.openConnection();
                        url = jarConnection.getJarFileURL();
                        URI baseURI = new URI(url.toString()).resolve("..");
                        activeMQHome = new File(baseURI).getCanonicalFile();
                    } catch (Exception ignored) {
                    }
                }
            }

            if(activeMQHome==null){
                activeMQHome = new File(".");
            }
        }
        return activeMQHome;
    }

    public Set getBrokerList(JMXServiceURL jmxUrl) throws Throwable {
        JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxUrl);
        MBeanServerConnection server = jmxConnector.getMBeanServerConnection();

        ObjectName brokerObjName = new ObjectName(DEFAULT_JMX_DOMAIN + ":Type=Broker,*");

        Set brokerMBeans = server.queryMBeans(brokerObjName, null);

        jmxConnector.close();

        return brokerMBeans;
    }

    // This section contains an array of the help notes of the different tasks
    private static final String[][] taskHelp = {
        // Main task help
        {
            "Usage: Main [task] [task-options] [task data]",
            "",
            "Tasks (default task is start):",
            "    start        - Creates and starts a broker using a configuration file, or a broker URI.",
            "    stop         - Stops a running broker specified by the broker name.",
            "    list         - Lists all available broker in the specified JMX context.",
            "    query        - Display selected broker component's attributes and statistics.",
            "    --version    - Display the version information.",
            "    -h,-?,--help - Display this help information. To display task specific help, use Main [task] -h,-?,--help",
            "",
            "Task Options:",
            "    - Properties specific to each task.",
            "",
            "Task Data:",
            "    - Information needed by each specific task.",
            ""
        },

        // Start broker task help
        {
            "Task Usage: Main start [start-options] [uri]",
            "Description: Creates and starts a broker using a configuration file, or a broker URI.",
            "",
            "Start Options:",
            "    --extdir <dir>        Add the jar files in the directory to the classpath.",
            "    -D<name>=<value>      Define a system property.",
            "    --version             Display the version information.",
            "    -h,-?,--help          Display the start broker help information.",
            "",
            "URI:",
            "",
            "    XBean based broker configuration:",
            "",
            "        Example: Main xbean:file:activemq.xml",
            "            Loads the xbean configuration file from the current working directory",
            "        Example: Main xbean:activemq.xml",
            "            Loads the xbean configuration file from the classpath",
            "",
            "    URI Parameter based broker configuration:",
            "",
            "        Example: Main broker:(tcp://localhost:61616, tcp://localhost:5000)?useJmx=true",
            "            Configures the broker with 2 transport connectors and jmx enabled",
            "        Example: Main broker:(tcp://localhost:61616, network:tcp://localhost:5000)?persistent=false",
            "            Configures the broker with 1 transport connector, and 1 network connector and persistence disabled",
            ""
        },

        // Stop broker task help
        {
            "Task Usage: Main stop [stop-options] [broker-name1] [broker-name2] ...",
            "Description: Stops a running broker.",
            "",
            "Stop Options:",
            "    --jmxurl <url>      Set the JMX URL to connect to.",
            "    --all               Stop all brokers.",
            "    --version           Display the version information.",
            "    -h,-?,--help        Display the stop broker help information.",
            "",
            "Broker Names:",
            "    Name of the brokers that will be stopped.",
            "    If omitted, it is assumed that there is only one broker running, and it will be stopped.",
            "    Use -all to stop all running brokers.",
            ""
        },

        // List brokers task help
        {
            "Task Usage: Main list [list-options]",
            "Description:  Lists all available broker in the specified JMX context.",
            "",
            "List Options:",
            "    --jmxurl <url>      Set the JMX URL to connect to.",
            "    --version           Display the version information.",
            "    -h,-?,--help        Display the stop broker help information.",
            "",
        },

        // Query brokers task help
        {
            "Task Usage: Main query [query-options]",
            "Description: Display selected broker component's attributes and statistics.",
            "",
            "Query Options:",
            "    -Q<type>=<name>               Add to the search list the specific object type matched by the defined object identifier.",
            "    -Q!<type>=<name>              Remove from the search list the specific object type matched by the object identifier.",
            "    --objname <query>             Add to the search list objects matched by the query similar to the JMX object name format.",
            "    --objname !<query>            Remove from the search list objects matched by the query similar to the JMX object name format.",
            "    --view <attr1>,<attr2>,...    Select the specific attribute of the object to view. By default all attributes will be displayed.",
            "    --jmxurl <url>                Set the JMX URL to connect to.",
            "    --version                     Display the version information.",
            "    -h,-?,--help                  Display the query broker help information.",
            "",
            "Examples:",
            "    Main query",
            "        - Print all the attributes of all registered objects (queues, topics, connections, etc).",
            "",
            "    Main query -QQueue=TEST.FOO",
            "        - Print all the attributes of the queue with destination name TEST.FOO.",
            "",
            "    Main query -QTopic=*",
            "        - Print all the attributes of all registered topics.",
            "",
            "    Main query --view EnqueueCount,DequeueCount",
            "        - Print the attributes EnqueueCount and DequeueCount of all registered objects.",
            "",
            "    Main -QTopic=* --view EnqueueCount,DequeueCount",
            "        - Print the attributes EnqueueCount and DequeueCount of all registered topics.",
            "",
            "    Main -QTopic=* -QQueue=* --view EnqueueCount,DequeueCount",
            "        - Print the attributes EnqueueCount and DequeueCount of all registered topics and queues.",
            "",
            "    Main -QTopic=* -Q!Topic=ActiveMQ.Advisory.*",
            "        - Print all attributes of all topics except those that has a name that begins with \"ActiveMQ.Advisory\".",
            "",
            "    Main --objname Type=*Connect*,BrokerName=local* -Q!NetworkConnector=*",
            "        - Print all attributes of all connectors, connections excluding network connectors that belongs to the broker that begins with local.",
            "",
            "    Main -QQueue=* -Q!Queue=????",
            "        - Print all attributes of all queues except those that are 4 letters long.",
            "",
        }
    };
}
