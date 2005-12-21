/**
 * <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
 *
 * Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
package org.activemq.broker;

import javax.management.remote.JMXServiceURL;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ObjectInstance;
import javax.management.MBeanAttributeInfo;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.JarURLConnection;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

/**
 * Main class that can bootstrap a ActiveMQ Broker. Handles command line
 * argument parsing to set up the broker classpath and System properties.
 *
 * @version $Revision$
 */
public class Main {
    public static final int HELP_MAIN_APP     = 0;
    public static final int HELP_START_BROKER = 1;
    public static final int HELP_STOP_BROKER  = 2;
    public static final int HELP_LIST_BROKER  = 3;
    public static final int HELP_STAT_BROKER  = 4;
    public static final int HELP_QUERY_BROKER = 5;

    public static final int TASK_NONE              = 0;
    public static final int TASK_START_BROKER      = 1;
    public static final int TASK_STOP_BROKER       = 2;
    public static final int TASK_LIST_BROKER       = 3;
    public static final int TASK_STAT_BROKER       = 4;
    public static final int TASK_QUERY_BROKER      = 5;
    public static final int TASK_PRINT_MAIN_HELP   = 6;
    public static final int TASK_PRINT_START_HELP  = 7;
    public static final int TASK_PRINT_STOP_HELP   = 8;
    public static final int TASK_PRINT_LIST_HELP   = 9;
    public static final int TASK_PRINT_STAT_HELP   = 10;
    public static final int TASK_PRINT_QUERY_HELP  = 11;
    public static final int TASK_PRINT_ALL_HELP    = 12;
    public static final int TASK_PRINT_VER         = 13;

    public static final String BROKER_FACTORY_CLASS = "org.activemq.broker.BrokerFactory";
    public static final String DEFAULT_CONFIG_URI   = "xbean:activemq.xml";
    public static final String DEFAULT_JMX_URL      = "service:jmx:rmi:///jndi/rmi://localhost:1099/jmxconnector";
    public static final String DEFAULT_JMX_DOMAIN   = "org.activemq";

    private static final String   DEFAULT_KEY_BROKER_NAME    = "BrokerName";
    private static final String   DEFAULT_METHOD_BROKER_STOP = "terminateJVM";
    private static final Object[] DEFAULT_PARAM_BROKER_STOP  = new Object[] {new Integer(0)};
    private static final String[] DEFAULT_SIGN_BROKER_STOP   = new String[] {"int"};

    // Stat retrieve flags
    private static final int STAT_BROKER  = Integer.parseInt("0001", 2);
    private static final int STAT_ALL     = Integer.parseInt("1111", 2);

    private static final String[] STAT_BROKER_MAP = new String[] {
        "TotalEnqueueCount", "TotalDequeueCount", "TotalConsumerCount", "TotalMessages",
        "TotalMessagesCached", "MemoryPercentageUsed", "MemoryLimit"
    };

    // Stat display flags
    private static final int STAT_DISP_BROKER  = Integer.parseInt("0001", 2);
    private static final int STAT_DISP_ALL     = Integer.parseInt("1111", 2);

    // Query object type to id mapping
    private static final Properties QUERY_TYPE_ID_MAP = new Properties();

    static
    {
        QUERY_TYPE_ID_MAP.setProperty("Broker",           "BrokerName");
        QUERY_TYPE_ID_MAP.setProperty("Connection",       "Connection");
        QUERY_TYPE_ID_MAP.setProperty("Connector",        "ConnectorName");
        QUERY_TYPE_ID_MAP.setProperty("NetworkConnector", "BrokerName");
        QUERY_TYPE_ID_MAP.setProperty("Queue",            "Destination");
        QUERY_TYPE_ID_MAP.setProperty("Topic",            "Destination");
    };

    private final ArrayList extensions   = new ArrayList();
    private final Map       queryObjects = new HashMap();
    private final List      queryViews   = new ArrayList();

    private int           taskType = TASK_NONE;
    private boolean       stopAll  = false;
    private JMXServiceURL jmxUrl;
    private URI           configURI;
    private File          activeMQHome;
    private ClassLoader   classLoader;

    public static void main(String[] args) {
        Main app = new Main();

        // Convert arguments to collection for easier management
        ArrayList tokens =  new ArrayList(Arrays.asList(args));

        // First token should be task type (start|stop|list|-h|-?|--help|--version)
        app.setTaskType(app.parseTask(tokens));

        // Succeeding tokens should be task specific options identified by "-" at the start
        app.parseOptions(tokens);

        // Succeeding tokens should be the task data
        switch (app.getTaskType()) {
            case  TASK_START_BROKER:
                try {
                    app.taskStartBrokers(tokens);
                } catch (Throwable e) {
                    System.out.println("Failed to start broker. Reason: " + e.getMessage());
                }
                break;

            case  TASK_STOP_BROKER:
                try {
                    app.taskStopBrokers(tokens);
                } catch (Throwable e) {
                    System.out.println("Failed to stop broker(s). Reason: " + e.getMessage());
                }
                break;

            case  TASK_LIST_BROKER:
                try {
                    app.taskListBrokers();
                } catch (Throwable e) {
                    e.printStackTrace();
                    System.out.println("Failed to list broker(s). Reason: " + e.getMessage());
                }
                break;

            case  TASK_STAT_BROKER:
                try {
                    app.taskStatBrokers(tokens);
                } catch (Throwable e) {
                    System.out.println("Failed to print broker statistics. Reason: " + e.getMessage());
                }
                break;

            case  TASK_QUERY_BROKER:
                try {
                    app.taskQueryBrokers();
                } catch (Throwable e) {
                    System.out.println("Failed to query broker. Reason: " + e.getMessage());
                }
                break;

            case  TASK_PRINT_MAIN_HELP:
                app.printHelp(HELP_MAIN_APP);
                break;

            case  TASK_PRINT_START_HELP:
                app.printHelp(HELP_START_BROKER);
                break;

            case  TASK_PRINT_STOP_HELP:
                app.printHelp(HELP_STOP_BROKER);
                break;

            case  TASK_PRINT_LIST_HELP:
                app.printHelp(HELP_LIST_BROKER);
                break;

            case  TASK_PRINT_STAT_HELP:
                app.printHelp(HELP_STAT_BROKER);
                break;

            case  TASK_PRINT_QUERY_HELP:
                app.printHelp(HELP_QUERY_BROKER);
                break;

            case  TASK_PRINT_VER:
                app.printVersion();
                break;

            case  TASK_PRINT_ALL_HELP:
                app.printAllHelp();
                break;

            case TASK_NONE:
            default:
                break;
        }
    }

    public int parseTask(List tokens) {
        if (tokens.isEmpty()) {
            // If no defined arguments, assume start task and default uri
            return TASK_START_BROKER;
        }

        // Process task token
        String taskToken = (String)tokens.get(0);

        if (taskToken.equals("start")) {
            tokens.remove(0);
            return TASK_START_BROKER;
        } else if (taskToken.equals("stop")) {
            tokens.remove(0);
            return TASK_STOP_BROKER;
        } else if (taskToken.equals("list")) {
            tokens.remove(0);
            return TASK_LIST_BROKER;
        } else if (taskToken.equals("stat")) {
            tokens.remove(0);
            return TASK_STAT_BROKER;
        } else if (taskToken.equals("query")) {
            tokens.remove(0);
            return TASK_QUERY_BROKER;
        } else if (taskToken.equals("-h") || taskToken.equals("-?") || taskToken.equals("--help")) {
            // No need to parse other tokens
            tokens.clear();
            return TASK_PRINT_MAIN_HELP;
        } else if (taskToken.equals("--version")) {
            // No need to parse other tokens
            tokens.clear();
            return TASK_PRINT_VER;
        } else {
            // If not a valid task, assume start task and succeeding args are options
            return TASK_START_BROKER;
        }
    }

    public void parseOptions(List tokens) {
        String token;

        while (!tokens.isEmpty()) {
            token = (String)tokens.get(0);

            // If token is an option
            if (token.startsWith("-")) {

                // Consider token to be processed
                tokens.remove(0);

                // If token is a help option
                if (token.equals("-h") || token.equals("-?") || token.equals("--help")) {
                    switch (this.getTaskType()) {
                        case TASK_STOP_BROKER:
                            this.setTaskType(TASK_PRINT_STOP_HELP);
                            tokens.clear();
                            return;

                        case TASK_LIST_BROKER:
                            this.setTaskType(TASK_PRINT_LIST_HELP);
                            tokens.clear();
                            return;

                        case TASK_STAT_BROKER:
                            this.setTaskType(TASK_PRINT_STAT_HELP);
                            tokens.clear();
                            return;

                        case TASK_QUERY_BROKER:
                            this.setTaskType(TASK_PRINT_QUERY_HELP);
                            tokens.clear();
                            return;

                        case TASK_START_BROKER:
                        default:
                            this.setTaskType(TASK_PRINT_START_HELP);
                            tokens.clear();
                            return;

                    }

                // If token is a version option
                } else if (token.equals("--version")) {
                    this.setTaskType(TASK_PRINT_VER);
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

                    this.addExtensionDirectory(extDir);
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

                // If token is a query define option
                else if (token.startsWith("-Q")) {
                    String key = token.substring(2);
                    String value = "";
                    int pos = key.indexOf("=");
                    if (pos >= 0) {
                        value = key.substring(pos + 1);
                        key = key.substring(0, pos);
                    }

                    queryObjects.put(key, value);
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
                    this.setStopAllBrokers(true);

                } else {
                    System.out.println("Ignoring unrecognized option: " + token);
                }

            // Finish parsing options
            } else {
                return;
            }
        }
    }

    protected void taskStartBrokers(List brokerURIs) throws Throwable {

        // Flag an error if there are multiple configuration uris
        if (brokerURIs.size() > 1) {
            printError("Multiple configuration uris or broker names cannot be specified.");
            brokerURIs.clear();
            return;
        }

        // Add the default directories.
        if(canUseExtdir()) {
            this.addExtensionDirectory(new File(this.getActiveMQHome(), "conf"));
            this.addExtensionDirectory(new File(this.getActiveMQHome(), "lib"));
            this.addExtensionDirectory(new File(new File(this.getActiveMQHome(), "lib"), "optional"));
        }

        // If no config uri, use default setting
        if (brokerURIs.isEmpty()) {
            this.setConfigUri(this.getDefaultUri());
            this.startBroker(this.getConfigUri());

        // Set configuration data, if available, which in this case would be the config URI
        } else {
            String strConfigURI;
//            while (!brokerURIs.isEmpty()) {
                strConfigURI = (String)brokerURIs.remove(0);

                try {
                    this.setConfigUri(new URI(strConfigURI));
                } catch (URISyntaxException e) {
                    printError("Invalid broker configuration URI: " + strConfigURI + ", reason: " + e.getMessage());
                    return;
                }

                this.startBroker(this.getConfigUri());
//            }
        }
    }

    protected void taskStopBrokers(List brokerNames) throws Throwable {
        // Check if there is a user-specified JMX URL
        if (this.getJmxUrl() == null) {
            this.setJmxUrl(this.getDefaultJmxUrl());
        }

        // Stop all brokers
        if (this.isStopAllBrokers()) {
            JMXConnector jmxConnector = JMXConnectorFactory.connect(this.getJmxUrl());
            MBeanServerConnection server = jmxConnector.getMBeanServerConnection();

            ObjectName brokerObjName = new ObjectName(DEFAULT_JMX_DOMAIN + ":Type=Broker,*");

            this.stopBroker(server, brokerObjName);

            brokerNames.clear();

            // Maybe no need to close, since context is already closed by broker
            //jmxConnector.close();

            return;
        }

        // Stop the default broker
        if (brokerNames.isEmpty()) {
            Set brokerList = this.getBrokerList(this.getJmxUrl());

            // If there is no broker to stop
            if (brokerList.isEmpty()) {
                System.out.println("There are no brokers to stop.");
                return;

            // There should only be one broker to stop
            } else if (brokerList.size() > 1) {
                System.out.println("There are multiple brokers to stop. Please select the broker(s) to stop or use --all to stop all brokers.");
                System.out.println();
                printHelp(HELP_STOP_BROKER);
                printBrokerList(brokerList);
                return;

            // Stop the only running broker
            } else {
                Iterator brokerIter = brokerList.iterator();

                JMXConnector jmxConnector = JMXConnectorFactory.connect(this.getJmxUrl());
                MBeanServerConnection server = jmxConnector.getMBeanServerConnection();

                this.stopBroker(server, ((ObjectInstance)brokerIter.next()).getObjectName());

                // Maybe no need to close, since context is already closed by broker
                //jmxConnector.close();
                return;
            }
        }

        // Stop each specified broker
        String brokerName;

        JMXConnector jmxConnector = JMXConnectorFactory.connect(this.getJmxUrl());
        MBeanServerConnection server = jmxConnector.getMBeanServerConnection();

        while (!brokerNames.isEmpty()) {
            brokerName = (String)brokerNames.remove(0);
            this.stopBroker(server, brokerName);
        }

        // Maybe be no need to close, since context is already closed by broker
        //jmxConnector.close();
    }

    protected void taskListBrokers() throws Throwable {
        // Check if there is a user-specified JMX URL
        if (this.getJmxUrl() == null) {
            this.setJmxUrl(this.getDefaultJmxUrl());
        }

        printBrokerList(this.getBrokerList(this.getJmxUrl()));
    }

    protected void taskStatBrokers(List brokerNames) throws Throwable {
        // Check if there is a user-specified JMX URL
        if (this.getJmxUrl() == null) {
            this.setJmxUrl(this.getDefaultJmxUrl());
        }

        // Print the statistics for the default broker
        if (brokerNames.isEmpty()) {
            Set brokerList = this.getBrokerList(this.getJmxUrl());

            // If there is no broker to stop
            if (brokerList.isEmpty()) {
                System.out.println("There are no brokers running.");
                return;

            // There should only be one broker to stop
            } else if (brokerList.size() > 1) {
                System.out.println("There are multiple brokers running. Please select the broker to display the statistics for.");
                System.out.println();
                printHelp(HELP_STAT_BROKER);
                printBrokerList(brokerList);
                return;

            // Print the statistics for the only running broker
            } else {
                Iterator brokerIter = brokerList.iterator();

                JMXConnector jmxConnector = JMXConnectorFactory.connect(this.getJmxUrl());
                MBeanServerConnection server = jmxConnector.getMBeanServerConnection();

                ObjectName brokerObjName = ((ObjectInstance)brokerIter.next()).getObjectName();
                this.printBrokerStat(brokerObjName.getKeyProperty(DEFAULT_KEY_BROKER_NAME), this.getBrokerStat(server, brokerObjName));

                jmxConnector.close();
                return;
            }
        }

        // Print the statistics for each specified broker
        String brokerName;

        JMXConnector jmxConnector = JMXConnectorFactory.connect(this.getJmxUrl());
        MBeanServerConnection server = jmxConnector.getMBeanServerConnection();

        while (!brokerNames.isEmpty()) {
            brokerName = (String)brokerNames.remove(0);
            System.out.println("-----------------------------------------------------");
            this.printBrokerStat(brokerName, this.getBrokerStat(server, brokerName));
            System.out.println();
        }

        jmxConnector.close();
    }

    protected void taskQueryBrokers() throws Throwable {
        // Check if there is a user-specified JMX URL
        if (this.getJmxUrl() == null) {
            this.setJmxUrl(this.getDefaultJmxUrl());
        }

        JMXConnector jmxConnector = JMXConnectorFactory.connect(this.getJmxUrl());
        MBeanServerConnection server = jmxConnector.getMBeanServerConnection();

        Set mbeans;
        // If there is no query defined get all mbeans
        if (this.getQueryObjects().isEmpty()) {
            ObjectName queryName = new ObjectName(DEFAULT_JMX_DOMAIN + ":*");

            mbeans = server.queryMBeans(queryName, null);

        // Construct the object name based on the query
        } else {
            mbeans = new CopyOnWriteArraySet();
            Set queryKeys = queryObjects.keySet();
            for (Iterator i=queryKeys.iterator(); i.hasNext();) {
                String objType = (String)i.next();
                String objName = (String)queryObjects.get(objType);

                // If select all type
                ObjectName queryName;
                if (objName.equals("*")) {
                    queryName = new ObjectName(DEFAULT_JMX_DOMAIN + ":Type=" + objType + ",*");
                } else {
                    queryName = new ObjectName(DEFAULT_JMX_DOMAIN + ":Type=" + objType + "," +
                                               QUERY_TYPE_ID_MAP.getProperty(objType) + "=" + objName + ",*");
                }
                mbeans.addAll(server.queryMBeans(queryName, null));
            }
        }

        for (Iterator i=mbeans.iterator(); i.hasNext();) {
            printMBeanAttr(server, (ObjectInstance)i.next(), this.getQueryViews());
        }

        jmxConnector.close();
    }

    public void addExtensionDirectory(File directory) {
        extensions.add(directory);
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

            Method start = broker.getClass().getMethod("start", new Class[]{});
            start.invoke(broker, new Object[]{});

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
        String brokerName = brokerObjName.getKeyProperty(DEFAULT_KEY_BROKER_NAME);

        try {
            server.invoke(brokerObjName, DEFAULT_METHOD_BROKER_STOP, DEFAULT_PARAM_BROKER_STOP, DEFAULT_SIGN_BROKER_STOP);
            System.out.println("Succesfully stopped broker: " + brokerName);
        } catch (Exception e) {
            // TODO: Check the exceptions thrown
            // System.out.println("Failed to stop broker: [ " + brokerName + " ]. Reason: " + e.getMessage());
            return;
        }
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

    public void printMBeanAttr(MBeanServerConnection server, ObjectInstance mbean, List attrView) {
        ObjectName mbeanObjName = mbean.getObjectName();
        String mbeanType = mbeanObjName.getKeyProperty("Type");
        String mbeanName = mbeanObjName.getKeyProperty(QUERY_TYPE_ID_MAP.getProperty(mbeanType));
        System.out.println("MBean Type: " + mbeanType);
        System.out.println("MBean Name: " + mbeanName);
        System.out.println("MBean Attributes:");

        try {
            MBeanAttributeInfo[] attrs = server.getMBeanInfo(mbeanObjName).getAttributes();

            // If there mbean has no attribute, print a no attribute message
            if (attrs.length == 0) {
                System.out.println("    MBean has no attributes.");
                System.out.println();
                return;
            }

            // If there is no view specified, print all attributes
            if (attrView == null || attrView.isEmpty()) {
                for (int i=0; i<attrs.length; i++) {
                    Object attrVal = server.getAttribute(mbeanObjName, attrs[i].getName());
                    System.out.println("    " + attrs[i].getName() + " = " + attrVal.toString());
                }
                System.out.println();
                return;
            }

            // Print attributes specified by view
            boolean matchedAttr = false;
            for (int i=0; i<attrs.length; i++) {
                if (attrView.contains(attrs[i].getName())) {
                    matchedAttr = true;
                    Object attrVal = server.getAttribute(mbeanObjName, attrs[i].getName());
                    System.out.println("    " + attrs[i].getName() + " = " + attrVal.toString());
                }
            }

            // If the mbean's attributes did not match any of the view, display a message
            if (!matchedAttr) {
                System.out.println("    View did not match any of the mbean's attributes.");
            }
            System.out.println();
        } catch (Exception e) {
            System.out.println("Failed to print mbean attributes. Reason: " + e.getMessage());
        }
    }

    public void printBrokerStat(String brokerName, Map brokerStat) {
        printBrokerStat(brokerName, brokerStat, STAT_DISP_ALL);
    }

    public void printBrokerStat(String brokerName, Map brokerStat, int dispFlags) {

        System.out.println("Displaying usage statistics for broker: " + brokerName);

        if ((dispFlags & STAT_DISP_BROKER) != 0) {
            System.out.println("    Broker Enqueue Count: "        + brokerStat.get(STAT_BROKER_MAP[0]));
            System.out.println("    Broker Dequeue Count: "        + brokerStat.get(STAT_BROKER_MAP[1]));
            System.out.println("    Broker Consumer Count: "       + brokerStat.get(STAT_BROKER_MAP[2]));
            System.out.println("    Broker Message Count: "        + brokerStat.get(STAT_BROKER_MAP[3]));
            System.out.println("    Broker Cached Message Count: " + brokerStat.get(STAT_BROKER_MAP[4]));
            System.out.println("    Broker Memory Percent Used: "  + brokerStat.get(STAT_BROKER_MAP[5]));
            System.out.println("    Broker Memory Limit: "         + brokerStat.get(STAT_BROKER_MAP[6]));
            System.out.println();
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
        // TODO: Why is version returned invalid?
        ClassLoader cl = getClassLoader();
        // Use reflection to get the version
        try {
            Class activeMQConnectionMetaData = cl.loadClass("org.activemq.ActiveMQConnectionMetaData");
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

    public void setStopAllBrokers(boolean stopAll) {
        this.stopAll = stopAll;
    }

    public boolean isStopAllBrokers() {
        return stopAll;
    }

    public Map getQueryObjects() {
        return queryObjects;
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
                URL url = Main.class.getClassLoader().getResource("org/activemq/broker/Main.class");
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

    public Map getBrokerStat(MBeanServerConnection server, String brokerName) throws Throwable {
        return getBrokerStat(server, brokerName, STAT_ALL);
    }

    public Map getBrokerStat(MBeanServerConnection server, ObjectName brokerObjName) {
        return getBrokerStat(server, brokerObjName, STAT_ALL);
    }

    public Map getBrokerStat(MBeanServerConnection server, String brokerName, int statFlags) throws Throwable {
        ObjectName brokerObjName = null;
        try {
            brokerObjName = new ObjectName(DEFAULT_JMX_DOMAIN + ":Type=Broker," + DEFAULT_KEY_BROKER_NAME + "=" + brokerName);
        } catch (Exception e) {
            System.out.println("Invalid broker name: " + brokerName);
            return null;
        }

        return getBrokerStat(server, brokerObjName, statFlags);
    }

    public Map getBrokerStat(MBeanServerConnection server, ObjectName brokerObjName, int statFlags) {
        Map brokerStat = new HashMap();

        try {
            if ((statFlags & STAT_BROKER) != 0) {
                for (int i=0; i<STAT_BROKER_MAP.length; i++) {
                    brokerStat.put(STAT_BROKER_MAP[i], // key name of statistic
                        server.getAttribute(brokerObjName, (String)STAT_BROKER_MAP[i]) // attribute to get)
                    );
                }
            }
        } catch (Exception e) {
            return null;
        }

        return brokerStat;
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
            "",
            "List Options:",
            "    --jmxurl <url>      Set the JMX URL to connect to.",
            "    --version           Display the version information.",
            "    -h,-?,--help        Display the stop broker help information.",
            "",
        },

        // Stat brokers task help
        {
            "Task Usage: Main stat [stat-options] [broker-name1] [broker-name2] ...",
            "",
            "Stat Options:",
            "    --jmxurl <url>      Set the JMX URL to connect to.",
            "    --version           Display the version information.",
            "    -h,-?,--help        Display the stat broker help information.",
            "",
        },

        // Query brokers task help
        {
            "Task Usage: Main query [query-options]",
            "",
            "Query Options:",
            "    -Q<type>=<name>               Filter the specific object type using the defined object identifier.",
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
            ""
        }
    };
}
