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
package org.apache.activemq.transport;

import java.io.IOException;

import javax.management.ObjectName;

import org.apache.activemq.broker.jmx.AnnotatedMBean;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.LogWriterFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.TransportLoggerSupport.defaultJmxPort;

/**
 * Singleton class to create TransportLogger objects.
 * When the method getInstance() is called for the first time,
 * a TransportLoggerControlMBean is created and registered.
 * This MBean permits enabling and disabling the logging for
 * all TransportLogger objects at once.
 *
 * @author David Martin Clavo david(dot)martin(dot)clavo(at)gmail.com
 *
 * @see TransportLoggerControlMBean
 */
public class TransportLoggerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TransportLoggerFactory.class);

    private static TransportLoggerFactory instance;
    private static int lastId=0;
    private static final LogWriterFinder logWriterFinder = new LogWriterFinder("META-INF/services/org/apache/activemq/transport/logwriters/");

    /**
     * LogWriter that will be used if none is specified.
     */
    public static String defaultLogWriterName = "default";
    /**
     * If transport logging is enabled, it will be possible to control
     * the transport loggers or not based on this value
     */
    private static boolean defaultDynamicManagement = false;
    /**
     * If transport logging is enabled, the transport loggers will initially
     * output or not depending on this value.
     * This setting only has a meaning if
     */
    private static boolean defaultInitialBehavior = true;

    private boolean transportLoggerControlCreated = false;
    private ManagementContext managementContext;
    private ObjectName objectName;

    /**
     * Private constructor.
     */
    private TransportLoggerFactory() {
    }

    /**
     * Returns a TransportLoggerFactory object which can be used to create TransportLogger objects.
     * @return a TransportLoggerFactory object
     */
    public static synchronized TransportLoggerFactory getInstance() {
        if (instance == null) {
            instance = new TransportLoggerFactory();
        }
        return instance;
    }

    public void stop() {
        try {
            if (this.transportLoggerControlCreated) {
                this.managementContext.unregisterMBean(this.objectName);
                this.managementContext.stop();
                this.managementContext = null;
            }
        } catch (Exception e) {
            LOG.error("TransportLoggerFactory could not be stopped, reason: " + e, e);
        }

    }

    /**
     * Creates a TransportLogger object, that will be inserted in the Transport Stack.
     * Uses the default initial behavior, the default log writer, and creates a new
     * log4j object to be used by the TransportLogger.
     * @param next The next Transport layer in the Transport stack.
     * @return A TransportLogger object.
     * @throws IOException
     */
    public TransportLogger createTransportLogger(Transport next) throws IOException {
        int id = getNextId();
        return createTransportLogger(next, id, createLog(id), defaultLogWriterName, defaultDynamicManagement, defaultInitialBehavior, defaultJmxPort);
    }

    /**
     * Creates a TransportLogger object, that will be inserted in the Transport Stack.
     * Uses the default initial behavior and the default log writer.
     * @param next The next Transport layer in the Transport stack.
     * @param log The log4j log that will be used by the TransportLogger.
     * @return A TransportLogger object.
     * @throws IOException
     */
    public TransportLogger createTransportLogger(Transport next, Logger log) throws IOException {
        return createTransportLogger(next, getNextId(), log, defaultLogWriterName, defaultDynamicManagement, defaultInitialBehavior, defaultJmxPort);
    }

    /**
     * Creates a TransportLogger object, that will be inserted in the Transport Stack.
     * Creates a new log4j object to be used by the TransportLogger.
     * @param next The next Transport layer in the Transport stack.
     * @param startLogging Specifies if this TransportLogger should be initially active or not.
     * @param logWriterName The name or the LogWriter to be used. Different log writers can output
     * logs with a different format.
     * @return A TransportLogger object.
     * @throws IOException
     */
    public TransportLogger createTransportLogger(Transport next, String logWriterName,
            boolean useJmx, boolean startLogging, int jmxport) throws IOException {
        int id = -1; // new default to single logger
        // allow old behaviour with incantation
        if (!useJmx && jmxport != defaultJmxPort) {
            id = getNextId();
        }
        return createTransportLogger(next, id, createLog(id), logWriterName, useJmx, startLogging, jmxport);
    }



    /**
     * Creates a TransportLogger object, that will be inserted in the Transport Stack.
     * @param next The next Transport layer in the Transport stack.
     * @param id The id of the transport logger.
     * @param log The log4j log that will be used by the TransportLogger.
     * @param logWriterName The name or the LogWriter to be used. Different log writers can output
     * @param dynamicManagement Specifies if JMX will be used to switch on/off the TransportLogger to be created.
     * @param startLogging Specifies if this TransportLogger should be initially active or not. Only has a meaning if
     * dynamicManagement = true.
     * @param jmxport the port to be used by the JMX server. It should only be different from 1099 (broker's default JMX port)
     * when it's a client that is using Transport Logging. In a broker, if the port is different from 1099, 2 JMX servers will
     * be created, both identical, with all the MBeans.
     * @return A TransportLogger object.
     * @throws IOException
     */
    public TransportLogger createTransportLogger(Transport next, int id, Logger log,
            String logWriterName, boolean dynamicManagement, boolean startLogging, int jmxport) throws IOException {
        try {
            LogWriter logWriter = logWriterFinder.newInstance(logWriterName);
            if (id == -1) {
                logWriter.setPrefix(String.format("%08X: ", getNextId()));
            }
            TransportLogger tl =  new TransportLogger (next, log, startLogging, logWriter);
            if (dynamicManagement) {
                synchronized (this) {
                    if (!this.transportLoggerControlCreated) {
                        this.createTransportLoggerControl(jmxport);
                    }
                }
                TransportLoggerView tlv = new TransportLoggerView(tl, next.toString(), id, this.managementContext);
                tl.setView(tlv);
            }
            return tl;
        } catch (Throwable e) {
            throw IOExceptionSupport.create("Could not create log writer object for: " + logWriterName + ", reason: " + e, e);
        }
    }

    synchronized private static int getNextId() {
        return ++lastId;
    }

    private static Logger createLog(int id) {
        return LoggerFactory.getLogger(TransportLogger.class.getName()+".Connection" + (id > 0 ? ":"+id : "" ));
    }

    /**
     * Starts the management context.
     * Creates and registers a TransportLoggerControl MBean which enables the user
     * to enable/disable logging for all transport loggers at once.
     */
     private void createTransportLoggerControl(int port) {
         try {
             this.managementContext = new ManagementContext();
             this.managementContext.setConnectorPort(port);
             this.managementContext.start();
         } catch (Exception e) {
             LOG.error("Management context could not be started, reason: " + e, e);
         }

         try {
             this.objectName = new ObjectName(this.managementContext.getJmxDomainName()+":"+ "Type=TransportLoggerControl");
             AnnotatedMBean.registerMBean(this.managementContext, new TransportLoggerControl(this.managementContext),this.objectName);

             this.transportLoggerControlCreated = true;

         } catch (Exception e) {
             LOG.error("TransportLoggerControlMBean could not be registered, reason: " + e, e);
         }
     }

}
