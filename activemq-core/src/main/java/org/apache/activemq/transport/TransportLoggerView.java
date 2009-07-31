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

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.management.ObjectName;

import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.util.JMXSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Class implementing the TransportLoggerViewMBean interface.
 * When an object of this class is created, it registers itself in
 * the MBeanServer of the management context provided.
 * When a TransportLogger object is finalized because the Transport Stack
 * where it resides is no longer in use, the method unregister() will be called.
 * 
 * @author David Martin Clavo david(dot)martin(dot)clavo(at)gmail.com
 * @version $Revision$ 
 * @see TransportLoggerViewMBean.
 */
public class TransportLoggerView implements TransportLoggerViewMBean {

    private static final Log log = LogFactory.getLog(TransportLoggerView.class);
    
    /**
     * Set with the TransportLoggerViews objects created.
     * Used by the methods enableAllTransportLoggers and diablellTransportLoggers.
     * The method unregister() removes objects from this set.
     */
    private static Set<TransportLoggerView> transportLoggerViews = Collections.synchronizedSet(new HashSet<TransportLoggerView>());

    private final WeakReference<TransportLogger> transportLogger;
    private final String nextTransportName;
    private final int id;
    private final ManagementContext managementContext;
    private final ObjectName name;

    /**
     * Constructor.
     * @param transportLogger The TransportLogger object which is to be managed by this MBean.
     * @param nextTransportName The name of the next TransportLayer. This is used to give a unique
     * name for each MBean of the TransportLoggerView class.
     * @param id The id of the TransportLogger to be watched.
     * @param managementContext The management context who has the MBeanServer where this MBean will be registered.
     */
    public TransportLoggerView (TransportLogger transportLogger, String nextTransportName, int id, ManagementContext managementContext) {
        this.transportLogger = new WeakReference<TransportLogger>(transportLogger);
        this.nextTransportName = nextTransportName;
        this.id = id;
        this.managementContext = managementContext;
        this.name = this.createTransportLoggerObjectName();
        
        TransportLoggerView.transportLoggerViews.add(this);
        this.register();
    }
    
    /**
     * Enable logging for all Transport Loggers at once.
     */
    public static void enableAllTransportLoggers() {
        for (TransportLoggerView view : transportLoggerViews) {
            view.enableLogging();
        }
    }
    
    /**
     * Disable logging for all Transport Loggers at once.
     */
    public static void disableAllTransportLoggers() {
        for (TransportLoggerView view : transportLoggerViews) {
            view.disableLogging();
        }
    }

    // doc comment inherited from TransportLoggerViewMBean
    public void enableLogging() {
        this.setLogging(true);
    }

    // doc comment inherited from TransportLoggerViewMBean
    public void disableLogging() {
        this.setLogging(false);
    }   

    // doc comment inherited from TransportLoggerViewMBean
    public boolean isLogging() {
        return transportLogger.get().isLogging();
    }

    // doc comment inherited from TransportLoggerViewMBean
    public void setLogging(boolean logging) {
        transportLogger.get().setLogging(logging);
    }

    /**
     * Registers this MBean in the MBeanServer of the management context
     * provided at creation time. This method is only called by the constructor.
     */
    private void register() {
        try {
            this.managementContext.registerMBean(this, this.name);
        } catch (Exception e) {
            log.error("Could not register MBean for TransportLoggerView " + id + "with name " + this.name.toString() + ", reason: " + e, e);
        }

    }

    /**
     * Unregisters the MBean from the MBeanServer of the management context
     * provided at creation time.
     * This method is called by the TransportLogger object being managed when
     * the TransportLogger object is finalized, to avoid the memory leak that
     * would be caused if MBeans were not unregistered. 
     */
    public void unregister() {
        
        TransportLoggerView.transportLoggerViews.remove(this);
        
        try {
            this.managementContext.unregisterMBean(this.name);
        } catch (Exception e) {
            log.error("Could not unregister MBean for TransportLoggerView " + id + "with name " + this.name.toString() + ", reason: " + e, e);
        }
    }

    /**
     * Creates the ObjectName to be used when registering the MBean.
     * @return the ObjectName to be used when registering the MBean.
     */
    private ObjectName createTransportLoggerObjectName()  {
        try {
            return new ObjectName(
                    createTransportLoggerObjectNameRoot(this.managementContext)
                    + JMXSupport.encodeObjectNamePart(TransportLogger.class.getSimpleName()
                            + " " + this.id + ";" + this.nextTransportName));
        } catch (Exception e) {
            log.error("Could not create ObjectName for TransportLoggerView " + id + ", reason: " + e, e);
            return null;
        }
    }

    /**
     * Creates the part of the ObjectName that will be used by all MBeans.
     * This method is public so it can be used by the TransportLoggerControl class.
     * @param managementContext
     * @return A String with the part of the ObjectName common to all the TransportLoggerView MBeans.
     */
    public static String createTransportLoggerObjectNameRoot(ManagementContext managementContext) {
        return managementContext.getJmxDomainName()+":"+"Type=TransportLogger,"+"TransportLoggerName=";
    }

}
