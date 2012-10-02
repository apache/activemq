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

import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.broker.jmx.ManagementContext;

/**
 * Implementation of the TransportLoggerControlMBean interface,
 * which is an MBean used to control all TransportLoggers at once.
 * 
 * @author David Martin Clavo david(dot)martin(dot)clavo(at)gmail.com
 * 
 */
public class TransportLoggerControl implements TransportLoggerControlMBean {

    /**
     * Constructor
     */
    public TransportLoggerControl(ManagementContext managementContext) {
    }

    // doc comment inherited from TransportLoggerControlMBean
    public void disableAllTransportLoggers() {
        TransportLoggerView.disableAllTransportLoggers();
    }

    // doc comment inherited from TransportLoggerControlMBean
    public void enableAllTransportLoggers() {
        TransportLoggerView.enableAllTransportLoggers();
    }

    //  doc comment inherited from TransportLoggerControlMBean
    public void reloadLog4jProperties() throws Throwable {
        new BrokerView(null, null).reloadLog4jProperties();
    }

}
