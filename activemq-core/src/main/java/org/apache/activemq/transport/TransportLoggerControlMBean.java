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

/**
 * MBean used to manage all of the TransportLoggers at once.
 * Avalaible operations:
 *  -Enable logging for all TransportLoggers at once.
 *  -Disable logging for all TransportLoggers at once.
 *  
 * @author David Martin Clavo david(dot)martin(dot)clavo(at)gmail.com
 * @version $Revision$
 */
public interface TransportLoggerControlMBean {

    /**
     * Enable logging for all Transport Loggers at once.
     */
    public void enableAllTransportLoggers();

    /**
     * Disable logging for all Transport Loggers at once.
     */
    public void disableAllTransportLoggers();

    /**
     * Reloads log4j.properties from the classpath
     * @throws Throwable 
     */
    public void reloadLog4jProperties() throws Throwable;

}
