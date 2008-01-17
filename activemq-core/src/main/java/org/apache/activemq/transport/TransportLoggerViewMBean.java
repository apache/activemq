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
 * MBean to manage a single Transport Logger.
 * It can inform if the logger is currently writing to a log file or not,
 * by setting the logging property or by using the operations
 * enableLogging() and disableLogging().
 * 
 * @author David Martin Clavo david(dot)martin(dot)clavo(at)gmail.com
 * @version $Revision$
 */
public interface TransportLoggerViewMBean {

    /**
     * Returns if the managed TransportLogger is currently active
     * (writing to a log) or not.
     * @return if the managed TransportLogger is currently active
     * (writing to a log) or not.
     */
    public boolean isLogging();
    
    /**
     * Enables or disables logging for the managed TransportLogger.
     * @param logging Boolean value to enable or disable logging for
     * the managed TransportLogger.
     * true to enable logging, false to disable logging.
     */
    public void setLogging(boolean logging);
    
    /**
     * Enables logging for the managed TransportLogger.
     */
    public void enableLogging();
    
    /**
     * Disables logging for the managed TransportLogger.
     */
    public void disableLogging();
    
}
