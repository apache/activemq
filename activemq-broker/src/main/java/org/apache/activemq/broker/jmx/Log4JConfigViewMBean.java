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

import java.util.List;

/**
 * Log4J Configuration Management MBean used to alter the runtime log levels
 * or force a reload of the Log4J configuration file.
 */
public interface Log4JConfigViewMBean {

    /**
     * Get the log level for the root logger
     *
     * @returns the current log level of the root logger.
     *
     * @throws Exception if an error occurs while getting the root level.
     */
    @MBeanInfo("Returns the current logging level of the root logger.")
    String getRootLogLevel() throws Exception;

    /**
     * Get the log level for the root logger
     *
     * @param level
     *        the new level to assign to the root logger.
     *
     * @throws Exception if an error occurs while setting the root level.
     */
    @MBeanInfo("Sets the current logging level of the root logger.")
    void setRootLogLevel(String level) throws Exception;

    /**
     * list of all the logger names and their levels
     *
     * @return a List of all known loggers names.
     *
     * @throws Exception if an error occurs while getting the loggers.
     */
    @MBeanInfo("List of all loggers that are available for configuration.")
    List<String> getLoggers() throws Exception;

    /**
     * Get the log level for a given logger
     *
     * @param loggerName
     *        the name of the logger whose level should be queried.
     *
     * @return the current log level of the given logger.
     *
     * @throws Exception if an error occurs while getting the log level.
     */
    @MBeanInfo("Returns the current logging level of a named logger.")
    String getLogLevel(String loggerName) throws Exception;

    /**
     * Set the log level for a given logger
     *
     * @param loggerName
     *        the name of the logger whose level is to be adjusted.
     * @param level
     *        the new level to assign the given logger.
     *
     * @throws Exception if an error occurs while setting the log level.
     */
    @MBeanInfo("Sets the logging level for the named logger.")
    void setLogLevel(String loggerName, String level) throws Exception;

    /**
     * Reloads log4j.properties from the classpath.
     *
     * @throws Exception if an error occurs trying to reload the config file.
     */
    @MBeanInfo(value="Reloads log4j.properties from the classpath.")
    public void reloadLog4jProperties() throws Throwable;

}
