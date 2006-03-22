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
package org.apache.activemq.console.formatter;

import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.AttributeList;
import javax.jms.Message;
import java.util.Collection;
import java.util.Map;
import java.io.OutputStream;

public interface OutputFormatter {

    /**
     * Retrieve the output stream being used by the formatter
     * @return
     */
    public OutputStream getOutputStream();

    /**
     * Print an ObjectInstance format of an mbean
     * @param mbean - mbean to print
     */
    public void printMBean(ObjectInstance mbean);

    /**
     * Print an ObjectName format of an mbean
     * @param mbean - mbean to print
     */
    public void printMBean(ObjectName mbean);

    /**
     * Print an AttributeList format of an mbean
     * @param mbean - mbean to print
     */
    public void printMBean(AttributeList mbean);

    /**
     * Print a Map format of an mbean
     * @param mbean - mbean to print
     */
    public void printMBean(Map mbean);

    /**
     * Print a Collection format of mbeans
     * @param mbean - collection of mbeans
     */
    public void printMBean(Collection mbean);

    /**
     * Print a Map format of a JMS message
     * @param msg
     */
    public void printMessage(Map msg);

    /**
     * Print a Message format of a JMS message
     * @param msg - JMS message to print
     */
    public void printMessage(Message msg);

    /**
     * Print a Collection format of JMS messages
     * @param msg - collection of JMS messages
     */
    public void printMessage(Collection msg);

    /**
     * Print help messages
     * @param helpMsgs - help messages to print
     */
    public void printHelp(String[] helpMsgs);

    /**
     * Print an information message
     * @param info - information message to print
     */
    public void printInfo(String info);

    /**
     * Print an exception message
     * @param e - exception to print
     */
    public void printException(Exception e);

    /**
     * Print a version information
     * @param version - version info to print
     */
    public void printVersion(String version);

    /**
     * Print a generic key value mapping
     * @param map to print
     */
    public void print(Map map);

    /**
     * Print a generic array of strings
     * @param strings - string array to print
     */
    public void print(String[] strings);

    /**
     * Print a collection of objects
     * @param collection - collection to print
     */
    public void print(Collection collection);

    /**
     * Print a java string
     * @param string - string to print
     */
    public void print(String string);
}
