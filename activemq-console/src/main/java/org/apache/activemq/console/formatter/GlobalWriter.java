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
package org.apache.activemq.console.formatter;

import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;

import javax.jms.Message;
import javax.management.AttributeList;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

public final class GlobalWriter {
    private static OutputFormatter formatter;

    /**
     * Creates a singleton global writer
     */
    private GlobalWriter() {
    }

    /**
     * Maintains a global output formatter
     * 
     * @param formatter - the output formatter to use
     */
    public static void instantiate(OutputFormatter formatter) {
        GlobalWriter.formatter = formatter;
    }

    /**
     * Retrieve the output stream being used by the global formatter
     * 
     * @return
     */
    public static OutputStream getOutputStream() {
        if (formatter == null) {
            throw new IllegalStateException("No OutputFormatter specified. Use GlobalWriter.instantiate(OutputFormatter).");
        }
        return formatter.getOutputStream();
    }

    /**
     * Print an ObjectInstance format of an mbean
     * 
     * @param mbean - mbean to print
     */
    public static void printMBean(ObjectInstance mbean) {
        if (formatter == null) {
            throw new IllegalStateException("No OutputFormatter specified. Use GlobalWriter.instantiate(OutputFormatter).");
        }
        formatter.printMBean(mbean);
    }

    /**
     * Print an ObjectName format of an mbean
     * 
     * @param mbean - mbean to print
     */
    public static void printMBean(ObjectName mbean) {
        if (formatter == null) {
            throw new IllegalStateException("No OutputFormatter specified. Use GlobalWriter.instantiate(OutputFormatter).");
        }
        formatter.printMBean(mbean);
    }

    /**
     * Print an AttributeList format of an mbean
     * 
     * @param mbean - mbean to print
     */
    public static void printMBean(AttributeList mbean) {
        if (formatter == null) {
            throw new IllegalStateException("No OutputFormatter specified. Use GlobalWriter.instantiate(OutputFormatter).");
        }
        formatter.printMBean(mbean);
    }

    /**
     * Print a Map format of an mbean
     * 
     * @param mbean
     */
    public static void printMBean(Map mbean) {
        if (formatter == null) {
            throw new IllegalStateException("No OutputFormatter specified. Use GlobalWriter.instantiate(OutputFormatter).");
        }
        formatter.printMBean(mbean);
    }

    /**
     * Print a Collection format of mbeans
     * 
     * @param mbean - collection of mbeans
     */
    public static void printMBean(Collection mbean) {
        if (formatter == null) {
            throw new IllegalStateException("No OutputFormatter specified. Use GlobalWriter.instantiate(OutputFormatter).");
        }
        formatter.printMBean(mbean);
    }

    /**
     * Print a Map format of a JMS message
     * 
     * @param msg
     */
    public static void printMessage(Map msg) {
        if (formatter == null) {
            throw new IllegalStateException("No OutputFormatter specified. Use GlobalWriter.instantiate(OutputFormatter).");
        }
        formatter.printMessage(msg);
    }

    /**
     * Print a Message format of a JMS message
     * 
     * @param msg - JMS message to print
     */
    public static void printMessage(Message msg) {
        if (formatter == null) {
            throw new IllegalStateException("No OutputFormatter specified. Use GlobalWriter.instantiate(OutputFormatter).");
        }
        formatter.printMessage(msg);
    }

    /**
     * Print a collection of JMS messages
     * 
     * @param msg - collection of JMS messages
     */
    public static void printMessage(Collection msg) {
        if (formatter == null) {
            throw new IllegalStateException("No OutputFormatter specified. Use GlobalWriter.instantiate(OutputFormatter).");
        }
        formatter.printMessage(msg);
    }

    /**
     * Print help messages
     * 
     * @param helpMsgs - help messages to print
     */
    public static void printHelp(String[] helpMsgs) {
        if (formatter == null) {
            throw new IllegalStateException("No OutputFormatter specified. Use GlobalWriter.instantiate(OutputFormatter).");
        }
        formatter.printHelp(helpMsgs);
    }

    /**
     * Print an information message
     * 
     * @param info - information message to print
     */
    public static void printInfo(String info) {
        if (formatter == null) {
            throw new IllegalStateException("No OutputFormatter specified. Use GlobalWriter.instantiate(OutputFormatter).");
        }
        formatter.printInfo(info);
    }

    /**
     * Print an exception message
     * 
     * @param e - exception to print
     */
    public static void printException(Exception e) {
        if (formatter == null) {
            throw new IllegalStateException("No OutputFormatter specified. Use GlobalWriter.instantiate(OutputFormatter).");
        }
        formatter.printException(e);
    }

    /**
     * Print a version information
     * 
     * @param version - version info to print
     */
    public static void printVersion(String version) {
        if (formatter == null) {
            throw new IllegalStateException("No OutputFormatter specified. Use GlobalWriter.instantiate(OutputFormatter).");
        }
        formatter.printVersion(version);
    }

    /**
     * Print a generic key value mapping
     * 
     * @param map to print
     */
    public static void print(Map map) {
        if (formatter == null) {
            throw new IllegalStateException("No OutputFormatter specified. Use GlobalWriter.instantiate(OutputFormatter).");
        }
        formatter.print(map);
    }

    /**
     * Print a generic array of strings
     * 
     * @param strings - string array to print
     */
    public static void print(String[] strings) {
        if (formatter == null) {
            throw new IllegalStateException("No OutputFormatter specified. Use GlobalWriter.instantiate(OutputFormatter).");
        }
        formatter.print(strings);
    }

    /**
     * Print a collection of objects
     * 
     * @param collection - collection to print
     */
    public static void print(Collection collection) {
        if (formatter == null) {
            throw new IllegalStateException("No OutputFormatter specified. Use GlobalWriter.instantiate(OutputFormatter).");
        }
        formatter.print(collection);
    }

    /**
     * Print a java string
     * 
     * @param string - string to print
     */
    public static void print(String string) {
        if (formatter == null) {
            throw new IllegalStateException("No OutputFormatter specified. Use GlobalWriter.instantiate(OutputFormatter).");
        }
        formatter.print(string);
    }
}
