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
package org.apache.activemq.util;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.transport.LogWriter;
import org.apache.activemq.transport.TransportLoggerView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to find a LogWriter implementation, and returning
 * a LogWriter object, taking as argument the name of a log writer.
 * The mapping between the log writer names and the classes
 * implementing LogWriter is specified by the files in the
 * resources/META-INF/services/org/apache/activemq/transport/logwriters
 * directory.
 * 
 * @author David Martin Clavo david(dot)martin(dot)clavo(at)gmail.com
 * @version $Revision$
 */
public class LogWriterFinder {
    
    private static final Logger log = LoggerFactory.getLogger(TransportLoggerView.class);

    private final String path;
    private final ConcurrentHashMap classMap = new ConcurrentHashMap();

    /**
     * Builds a LogWriterFinder that will look for the mappings between
     * LogWriter names and classes in the directory "path".
     * @param path The directory where the files that map log writer names to
     * LogWriter classes are. 
     */
    public LogWriterFinder(String path) {
        this.path = path;
    }

    /**
     * Returns a LogWriter object, given a log writer name (for example "default", or "custom").
     * Uses a ConcurrentHashMap to cache the Class objects that have already been loaded.
     * @param logWriterName a log writer name (for example "default", or "custom").
     * @return a LogWriter object to be used by the TransportLogger class.
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public LogWriter newInstance(String logWriterName)
    throws IllegalAccessException, InstantiationException, IOException, ClassNotFoundException
    {
        Class clazz = (Class) classMap.get(logWriterName);
        if (clazz == null) {
            clazz = newInstance(doFindLogWriterProperties(logWriterName));
            classMap.put(logWriterName, clazz);
        }
        return (LogWriter)clazz.newInstance();
    }
    
    /**
     * Loads and returns a class given a Properties object with a "class" property.
     * @param properties a Properties object with a "class" property.
     * @return a Class object.
     * @throws ClassNotFoundException
     * @throws IOException
     */
    private Class newInstance(Properties properties) throws ClassNotFoundException, IOException {

        String className = properties.getProperty("class");
        if (className == null) {
            throw new IOException("Expected property is missing: " + "class");
        }
        Class clazz;
        try {
            clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
        } catch (ClassNotFoundException e) {
            clazz = LogWriterFinder.class.getClassLoader().loadClass(className);
        }

        return clazz;
    }

    /**
     * Given a log writer name, returns a Properties object with a "class" property
     * whose value is a String with the name of the class to be loaded.
     * @param logWriterName a log writer name.
     * @return a Properties object with a "class" property
     * @throws IOException
     */
    protected Properties doFindLogWriterProperties (String logWriterName) throws IOException {

        String uri = path + logWriterName;

        // lets try the thread context class loader first
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) classLoader = getClass().getClassLoader();
        InputStream in = classLoader.getResourceAsStream(uri);
        if (in == null) {
            in = LogWriterFinder.class.getClassLoader().getResourceAsStream(uri);
            if (in == null) {
                log.error("Could not find log writer for resource: " + uri);
                throw new IOException("Could not find log writer for resource: " + uri);
            }
        }

        // lets load the file
        BufferedInputStream reader = null;
        Properties properties = new Properties();
        try {
            reader = new BufferedInputStream(in);
            properties.load(reader);
            return properties;
        } finally {
            try {
                reader.close();
            } catch (Exception e) {
            }
        }
    }


}
