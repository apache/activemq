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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log4JConfigView implements Log4JConfigViewMBean {

    private static final Logger LOG = LoggerFactory.getLogger(Log4JConfigView.class);

    @Override
    public String getRootLogLevel() throws Exception {
        ClassLoader cl = getClassLoader();

        if (!isLog4JAvailable(cl)) {
            return null;
        }

        Class<?> loggerClass = getLoggerClass(cl);
        if (loggerClass == null) {
            return null;
        }

        Method getRootLogger = loggerClass.getMethod("getRootLogger", new Class[]{});
        Method getLevel = loggerClass.getMethod("getLevel", new Class[]{});
        Object rootLogger = getRootLogger.invoke(null, (Object[])null);

        return getLevel.invoke(rootLogger, (Object[])null).toString();
    }

    @Override
    public void setRootLogLevel(String level) throws Exception {
        ClassLoader cl = getClassLoader();

        if (!isLog4JAvailable(cl)) {
            return;
        }

        Class<?> loggerClass = getLoggerClass(cl);
        Class<?> levelClass = getLevelClass(cl);
        if (levelClass == null || loggerClass == null) {
            return;
        }

        String targetLevel = level.toUpperCase(Locale.US);
        Method getRootLogger = loggerClass.getMethod("getRootLogger", new Class[]{});
        Method setLevel = loggerClass.getMethod("setLevel", levelClass);
        Object rootLogger = getRootLogger.invoke(null, (Object[])null);
        Method toLevel = levelClass.getMethod("toLevel", String.class);
        Object newLevel = toLevel.invoke(null, targetLevel);

        // Check that the level conversion worked and that we got a level
        // that matches what was asked for.  A bad level name will result
        // in the lowest level value and we don't want to change unless we
        // matched what the user asked for.
        if (newLevel != null && newLevel.toString().equals(targetLevel)) {
            LOG.debug("Set level {} for root logger.", level);
            setLevel.invoke(rootLogger, newLevel);
        }
    }

    @Override
    public List<String> getLoggers() throws Exception {

        ClassLoader cl = getClassLoader();

        if (!isLog4JAvailable(cl)) {
            return Collections.emptyList();
        }

        Class<?> logManagerClass = getLogManagerClass(cl);
        Class<?> loggerClass = getLoggerClass(cl);
        if (logManagerClass == null || loggerClass == null) {
            return Collections.emptyList();
        }

        Method getCurrentLoggers = logManagerClass.getMethod("getCurrentLoggers", new Class[]{});
        Method getName = loggerClass.getMethod("getName", new Class[]{});

        List<String> list = new ArrayList<String>();
        Enumeration<?> loggers = (Enumeration<?>)getCurrentLoggers.invoke(null, (Object[])null);

        while (loggers.hasMoreElements()) {
            Object logger = loggers.nextElement();
            if (logger != null) {
                list.add((String) getName.invoke(logger, (Object[])null));
            }
        }

        LOG.debug("Found {} loggers", list.size());

        return list;
    }

    @Override
    public String getLogLevel(String loggerName) throws Exception {

        ClassLoader cl = getClassLoader();

        if (!isLog4JAvailable(cl)) {
            return null;
        }

        Class<?> loggerClass = getLoggerClass(cl);
        if (loggerClass == null) {
            return null;
        }

        Method getLogger = loggerClass.getMethod("getLogger", String.class);
        String logLevel = null;

        if (loggerName != null && !loggerName.isEmpty()) {
            Object logger = getLogger.invoke(null, loggerName);
            if (logger != null) {
                LOG.debug("Found level {} for logger: {}", logLevel, loggerName);
                Method getLevel = loggerClass.getMethod("getLevel", new Class[]{});
                Object level = getLevel.invoke(logger, (Object[])null);
                if (level != null) {
                    logLevel = level.toString();
                } else {
                    Method getRootLogger = loggerClass.getMethod("getRootLogger", new Class[]{});
                    Object rootLogger = getRootLogger.invoke(null, (Object[])null);
                    logLevel = getLevel.invoke(rootLogger, (Object[])null).toString();
                }
            }
        } else {
            throw new IllegalArgumentException("Logger names cannot be null or empty strings");
        }

        return logLevel;
    }

    @Override
    public void setLogLevel(String loggerName, String level) throws Exception {

        if (loggerName == null || loggerName.isEmpty()) {
            throw new IllegalArgumentException("Logger names cannot be null or empty strings");
        }

        if (level == null || level.isEmpty()) {
            throw new IllegalArgumentException("Level name cannot be null or empty strings");
        }

        ClassLoader cl = getClassLoader();

        if (!isLog4JAvailable(cl)) {
            return;
        }

        Class<?> loggerClass = getLoggerClass(cl);
        Class<?> levelClass = getLevelClass(cl);
        if (loggerClass == null || levelClass == null) {
            return;
        }

        String targetLevel = level.toUpperCase(Locale.US);
        Method getLogger = loggerClass.getMethod("getLogger", String.class);
        Method setLevel = loggerClass.getMethod("setLevel", levelClass);
        Method toLevel = levelClass.getMethod("toLevel", String.class);

        Object logger = getLogger.invoke(null, loggerName);
        if (logger != null) {
            Object newLevel = toLevel.invoke(null, targetLevel);

            // Check that the level conversion worked and that we got a level
            // that matches what was asked for.  A bad level name will result
            // in the lowest level value and we don't want to change unless we
            // matched what the user asked for.
            if (newLevel != null && newLevel.toString().equals(targetLevel)) {
                LOG.debug("Set level {} for logger: {}", level, loggerName);
                setLevel.invoke(logger, newLevel);
            }
        }
    }

    @Override
    public void reloadLog4jProperties() throws Throwable {
        doReloadLog4jProperties();
    }

    //---------- Static Helper Methods ---------------------------------------//

    public static void doReloadLog4jProperties() throws Throwable {
        try {
            ClassLoader cl = Log4JConfigView.class.getClassLoader();
            Class<?> logManagerClass = getLogManagerClass(cl);
            if (logManagerClass == null) {
                LOG.debug("Could not locate log4j classes on classpath.");
                return;
            }

            Method resetConfiguration = logManagerClass.getMethod("resetConfiguration", new Class[]{});
            resetConfiguration.invoke(null, new Object[]{});

            String configurationOptionStr = System.getProperty("log4j.configuration");
            URL log4jprops = null;
            if (configurationOptionStr != null) {
                try {
                    log4jprops = new URL(configurationOptionStr);
                } catch (MalformedURLException ex) {
                    log4jprops = cl.getResource("log4j.properties");
                }
            } else {
               log4jprops = cl.getResource("log4j.properties");
            }

            if (log4jprops != null) {
                Class<?> propertyConfiguratorClass = cl.loadClass("org.apache.log4j.PropertyConfigurator");
                Method configure = propertyConfiguratorClass.getMethod("configure", new Class[]{URL.class});
                configure.invoke(null, new Object[]{log4jprops});
            }
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    public static boolean isLog4JAvailable() {
        return isLog4JAvailable(getClassLoader());
    }

    private static ClassLoader getClassLoader() {
        return Log4JConfigView.class.getClassLoader();
    }

    private static boolean isLog4JAvailable(ClassLoader cl) {
        if (getLogManagerClass(cl) != null) {
            return true;
        }

        LOG.debug("Could not locate log4j classes on classpath.");

        return false;
    }

    private static Class<?> getLogManagerClass(ClassLoader cl) {
        Class<?> logManagerClass = null;
        try {
            logManagerClass = cl.loadClass("org.apache.log4j.LogManager");
        } catch (ClassNotFoundException e) {
        }
        return logManagerClass;
    }

    private static Class<?> getLoggerClass(ClassLoader cl) {
        Class<?> loggerClass = null;
        try {
            loggerClass = cl.loadClass("org.apache.log4j.Logger");
        } catch (ClassNotFoundException e) {
        }
        return loggerClass;
    }

    private static Class<?> getLevelClass(ClassLoader cl) {
        Class<?> levelClass = null;
        try {
            levelClass = cl.loadClass("org.apache.log4j.Level");
        } catch (ClassNotFoundException e) {
        }
        return levelClass;
    }
}
