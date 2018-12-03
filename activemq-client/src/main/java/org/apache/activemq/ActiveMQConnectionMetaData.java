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
package org.apache.activemq;

import java.util.Enumeration;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jms.ConnectionMetaData;

/**
 * A <CODE>ConnectionMetaData</CODE> object provides information describing
 * the <CODE>Connection</CODE> object.
 */
public final class ActiveMQConnectionMetaData implements ConnectionMetaData {

    public static final String PROVIDER_VERSION;
    public static final int PROVIDER_MAJOR_VERSION;
    public static final int PROVIDER_MINOR_VERSION;
    public static final String PROVIDER_NAME = "ActiveMQ";
    public static final String DEFAULT_PLATFORM_DETAILS = "Java";
    public static final String PLATFORM_DETAILS;

    public static final ActiveMQConnectionMetaData INSTANCE = new ActiveMQConnectionMetaData();

    static {
        String version = null;
        int major = 0;
        int minor = 0;
        try {
            Package p = Package.getPackage("org.apache.activemq");
            if (p != null) {
                version = p.getImplementationVersion();
                if (version != null) {
                    Pattern pattern = Pattern.compile("(\\d+)\\.(\\d+).*");
                    Matcher m = pattern.matcher(version);
                    if (m.matches()) {
                        major = Integer.parseInt(m.group(1));
                        minor = Integer.parseInt(m.group(2));
                    }
                }
            }
        } catch (Throwable e) {
        }
        PROVIDER_VERSION = version;
        PROVIDER_MAJOR_VERSION = major;
        PROVIDER_MINOR_VERSION = minor;
        PLATFORM_DETAILS = ActiveMQConnectionMetaData.getPlatformDetails();
    }

    private ActiveMQConnectionMetaData() {
    }

    /**
     * Gets the JMS API version.
     *
     * @return the JMS API version
     */
    @Override
    public String getJMSVersion() {
        return "1.1";
    }

    /**
     * Gets the JMS major version number.
     *
     * @return the JMS API major version number
     */
    @Override
    public int getJMSMajorVersion() {
        return 1;
    }

    /**
     * Gets the JMS minor version number.
     *
     * @return the JMS API minor version number
     */
    @Override
    public int getJMSMinorVersion() {
        return 1;
    }

    /**
     * Gets the JMS provider name.
     *
     * @return the JMS provider name
     */
    @Override
    public String getJMSProviderName() {
        return "ActiveMQ";
    }

    /**
     * Gets the JMS provider version.
     *
     * @return the JMS provider version
     */
    @Override
    public String getProviderVersion() {
        return PROVIDER_VERSION;
    }

    /**
     * Gets the JMS provider major version number.
     *
     * @return the JMS provider major version number
     */
    @Override
    public int getProviderMajorVersion() {
        return PROVIDER_MAJOR_VERSION;
    }

    /**
     * Gets the JMS provider minor version number.
     *
     * @return the JMS provider minor version number
     */
    @Override
    public int getProviderMinorVersion() {
        return PROVIDER_MINOR_VERSION;
    }

    /**
     * Gets an enumeration of the JMSX property names.
     *
     * @return an Enumeration of JMSX property names
     */
    @Override
    public Enumeration<String> getJMSXPropertyNames() {
        Vector<String> jmxProperties = new Vector<String>();
        jmxProperties.add("JMSXUserID");
        jmxProperties.add("JMSXGroupID");
        jmxProperties.add("JMSXGroupSeq");
        jmxProperties.add("JMSXDeliveryCount");
        jmxProperties.add("JMSXProducerTXID");
        return jmxProperties.elements();
    }

    /**
     * Get the platform details for the JMS provider.
     *
     * @return String containing the platform details
     */
    private static String getPlatformDetails() {
        String details = "java";
        try {
            StringBuilder platformInfo = new StringBuilder(128);

            platformInfo.append("JVM: ");
            platformInfo.append(System.getProperty("java.version"));
            platformInfo.append(", ");
            platformInfo.append(System.getProperty("java.vm.version"));
            platformInfo.append(", ");
            platformInfo.append(System.getProperty("java.vendor"));
            platformInfo.append(", OS: ");
            platformInfo.append(System.getProperty("os.name"));
            platformInfo.append(", ");
            platformInfo.append(System.getProperty("os.version"));
            platformInfo.append(", ");
            platformInfo.append(System.getProperty("os.arch"));

            details = platformInfo.toString();
        } catch (Throwable e) {
        }
        return details;
    }
}
