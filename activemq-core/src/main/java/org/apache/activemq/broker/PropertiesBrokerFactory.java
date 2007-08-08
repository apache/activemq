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
package org.apache.activemq.broker;

import org.apache.activemq.util.IntrospectionSupport;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.Properties;

/**
 * A {@link BrokerFactoryHandler} which uses a properties file to configure the
 * broker's various policies.
 * 
 * @version $Revision$
 */
public class PropertiesBrokerFactory implements BrokerFactoryHandler {

    public BrokerService createBroker(URI brokerURI) throws Exception {

        Map properties = loadProperties(brokerURI);
        BrokerService brokerService = createBrokerService(brokerURI, properties);

        IntrospectionSupport.setProperties(brokerService, properties);
        return brokerService;
    }

    /**
     * Lets load the properties from some external URL or a relative file
     */
    protected Map loadProperties(URI brokerURI) throws IOException {
        // lets load a URI
        String remaining = brokerURI.getSchemeSpecificPart();
        Properties properties = new Properties();
        File file = new File(remaining);

        InputStream inputStream = null;
        if (file.exists()) {
            inputStream = new FileInputStream(file);
        } else {
            URL url = null;
            try {
                url = new URL(remaining);
            } catch (MalformedURLException e) {
                // lets now see if we can find the name on the classpath
                inputStream = findResourceOnClassPath(remaining);
                if (inputStream == null) {
                    throw new IOException("File does not exist: " + remaining + ", could not be found on the classpath and is not a valid URL: " + e);
                }
            }
            if (inputStream == null && url != null) {
                inputStream = url.openStream();
            }
        }
        if (inputStream != null) {
            properties.load(inputStream);
            inputStream.close();
        }

        // should we append any system properties?
        try {
            Properties systemProperties = System.getProperties();
            properties.putAll(systemProperties);
        } catch (Exception e) {
            // ignore security exception
        }
        return properties;
    }

    protected InputStream findResourceOnClassPath(String remaining) {
        InputStream answer = Thread.currentThread().getContextClassLoader().getResourceAsStream(remaining);
        if (answer == null) {
            answer = getClass().getClassLoader().getResourceAsStream(remaining);
        }
        return answer;
    }

    protected BrokerService createBrokerService(URI brokerURI, Map properties) {
        return new BrokerService();
    }
}
