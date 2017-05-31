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

import org.apache.activemq.transport.Transport;

import java.io.IOException;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class TransportLoggerSupport {

    public static String defaultLogWriterName = "default";

    /**
     * Default port to control the transport loggers through JMX
     */
    public static int defaultJmxPort = 1099;


    public static interface SPI {
        public Transport createTransportLogger(Transport transport) throws IOException;
        public Transport createTransportLogger(Transport transport, String logWriterName, boolean dynamicManagement, boolean startLogging, int jmxPort) throws IOException;
    }

    final static public SPI spi;
    static {
        SPI temp;
        try {
            temp = (SPI) TransportLoggerSupport.class.getClassLoader().loadClass("org.apache.activemq.transport.TransportLoggerFactorySPI").newInstance();
        } catch (Throwable e) {
            temp = null;
        }
        spi = temp;
    }

    public static Transport createTransportLogger(Transport transport) throws IOException {
        if( spi!=null ) {
            return spi.createTransportLogger(transport);
        } else {
            return transport;
        }
    }

    public static Transport createTransportLogger(Transport transport, String logWriterName, boolean dynamicManagement, boolean startLogging, int jmxPort) throws IOException {
        if( spi!=null ) {
            return spi.createTransportLogger(transport, logWriterName, dynamicManagement, startLogging, jmxPort);
        } else {
            return transport;
        }
    }

}
