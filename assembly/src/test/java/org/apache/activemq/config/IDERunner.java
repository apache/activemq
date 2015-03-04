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
package org.apache.activemq.config;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.camel.util.FileUtil;

import java.io.File;

/**
 * A helper class that can be used to start the full broker distro with default configuration
 * in an IDE. It can be helpful for debugging/testing externally provided test cases.
 */
public class IDERunner {

    public static void main(String[] args) throws Exception {

        System.setProperty("activemq.base", ".");
        System.setProperty("activemq.home", "."); // not a valid home but ok for xml validation
        System.setProperty("activemq.data", "target/");
        System.setProperty("activemq.conf", "src/release/conf");

        FileUtil.removeDir(new File("target/kahadb"));

        BrokerService broker = BrokerFactory.createBroker("xbean:src/release/conf/activemq.xml");
        broker.start();
        broker.waitUntilStopped();

    }

}
