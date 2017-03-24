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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.spring.Utils;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;

public class RuntimeConfigTestSupport {
    public static final Logger LOG = LoggerFactory.getLogger(RuntimeConfigTestSupport.class);

    public static final int SLEEP = 4; // seconds
    public static final String EMPTY_UPDATABLE_CONFIG = "emptyUpdatableConfig1000" ;
    protected BrokerService brokerService;

    @Rule
    public TestWatcher watchman = new TestWatcher() {
        @Override
        public void starting(Description description) {
          LOG.info("{} being run...", description.getMethodName());
        }
    };

    public void startBroker(String configFileName) throws Exception {
        brokerService = createBroker(configFileName);
        brokerService.start();
        brokerService.waitUntilStarted();

        // File system lastMod time granularity can be up to 2 seconds
        TimeUnit.SECONDS.sleep(SLEEP);
    }

    public BrokerService createBroker(String configFileName) throws Exception {
        brokerService = new BrokerService();
        return BrokerFactory.createBroker("xbean:org/apache/activemq/" + configFileName + ".xml");
    }

    protected void applyNewConfig(String configName, String newConfigName) throws Exception {
        applyNewConfig(configName, newConfigName, 0l);
    }

    protected void applyNewConfig(String configName, String newConfigName, long sleep) throws Exception {
        Resource resource = Utils.resourceFromString("org/apache/activemq");
        File file = new File(resource.getFile(), configName + ".xml");
        FileOutputStream current = new FileOutputStream(file);
        FileInputStream modifications = new FileInputStream(new File(resource.getFile(), newConfigName + ".xml"));
        modifications.getChannel().transferTo(0, Long.MAX_VALUE, current.getChannel());
        current.flush();
        current.getChannel().force(true);
        current.close();
        modifications.close();
        LOG.info("Updated: " + file + " (" + file.lastModified() + ") " + new Date(file.lastModified()));

        if (sleep > 0) {
            // wait for mods to kick in
            TimeUnit.SECONDS.sleep(sleep);
        }
    }

    @After
    public void stopBroker() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }
}
