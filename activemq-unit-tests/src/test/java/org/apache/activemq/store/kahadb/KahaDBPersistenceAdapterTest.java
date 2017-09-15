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
package org.apache.activemq.store.kahadb;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.PersistenceAdapterTestSupport;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class KahaDBPersistenceAdapterTest extends PersistenceAdapterTestSupport {
    
    protected PersistenceAdapter createPersistenceAdapter(boolean delete) throws IOException {
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(new File("target/activemq-data/kahadb"));
        if (delete) {
            kaha.deleteAllMessages();
        }
        return kaha;
    }

    public void testNoReplayOnStop() throws Exception {
        brokerService.getPersistenceAdapter().checkpoint(true);
        brokerService.stop();

        final AtomicBoolean gotSomeReplay = new AtomicBoolean(Boolean.FALSE);
        final AtomicBoolean trappedLogMessages = new AtomicBoolean(Boolean.FALSE);

        Appender appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                trappedLogMessages.set(true);
                if (event.getLevel().equals(Level.INFO)) {
                    if (event.getMessage().toString().contains("Recovery replayed ")) {
                        gotSomeReplay.set(true);
                    }
                }
            }
        };

        try {
            Logger.getLogger(MessageDatabase.class.getName()).addAppender(appender);
            Logger.getLogger(MessageDatabase.class.getName()).setLevel(Level.INFO);

            brokerService = new BrokerService();
            pa = createPersistenceAdapter(false);
            brokerService.setPersistenceAdapter(pa);
            brokerService.start();

        } finally {
            Logger.getRootLogger().removeAppender(appender);
            Logger.getLogger(MessageDatabase.class.getName()).removeAppender(appender);
        }
        assertTrue("log capture working", trappedLogMessages.get());
        assertFalse("no replay message in the log", gotSomeReplay.get());
    }
}
