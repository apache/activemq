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
package org.apache.activemq.usecases;

import javax.jms.Session;
import javax.jms.TemporaryQueue;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;
import org.apache.activemq.EmbeddedBrokerAndConnectionTestSupport;

/**
 * 
 * @version $Revision: 1.1 $
 */
public class CreateLotsOfTemporaryQueuesTest extends EmbeddedBrokerAndConnectionTestSupport {

    private static int numberToCreate = 500;
    private static long sleep = 20;


    public static void main(String[] args) {
        configure(args);
        TestRunner.run(suite());
    }
    
    public static Test suite() {
        return new TestSuite(CreateLotsOfTemporaryQueuesTest.class);
    }

    public void testCreateLotsOfTemporaryQueues() throws Exception {
        LOG.info("Creating " + numberToCreate + " temporary queue(s)");

        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        for (int i = 0; i < numberToCreate; i++) {
            if (i % 1000 == 0) {
                LOG.info("attempt " + i);
            }
            TemporaryQueue temporaryQueue = session.createTemporaryQueue();
            temporaryQueue.delete();
            Thread.sleep(sleep );
        }
        LOG.info("Created " + numberToCreate + " temporary queue(s)");
    }

    public static void configure(String[] args) {
        if (args.length > 0) {
            numberToCreate = Integer.parseInt(args[0]);
        }
    }
}
