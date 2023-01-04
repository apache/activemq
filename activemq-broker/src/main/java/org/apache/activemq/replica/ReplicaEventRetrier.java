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
package org.apache.activemq.replica;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class ReplicaEventRetrier {

    private final Logger logger = LoggerFactory.getLogger(ReplicaEventRetrier.class);

    private final int INITIAL_SLEEP_RETRY_INTERVAL_MS = 10;
    private final int MAX_SLEEP_RETRY_INTERVAL_MS = 10000;

    private final Callable<Void> task;

    public ReplicaEventRetrier(Callable<Void> task) {
        this.task = task;
    }

    public void process() {
        long attemptNumber = 0;
        while (true) {
            try {
                task.call();
                return;
            } catch (Exception e) {
                logger.info("Caught exception while processing a replication event.", e);
                try {
                    int sleepInterval = Math.min((int)(INITIAL_SLEEP_RETRY_INTERVAL_MS * Math.pow(2.0, attemptNumber)),
                            MAX_SLEEP_RETRY_INTERVAL_MS);
                    attemptNumber++;
                    logger.info("Retry attempt number {}. Sleeping for {} ms.", attemptNumber, sleepInterval);
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException ex) {
                    logger.error("Retry sleep interrupted: {}", ex.toString());
                }
            }
        }
    }
}
