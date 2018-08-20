/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.broker.scheduler;

import org.apache.activemq.broker.SslContext;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SslContextTest {
    SslContext underTest = new SslContext();

    @Test
    public void testConcurrentGet() throws Exception {

        final int numReps = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(numReps);
        final SSLContext[] results = new SSLContext[numReps];

        for (int i=0; i<numReps; i++) {
            final int instanceIndex = i;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        results[instanceIndex] = underTest.getSSLContext();
                    } catch (NoSuchProviderException e) {
                        e.printStackTrace();
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    } catch (KeyManagementException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        executorService.shutdown();
        assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));

        for (int i=0; i<numReps; i++) {
            assertEquals("single instance", results[0], results[i]);
        }
    }
}
