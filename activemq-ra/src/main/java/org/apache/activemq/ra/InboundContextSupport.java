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
package org.apache.activemq.ra;

/**
 * A helper class used to provide access to the current active
 * {@link InboundContext} instance being used to process a message
 * in the current thread so that messages can be produced using the same
 * session.
 *
 * @version $Revision$
 */
public class InboundContextSupport {
    private static final ThreadLocal<InboundContext> threadLocal = new ThreadLocal<InboundContext>();

    /**
     * Returns the current {@link InboundContext} used by the current thread which is processing a message.
     * This allows us to access the current Session to send a message using the same underlying
     * session to avoid unnecessary XA or to use regular JMS transactions while using message driven POJOs.
     *
     * @return
     */
    public static InboundContext getActiveSessionAndProducer() {
        return threadLocal.get();
    }


    /**
     * Registers the session and producer which should be called before the
     * {@link javax.resource.spi.endpoint.MessageEndpoint#beforeDelivery(java.lang.reflect.Method)}
     * method is called.
     *
     * @param sessionAndProducer
     */
    public static void register(InboundContext sessionAndProducer) {
        threadLocal.set(sessionAndProducer);
    }

    /**
     * Unregisters the session and producer which should be called after the
     * {@link javax.resource.spi.endpoint.MessageEndpoint#afterDelivery()}
     * method is called.
     *
     * @param sessionAndProducer
     */
    public static void unregister(InboundContext sessionAndProducer) {
        threadLocal.set(null);
    }
}
