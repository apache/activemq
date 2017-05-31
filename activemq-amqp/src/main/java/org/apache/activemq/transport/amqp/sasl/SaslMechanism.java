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
package org.apache.activemq.transport.amqp.sasl;

import org.apache.qpid.proton.engine.Sasl;

/**
 * A SASL Mechanism implements this interface in order to provide the
 * AmqpAuthenticator with the means of providing authentication services
 * in the SASL handshake step.
 */
public interface SaslMechanism {

    /**
     * Perform the SASL processing for this mechanism type.
     *
     * @param sasl
     *        the SASL server that has read the incoming SASL exchange.
     */
    void processSaslStep(Sasl sasl);

    /**
     * @return the User Name extracted from the SASL echange or null if none.
     */
    String getUsername();

    /**
     * @return the Password extracted from the SASL echange or null if none.
     */
    String getPassword();

    /**
     * @return the name of the implemented SASL mechanism.
     */
    String getMechanismName();

    /**
     * @return true if the SASL processing failed during a step.
     */
    boolean isFailed();

    /**
     * @return a failure error to explain why the mechanism failed.
     */
    String getFailureReason();

}
