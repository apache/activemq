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
import org.fusesource.hawtbuf.Buffer;

/**
 * Implements the SASL Plain mechanism.
 */
public class PlainMechanism extends AbstractSaslMechanism {

    @Override
    public void processSaslStep(Sasl sasl) {
        byte[] data = new byte[sasl.pending()];
        sasl.recv(data, 0, data.length);
        Buffer[] parts = new Buffer(data).split((byte) 0);

        if (parts.length > 0) {
            username = parts[0].utf8().toString();
        }

        if (parts.length > 1) {
            password = parts[1].utf8().toString();
        }
    }

    @Override
    public String getMechanismName() {
        return "PLAIN";
    }
}
