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

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.activemq.command.Response;
import org.apache.activemq.transport.TransportSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;

/**
 * Minimal transport stub for unit tests. Records all commands sent via
 * {@code oneway} and {@code request}, returning a valid {@link Response}
 * for synchronous requests so that consumer registration succeeds.
 */
class StubTransport extends TransportSupport {

    private final Queue<Object> sent = new ConcurrentLinkedQueue<>();
    private int receiveCounter;

    @Override
    public void oneway(Object command) throws IOException {
        receiveCounter++;
        sent.add(command);
    }

    @Override
    public Object request(Object command) throws IOException {
        receiveCounter++;
        sent.add(command);
        return new Response();
    }

    @Override
    public Object request(Object command, int timeout) throws IOException {
        return request(command);
    }

    Queue<Object> getSent() {
        return sent;
    }

    @Override
    public String getRemoteAddress() {
        return "stub://localhost";
    }

    @Override
    public int getReceiveCounter() {
        return receiveCounter;
    }

    @Override
    public X509Certificate[] getPeerCertificates() {
        return null;
    }

    @Override
    public void setPeerCertificates(X509Certificate[] certificates) {
    }

    @Override
    public WireFormat getWireFormat() {
        return null;
    }

    @Override
    protected void doStart() throws Exception {
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
    }
}
