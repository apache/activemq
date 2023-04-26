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

import org.junit.Test;

import java.net.URI;

import static junit.framework.TestCase.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

public class ReplicaPolicyTest {

    @Test
    public void testGetTransportConnectorUriNotSet() {
        ReplicaPolicy replicaPolicy = new ReplicaPolicy();
        Throwable exception = assertThrows(NullPointerException.class, replicaPolicy::getTransportConnectorUri);
        assertEquals("Need replication transport connection URI for this broker", exception.getMessage());
    }

    @Test
    public void testGetTransportConnectorUriSet() throws Exception {
        URI uri = new URI("localhost:8080");
        ReplicaPolicy replicaPolicy = new ReplicaPolicy();
        replicaPolicy.setTransportConnectorUri(uri);
        assertThat(replicaPolicy.getTransportConnectorUri()).isEqualTo(uri);
    }
}
