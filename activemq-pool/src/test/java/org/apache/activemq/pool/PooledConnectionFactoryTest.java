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
package org.apache.activemq.pool;

import static org.junit.Assert.assertNotNull;

import javax.naming.Reference;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test JNDI
 */
public class PooledConnectionFactoryTest {

    private final Logger LOG = LoggerFactory.getLogger(PooledConnectionFactoryTest.class);

    @Test(timeout=240000)
    public void testGetReference() throws Exception {
        PooledConnectionFactory factory = createPooledConnectionFactory();
        Reference ref = factory.getReference();
        assertNotNull(ref);
    }

    protected PooledConnectionFactory createPooledConnectionFactory() {
        PooledConnectionFactory cf = new PooledConnectionFactory(
            "vm://localhost?broker.persistent=false");
        LOG.debug("ConnectionFactory initialized.");
        return cf;
    }
}
