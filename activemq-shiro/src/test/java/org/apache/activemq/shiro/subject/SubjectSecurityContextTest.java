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
package org.apache.activemq.shiro.subject;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.shiro.env.DefaultEnvironment;
import org.junit.Before;
import org.junit.Test;

/**
 * @since 5.10.0
 */
public class SubjectSecurityContextTest {

    SubjectSecurityContext ctx;

    @Before
    public void setUp() {
        SubjectConnectionReference conn = new SubjectConnectionReference(new ConnectionContext(), new ConnectionInfo(),
                new DefaultEnvironment(), new SubjectAdapter());
        ctx = new SubjectSecurityContext(conn);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testInOneOf() {
        ctx.isInOneOf(null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testGetAuthorizedWriteDests() {
        ctx.getAuthorizedWriteDests();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testGetPrincipals() {
        ctx.getPrincipals();
    }
}
