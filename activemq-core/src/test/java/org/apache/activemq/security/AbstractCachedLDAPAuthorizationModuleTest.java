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
package org.apache.activemq.security;

import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.jaas.UserPrincipal;
import org.apache.directory.shared.ldap.model.message.ModifyRequest;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractCachedLDAPAuthorizationModuleTest 
    extends AbstractCachedLDAPAuthorizationMapLegacyTest {

    static final UserPrincipal JDOE = new UserPrincipal("jdoe");

    @Test
    public void testQuery() throws Exception {
        map.query();
        Set<?> readACLs = map.getReadACLs(new ActiveMQQueue("TEST.FOOBAR"));
        assertEquals("set size: " + readACLs, 3, readACLs.size());
        assertTrue("Contains admin group", readACLs.contains(ADMINS));
        assertTrue("Contains users group", readACLs.contains(USERS));
        assertTrue("Contains jdoe user", readACLs.contains(JDOE));

        Set<?> failedACLs = map.getReadACLs(new ActiveMQQueue("FAILED"));
        assertEquals("set size: " + failedACLs, 0, failedACLs.size());
        
        super.testQuery();
    }

    @Override
    protected final void setupModifyRequest(ModifyRequest request) {
        request.remove("member", getMemberAttributeValueForModifyRequest());
    }
    
    protected abstract String getMemberAttributeValueForModifyRequest();

    @Override
    protected SimpleCachedLDAPAuthorizationMap createMap() {
        SimpleCachedLDAPAuthorizationMap map = super.createMap();
        map.setLegacyGroupMapping(false);
        return map;
    }
}

