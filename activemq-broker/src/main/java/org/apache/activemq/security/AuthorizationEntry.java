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

import org.apache.activemq.filter.DestinationMapEntry;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Represents an entry in a {@link DefaultAuthorizationMap} for assigning
 * different operations (read, write, admin) of user roles to a specific
 * destination or a hierarchical wildcard area of destinations.
 */
@SuppressWarnings("rawtypes")
public class AuthorizationEntry extends DestinationMapEntry {

    private Set<Object> readACLs = emptySet();
    private Set<Object> writeACLs = emptySet();
    private Set<Object> adminACLs = emptySet();

    protected String adminRoles;
    protected String readRoles;
    protected String writeRoles;

    private String groupClass;

    public String getGroupClass() {
        return groupClass;
    }

    private Set<Object> emptySet() {
        return Collections.emptySet();
    }

    public void setGroupClass(String groupClass) {
        this.groupClass = groupClass;
    }

    public Set<Object> getAdminACLs() {
        return adminACLs;
    }

    public void setAdminACLs(Set<Object> adminACLs) {
        this.adminACLs = adminACLs;
    }

    public Set<Object> getReadACLs() {
        return readACLs;
    }

    public void setReadACLs(Set<Object> readACLs) {
        this.readACLs = readACLs;
    }

    public Set<Object> getWriteACLs() {
        return writeACLs;
    }

    public void setWriteACLs(Set<Object> writeACLs) {
        this.writeACLs = writeACLs;
    }

    // helper methods for easier configuration in Spring
    // ACLs are already set in the afterPropertiesSet method to ensure that
    // groupClass is set first before
    // calling parceACLs() on any of the roles. We still need to add the call to
    // parceACLs inside the helper
    // methods for instances where we configure security programatically without
    // using xbean
    // -------------------------------------------------------------------------
    public void setAdmin(String roles) throws Exception {
        adminRoles = roles;
        setAdminACLs(parseACLs(adminRoles));
    }

    public void setRead(String roles) throws Exception {
        readRoles = roles;
        setReadACLs(parseACLs(readRoles));
    }

    public void setWrite(String roles) throws Exception {
        writeRoles = roles;
        setWriteACLs(parseACLs(writeRoles));
    }

    protected Set<Object> parseACLs(String roles) throws Exception {
        Set<Object> answer = new HashSet<Object>();
        StringTokenizer iter = new StringTokenizer(roles, ",");
        while (iter.hasMoreTokens()) {
            String name = iter.nextToken().trim();
            String groupClass = (this.groupClass != null ? this.groupClass : DefaultAuthorizationMap.DEFAULT_GROUP_CLASS);
            answer.add(DefaultAuthorizationMap.createGroupPrincipal(name, groupClass));
        }
        return answer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AuthorizationEntry)) return false;

        AuthorizationEntry that = (AuthorizationEntry) o;

        if (adminACLs != null ? !adminACLs.equals(that.adminACLs) : that.adminACLs != null) return false;
        if (adminRoles != null ? !adminRoles.equals(that.adminRoles) : that.adminRoles != null) return false;
        if (groupClass != null ? !groupClass.equals(that.groupClass) : that.groupClass != null) return false;
        if (readACLs != null ? !readACLs.equals(that.readACLs) : that.readACLs != null) return false;
        if (readRoles != null ? !readRoles.equals(that.readRoles) : that.readRoles != null) return false;
        if (writeACLs != null ? !writeACLs.equals(that.writeACLs) : that.writeACLs != null) return false;
        if (writeRoles != null ? !writeRoles.equals(that.writeRoles) : that.writeRoles != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = readACLs != null ? readACLs.hashCode() : 0;
        result = 31 * result + (writeACLs != null ? writeACLs.hashCode() : 0);
        result = 31 * result + (adminACLs != null ? adminACLs.hashCode() : 0);
        result = 31 * result + (adminRoles != null ? adminRoles.hashCode() : 0);
        result = 31 * result + (readRoles != null ? readRoles.hashCode() : 0);
        result = 31 * result + (writeRoles != null ? writeRoles.hashCode() : 0);
        result = 31 * result + (groupClass != null ? groupClass.hashCode() : 0);
        return result;
    }
}
