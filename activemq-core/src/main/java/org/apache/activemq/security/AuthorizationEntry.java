/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Set;

/**
 * Represents an entry in a {@link DefaultAuthorizationMap} for assigning
 * different operations (read, write, admin) of user roles to a specific
 * destination or a hierarchical wildcard area of destinations.
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision$
 */
public class AuthorizationEntry extends DestinationMapEntry {

    private Set readACLs = Collections.EMPTY_SET;
    private Set writeACLs = Collections.EMPTY_SET;
    private Set adminACLs = Collections.EMPTY_SET;

    public Set getAdminACLs() {
        return adminACLs;
    }

    public void setAdminACLs(Set adminACLs) {
        this.adminACLs = adminACLs;
    }

    public Set getReadACLs() {
        return readACLs;
    }

    public void setReadACLs(Set readACLs) {
        this.readACLs = readACLs;
    }

    public Set getWriteACLs() {
        return writeACLs;
    }

    public void setWriteACLs(Set writeACLs) {
        this.writeACLs = writeACLs;
    }

}
