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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.filter.DestinationMap;

/**
 * Represents a destination based configuration of policies so that individual
 * destinations or wildcard hierarchies of destinations can be configured using
 * different policies. Each entry in the map represents the authorization ACLs
 * for each operation.
 * 
 * @org.apache.xbean.XBean element="authorizationMap"
 * 
 * @version $Revision$
 */
public class DefaultAuthorizationMap extends DestinationMap implements AuthorizationMap {

    private AuthorizationEntry defaultEntry;

    private TempDestinationAuthorizationEntry tempDestinationAuthorizationEntry;

    public DefaultAuthorizationMap() {
    }

    public DefaultAuthorizationMap(List authorizationEntries) {
        setAuthorizationEntries(authorizationEntries);

    }

    public void setTempDestinationAuthorizationEntry(
                                                     TempDestinationAuthorizationEntry tempDestinationAuthorizationEntry) {
        this.tempDestinationAuthorizationEntry = tempDestinationAuthorizationEntry;
    }

    public TempDestinationAuthorizationEntry getTempDestinationAuthorizationEntry() {
        return this.tempDestinationAuthorizationEntry;
    }

    public Set getTempDestinationAdminACLs() {
        if (tempDestinationAuthorizationEntry != null)
            return tempDestinationAuthorizationEntry.getAdminACLs();
        else
            return null;
    }

    public Set getTempDestinationReadACLs() {
        if (tempDestinationAuthorizationEntry != null)
            return tempDestinationAuthorizationEntry.getReadACLs();
        else
            return null;
    }

    public Set getTempDestinationWriteACLs() {
        if (tempDestinationAuthorizationEntry != null)
            return tempDestinationAuthorizationEntry.getWriteACLs();
        else
            return null;
    }

    public Set getAdminACLs(ActiveMQDestination destination) {
        Set entries = getAllEntries(destination);
        Set answer = new HashSet();
        // now lets go through each entry adding individual
        for (Iterator iter = entries.iterator(); iter.hasNext();) {
            AuthorizationEntry entry = (AuthorizationEntry)iter.next();
            answer.addAll(entry.getAdminACLs());
        }
        return answer;
    }

    public Set getReadACLs(ActiveMQDestination destination) {
        Set entries = getAllEntries(destination);
        Set answer = new HashSet();

        // now lets go through each entry adding individual
        for (Iterator iter = entries.iterator(); iter.hasNext();) {
            AuthorizationEntry entry = (AuthorizationEntry)iter.next();
            answer.addAll(entry.getReadACLs());
        }
        return answer;
    }

    public Set getWriteACLs(ActiveMQDestination destination) {
        Set entries = getAllEntries(destination);
        Set answer = new HashSet();

        // now lets go through each entry adding individual
        for (Iterator iter = entries.iterator(); iter.hasNext();) {
            AuthorizationEntry entry = (AuthorizationEntry)iter.next();
            answer.addAll(entry.getWriteACLs());
        }
        return answer;
    }

    public AuthorizationEntry getEntryFor(ActiveMQDestination destination) {
        AuthorizationEntry answer = (AuthorizationEntry)chooseValue(destination);
        if (answer == null) {
            answer = getDefaultEntry();
        }
        return answer;
    }

    /**
     * Sets the individual entries on the authorization map
     * 
     * @org.apache.xbean.ElementType class="org.apache.activemq.security.AuthorizationEntry"
     */
    public void setAuthorizationEntries(List entries) {
        super.setEntries(entries);
    }

    public AuthorizationEntry getDefaultEntry() {
        return defaultEntry;
    }

    public void setDefaultEntry(AuthorizationEntry defaultEntry) {
        this.defaultEntry = defaultEntry;
    }

    protected Class getEntryClass() {
        return AuthorizationEntry.class;
    }

    protected Set getAllEntries(ActiveMQDestination destination) {
        Set entries = get(destination);
        if (defaultEntry != null) {
            entries.add(defaultEntry);
        }
        return entries;
    }

}
