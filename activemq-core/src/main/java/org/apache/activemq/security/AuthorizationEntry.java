/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

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
    
    private String groupClass = "org.apache.activemq.jaas.GroupPrincipal";
        
    public String getGroupClass() {
    	return groupClass;
    }
     
    public void setGroupClass(String groupClass) {
        this.groupClass = groupClass;
    }    

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

    // helper methods for easier configuration in Spring
    // -------------------------------------------------------------------------
    public void setAdmin(String roles) throws Exception {
        setAdminACLs(parseACLs(roles));
    }

    public void setRead(String roles) throws Exception {
        setReadACLs(parseACLs(roles));
    }

    public void setWrite(String roles) throws Exception {
        setWriteACLs(parseACLs(roles));
    }

    protected Set parseACLs(String roles) throws Exception {
        Set answer = new HashSet();
        StringTokenizer iter = new StringTokenizer(roles, ",");
        while (iter.hasMoreTokens()) {
            String name = iter.nextToken().trim();
            Class[] paramClass = new Class[1];
            paramClass[0] = String.class;
            
            Object[] param = new Object[1];
            param[0] = new String(name);

            try {
            	Class cls = Class.forName(groupClass);
            	
            	Constructor[] constructors = cls.getConstructors();
            	int i;
            	for (i=0; i<constructors.length; i++) {
            		Class[] paramTypes = constructors[i].getParameterTypes(); 
            		if (paramTypes.length!=0 && paramTypes[0].equals(paramClass[0])) break;
            	}
            	if (i < constructors.length) {
            		Object instance = constructors[i].newInstance(param);
                	answer.add(instance);
            	}
            	else {
            		Object instance = cls.newInstance();
            		Method[] methods = cls.getMethods();
            		i=0;
            		for (i=0; i<methods.length; i++) {
            			Class[] paramTypes = methods[i].getParameterTypes();
            			if (paramTypes.length!=0 && methods[i].getName().equals("setName") && paramTypes[0].equals(paramClass[0])) break;
            		}
                		
                	if (i < methods.length) {
                		methods[i].invoke(instance, param);
                    	answer.add(instance);
                	}
                	else throw new NoSuchMethodException();
            	}
            }
            catch (Exception e) { throw e; }
        }
        return answer;
    }
}
