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

/**
 * An MBean for adding and removing users, roles
 * and destinations.
 * 
 * @version $Revision: 1.1 $
 */
public interface SecurityAdminMBean {
    
    public static final String OPERATION_READ = "read";
    public static final String OPERATION_WRITE = "write";
    public static final String OPERATION_ADMIN = "admin";
    
    public void addRole(String role);
    public void removeRole(String role);

    public void addUserRole(String user, String role);
    public void removeUserRole(String user, String role);

    public void addTopicRole(String topic, String operation, String role);
    public void removeTopicRole(String topic, String operation, String role);
    
    public void addQueueRole(String queue, String operation, String role);
    public void removeQueueRole(String queue, String operation, String role);
    
}
