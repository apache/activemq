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
package org.apache.activemq.broker.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AuditLogService {

    private static final Log LOG = LogFactory.getLog(AuditLogService.class);

    private AuditLogFactory factory;

    private static AuditLogService auditLog;

    public static AuditLogService getAuditLog() {
        if (auditLog == null) {
            auditLog = new AuditLogService();
        }
        return auditLog;
    }

    private AuditLogService() {
	   String auditLogFactory = System.getProperty("org.apache.activemq.audit.factory", "org.apache.activemq.broker.util.DefaultAuditLogFactory");
       try {
           factory = (AuditLogFactory) Class.forName(auditLogFactory).newInstance();
       } catch (Exception e) {
           LOG.warn("Cannot instantiate audit log factory '" + auditLogFactory + "', using default audit log factory", e);
           factory = new DefaultAuditLogFactory();
        }

    }

    public void log(String message) {
        for (AuditLog log : factory.getAuditLogs()) {
            log.log(message);
        }
    }
}
