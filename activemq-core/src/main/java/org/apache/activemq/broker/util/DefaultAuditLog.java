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

public class DefaultAuditLog implements AuditLog {

    private static final Log LOG = LogFactory.getLog("org.apache.activemq.audit");

    public static AuditLog getAuditLog() {
        String auditLogClass = System.getProperty("org.apache.activemq.audit.class", "org.apache.activemq.broker.util.DefaultAuditLog");
        AuditLog log;
        try {
            log = (AuditLog) Class.forName(auditLogClass).newInstance();
        } catch (Exception e) {
            LOG.warn("Cannot instantiate audit log class '" + auditLogClass + "', using default audit log", e);
            log = new DefaultAuditLog();
        }
        return log;
    }

    public void log(String message) {
         LOG.info(message);
    }
}
