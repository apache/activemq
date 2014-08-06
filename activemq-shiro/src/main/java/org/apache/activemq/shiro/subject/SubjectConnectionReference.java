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
import org.apache.activemq.shiro.ConnectionReference;
import org.apache.shiro.env.Environment;
import org.apache.shiro.subject.Subject;

/**
 * {@link org.apache.activemq.shiro.ConnectionReference} that further provides access to the connection's Subject instance.
 *
 * @since 5.10.0
 */
public class SubjectConnectionReference extends ConnectionReference {

    private final Subject subject;

    public SubjectConnectionReference(ConnectionContext connCtx, ConnectionInfo connInfo,
                                      Environment environment, Subject subject) {
        super(connCtx, connInfo, environment);
        if (subject == null) {
            throw new IllegalArgumentException("Subject argument cannot be null.");
        }
        this.subject = subject;
    }

    public Subject getSubject() {
        return subject;
    }
}
