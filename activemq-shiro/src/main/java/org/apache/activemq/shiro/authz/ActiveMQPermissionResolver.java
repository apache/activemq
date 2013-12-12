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
package org.apache.activemq.shiro.authz;

import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.WildcardPermission;
import org.apache.shiro.authz.permission.WildcardPermissionResolver;

/**
 * {@link WildcardPermissionResolver} that can create case-sensitive (or case-insensitive)
 * {@link WildcardPermission} instances as expected for ActiveMQ.
 *
 * @since 5.10.0
 */
public class ActiveMQPermissionResolver extends WildcardPermissionResolver {

    private boolean caseSensitive;

    public ActiveMQPermissionResolver() {
        caseSensitive = true;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public void setCaseSensitive(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
    }

    /**
     * Creates a new {@link WildcardPermission} instance, with case-sensitivity determined by the
     * {@link #isCaseSensitive() caseSensitive} setting.
     *
     * @param permissionString the wildcard permission-formatted string.
     * @return a new {@link WildcardPermission} instance, with case-sensitivity determined by the
     *         {@link #isCaseSensitive() caseSensitive} setting.
     */
    @Override
    public Permission resolvePermission(String permissionString) {
        return new ActiveMQWildcardPermission(permissionString, isCaseSensitive());
    }
}
