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

import java.util.Collection;

/**
 * An {@code ActionPermissionResolver} will inspect an {@link Action} and return
 * {@link Permission}s that must be granted to a {@link org.apache.shiro.subject.Subject Subject} in order for the
 * {@code Subject} to execute the action.
 * <p/>
 * If a {@code Subject} is not granted all of the returned permissions, the {@code Action} will not be executed.
 *
 * @since 5.10.0
 */
public interface ActionPermissionResolver {

    /**
     * Returns all {@link Permission}s that must be granted to a
     * {@link org.apache.shiro.subject.Subject Subject} in order for the {@code Subject} to execute the action, or
     * an empty collection if no permissions are required.
     * <p/>
     * Most implementations will probably return a single Permission, but multiple permissions are possible, especially
     * if the Action represents behavior attempted on a
     * <a href="http://activemq.apache.org/composite-destinations.html">Composite Destination</a>.
     *
     * @param action the action attempted
     * @return all {@link Permission}s that must be granted to a
     *         {@link org.apache.shiro.subject.Subject Subject} in order for the {@code Subject} to execute the action,
     *         or an empty collection if no permissions are required.
     */
    Collection<Permission> getPermissions(Action action);

}
