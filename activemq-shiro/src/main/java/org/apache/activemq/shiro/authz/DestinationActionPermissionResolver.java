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

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.WildcardPermission;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * A {@code DestinationActionPermissionResolver} inspects {@link DestinationAction}s and returns one or more
 * {@link WildcardPermission}s that must be granted to a {@code Subject} in order for that {@code Subject} to
 * perform the action being taken on an {@link ActiveMQDestination}.
 * <p/>
 * See the {@link #createPermissionString createPermissionString documentation} to see what the
 * resulting {@link WildcardPermission} instances would look like.
 *
 * @see #createPermissionString(org.apache.activemq.command.ActiveMQDestination, String) )
 * @see #setPermissionStringPrefix(String)
 * @since 5.10.0
 */
public class DestinationActionPermissionResolver implements ActionPermissionResolver {

    private String permissionStringPrefix;
    private boolean permissionStringCaseSensitive = true;

    /**
     * Returns the String prefix that should be automatically prepended to a permission String before the
     * String is converted to a {@link WildcardPermission} instance.  This is convenient if you want to provide a
     * 'scope' or 'namespace' for ActiveMQ Destinations to clearly distinguish ActiveMQ-specific permissions from any
     * others you might assign to user accounts.  The default value is {@code null}, indicating no prefix will be
     * set by default.
     * <p/>
     * For example, the default settings might result in permissions Strings that look like this:
     * <pre>
     * topic:TEST:create
     * temp-queue:MyQueue:remove
     * topic:ActiveMQ.Advisory.*:read
     * </pre>
     * <p/>
     * However, if your application has any application-specific permissions that start with the tokens {@code topic},
     * {@code temp-topic}, {@code queue}, or {@code temp-queue}, you wouldn't be able to distinguish between
     * application-specific permissions and those specific to ActiveMQ.  In this case you might set the
     * {@code permissionStringPrefix}. For example, if you set:
     * {@code resolver.setPermissionStringPrefix(&quot;jms&quot;);}, the above permission strings would look like this:
     * <pre>
     * jms:topic:TEST:create
     * jms:temp-queue:MyQueue:remove
     * jms:topic:ActiveMQ.Advisory.*:read
     * </pre>
     * <p/>
     * Similarly, if the {@code permissionStringPrefix} was equal to {@code activeMQ}:
     * <pre>
     * activeMQ:topic:TEST:create
     * activeMQ:temp-queue:MyQueue:remove
     * activeMQ:topic:ActiveMQ.Advisory.*:read
     * </pre>
     *
     * @return any String prefix that should be automatically prepended to a permission String before the
     *         String is converted to a {@link WildcardPermission} instance.  Useful for namespacing permissions.
     */
    public String getPermissionStringPrefix() {
        return permissionStringPrefix;
    }

    /**
     * Sets the String prefix that should be automatically prepended to a permission String before the
     * String is converted to a {@link WildcardPermission} instance.  This is convenient if you want to provide a
     * 'scope' or 'namespace' for ActiveMQ Destinations to clearly distinguish ActiveMQ-specific permissions from any
     * others you might assign to user accounts. The default value is {@code null}, indicating no prefix will be
     * set by default.
     * <p/>
     * For example, the default settings might result in permissions Strings that look like this:
     * <pre>
     * topic:TEST:create
     * temp-queue:MyQueue:remove
     * topic:ActiveMQ.Advisory.*:read
     * </pre>
     * <p/>
     * However, if your application has any application-specific permissions that start with the tokens {@code topic},
     * {@code temp-topic}, {@code queue}, or {@code temp-queue}, you wouldn't be able to distinguish between
     * application-specific permissions and those specific to ActiveMQ.  In this case you might set the
     * {@code permissionStringPrefix}. For example, if you set:
     * {@code resolver.setPermissionStringPrefix(&quot;jms&quot;);}, the above permission strings would look like this:
     * <pre>
     * jms:topic:TEST:create
     * jms:temp-queue:MyQueue:remove
     * jms:topic:ActiveMQ.Advisory.*:read
     * </pre>
     * <p/>
     * Similarly, if the {@code permissionStringPrefix} was equal to {@code activeMQ}:
     * <pre>
     * activeMQ:topic:TEST:create
     * activeMQ:temp-queue:MyQueue:remove
     * activeMQ:topic:ActiveMQ.Advisory.*:read
     * </pre>
     *
     * @param permissionStringPrefix any String prefix that should be automatically prepended to a permission String
     *                               before the String is converted to a {@link WildcardPermission} instance.  Useful
     *                               for namespacing permissions.
     */
    public void setPermissionStringPrefix(String permissionStringPrefix) {
        this.permissionStringPrefix = permissionStringPrefix;
    }

    /**
     * Returns {@code true} if returned {@link WildcardPermission} instances should be considered case-sensitive,
     * {@code false} otherwise.  The default value is {@code true}, which is <em>not</em> the normal
     * {@link WildcardPermission} default setting.  This default was chosen to reflect ActiveMQ's
     * <a href="http://activemq.apache.org/are-destinations-case-sensitive.html">case-sensitive destination names</a>.
     *
     * @return {@code true} if returned {@link WildcardPermission} instances should be considered case-sensitive,
     *         {@code false} otherwise.
     */
    public boolean isPermissionStringCaseSensitive() {
        return permissionStringCaseSensitive;
    }

    /**
     * Sets whether returned {@link WildcardPermission} instances should be considered case-sensitive.
     * The default value is {@code true}, which is <em>not</em> the normal
     * {@link WildcardPermission} default setting.  This default was chosen to accurately reflect ActiveMQ's
     * <a href="http://activemq.apache.org/are-destinations-case-sensitive.html">case-sensitive destination names</a>.
     *
     * @param permissionStringCaseSensitive whether returned {@link WildcardPermission} instances should be considered
     *                                      case-sensitive.
     */
    public void setPermissionStringCaseSensitive(boolean permissionStringCaseSensitive) {
        this.permissionStringCaseSensitive = permissionStringCaseSensitive;
    }

    @Override
    public Collection<Permission> getPermissions(Action action) {
        if (!(action instanceof DestinationAction)) {
            throw new IllegalArgumentException("Action argument must be a " + DestinationAction.class.getName() + " instance.");
        }
        DestinationAction da = (DestinationAction) action;
        return getPermissions(da);
    }

    protected Collection<Permission> getPermissions(DestinationAction da) {
        ActiveMQDestination dest = da.getDestination();
        String verb = da.getVerb();
        return createPermissions(dest, verb);
    }

    protected Collection<Permission> createPermissions(ActiveMQDestination dest, String verb) {

        Set<Permission> set;

        if (dest.isComposite()) {
            ActiveMQDestination[] composites = dest.getCompositeDestinations();
            set = new LinkedHashSet<Permission>(composites.length);
            for(ActiveMQDestination d : composites) {
                Collection<Permission> perms = createPermissions(d, verb);
                set.addAll(perms);
            }
        } else {
            set = new HashSet<Permission>(1);
            String permString = createPermissionString(dest, verb);
            Permission perm = createPermission(permString);
            set.add(perm);
        }

        return set;
    }

    /**
     * Inspects the specified {@code destination} and {@code verb} and returns a {@link WildcardPermission}-compatible
     * String the represents the action.
     * <h3>Format</h3>
     * This implementation returns WildcardPermission strings with the following format:
     * <pre>
     * optionalPermissionStringPrefix + destinationType + ':' + destinationPhysicalName + ':' + actionVerb
     * </pre>
     * where:
     * <ol>
     * <li>{@code optionalPermissionStringPrefix} is the {@link #getPermissionStringPrefix() permissionStringPrefix}
     * followed by a colon delimiter (':').  This is only present if the {@code permissionStringPrefix} has been
     * specified and is non-null</li>
     * <li>{@code destinationType} is one of the following four string tokens:
     * <ul>
     * <li>{@code topic}</li>
     * <li>{@code temp-topic}</li>
     * <li>{@code queue}</li>
     * <li>{@code temp-queue}</li>
     * </ul>
     * based on whether the {@link DestinationAction#getDestination() destination} is
     * a topic, temporary topic, queue, or temporary queue (respectively).
     * </li>
     * <li>
     * {@code destinationPhysicalName} is
     * {@link org.apache.activemq.command.ActiveMQDestination#getPhysicalName() destination.getPhysicalName()}
     * </li>
     * <li>
     * {@code actionVerb} is {@link DestinationAction#getVerb() action.getVerb()}
     * </li>
     * </ol>
     * <h3>Examples</h3>
     * With the default settings (no {@link #getPermissionStringPrefix() permissionStringPrefix}), this might produce
     * strings that look like the following:
     * <pre>
     * topic:TEST:create
     * temp-queue:MyTempQueue:remove
     * queue:ActiveMQ.Advisory.*:read
     * </pre>
     * If {@link #getPermissionStringPrefix() permissionStringPrefix} was set to {@code jms}, the above examples would
     * look like this:
     * <pre>
     * jms:topic:TEST:create
     * jms:temp-queue:MyTempQueue:remove
     * jms:queue:ActiveMQ.Advisory.*:read
     * </pre>
     *
     * @param dest the destination to inspect and convert to a {@link WildcardPermission} string.
     * @param verb the behavior taken on the destination
     * @return a {@link WildcardPermission} string that represents the specified {@code action}.
     * @see #getPermissionStringPrefix() getPermissionStringPrefix() for more on why you might want to set this value
     */
    protected String createPermissionString(ActiveMQDestination dest, String verb) {
        if (dest.isComposite()) {
            throw new IllegalArgumentException("Use createPermissionStrings for composite destinations.");
        }

        StringBuilder sb = new StringBuilder();

        if (permissionStringPrefix != null) {
            sb.append(permissionStringPrefix);
            if (!permissionStringPrefix.endsWith(":")) {
                sb.append(":");
            }
        }

        if (dest.isTemporary()) {
            sb.append("temp-");
        }
        if (dest.isTopic()) {
            sb.append("topic:");
        } else {
            sb.append("queue:");
        }

        sb.append(dest.getPhysicalName());
        sb.append(':');
        sb.append(verb);

        return sb.toString();

    }

    protected Permission createPermission(String permissionString) {
        return new ActiveMQWildcardPermission(permissionString, isPermissionStringCaseSensitive());
    }
}
