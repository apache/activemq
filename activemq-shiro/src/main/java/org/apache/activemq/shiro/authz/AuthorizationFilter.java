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

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.shiro.env.EnvironmentFilter;
import org.apache.activemq.shiro.subject.ConnectionSubjectResolver;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.UnauthorizedException;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.Subject;

import java.util.Collection;

/**
 * The {@code AuthorizationFilter} asserts that actions are allowed to execute first before they are actually
 * executed.  Such actions include creating, removing, reading from and writing to destinations.
 * <p/>
 * This implementation is strictly permission-based, allowing for the finest-grained security policies possible.
 * Whenever a {@link Subject} associated with a connection attempts to perform an {@link org.apache.activemq.shiro.authz.Action} (such as creating a
 * destination, or reading from a queue, etc), one or more {@link Permission}s representing that {@code action} are
 * checked.
 * <p/>
 * If the {@code Subject}{@link Subject#isPermitted(org.apache.shiro.authz.Permission) isPermitted} to perform the
 * {@code action}, the action is allowed to execute and the broker filter chain executes uninterrupted.
 * <p/>
 * However, if the {@code Subject} is not permitted to perform the action, an {@link UnauthorizedException} will be
 * thrown, preventing the filter chain from executing that action.
 * <h2>ActionPermissionResolver</h2>
 * The attempted {@code Action} is guarded by one or more {@link Permission}s as indicated by a configurable
 * {@link #setActionPermissionResolver(org.apache.activemq.shiro.authz.ActionPermissionResolver) actionPermissionResolver}.  The
 * {@code actionPermissionResolver} indicates which permissions must be granted to the connection {@code Subject} in
 * order for the action to execute.
 * <p/>
 * The default {@code actionPermissionResolver} instance is a
 * {@link org.apache.activemq.shiro.authz.DestinationActionPermissionResolver DestinationActionPermissionResolver}, which indicates which permissions
 * are required to perform any action on a particular destination.  Those familiar with Shiro's
 * {@link org.apache.shiro.authz.permission.WildcardPermission WildcardPermission} syntax will find the
 * {@code DestinationActionPermissionResolver}'s
 * {@link org.apache.activemq.shiro.authz.DestinationActionPermissionResolver#createPermissionString createPermissionString} method
 * documentation valuable for understanding how destination actions are represented as permissions.
 *
 * @see org.apache.activemq.shiro.authz.ActionPermissionResolver
 * @see org.apache.activemq.shiro.authz.DestinationActionPermissionResolver
 * @since 5.10.0
 */
public class AuthorizationFilter extends EnvironmentFilter {

    private ActionPermissionResolver actionPermissionResolver;

    public AuthorizationFilter() {
        this.actionPermissionResolver = new DestinationActionPermissionResolver();
    }

    /**
     * Returns the {@code ActionPermissionResolver} used to indicate which permissions are required to be granted to
     * a {@link Subject} to perform a particular destination {@link org.apache.activemq.shiro.authz.Action}, (such as creating a
     * destination, or reading from a queue, etc).  The default instance is a
     * {@link DestinationActionPermissionResolver}.
     *
     * @return the {@code ActionPermissionResolver} used to indicate which permissions are required to be granted to
     *         a {@link Subject} to perform a particular destination {@link org.apache.activemq.shiro.authz.Action}, (such as creating a
     *         destination, or reading from a queue, etc).
     */
    public ActionPermissionResolver getActionPermissionResolver() {
        return actionPermissionResolver;
    }

    /**
     * Sets the {@code ActionPermissionResolver} used to indicate which permissions are required to be granted to
     * a {@link Subject} to perform a particular destination {@link org.apache.activemq.shiro.authz.Action}, (such as creating a
     * destination, or reading from a queue, etc).  Unless overridden by this method, the default instance is a
     * {@link DestinationActionPermissionResolver}.
     *
     * @param actionPermissionResolver the {@code ActionPermissionResolver} used to indicate which permissions are
     *                                 required to be granted to a {@link Subject} to perform a particular destination
     *                                 {@link org.apache.activemq.shiro.authz.Action}, (such as creating a destination, or reading from a queue, etc).
     */
    public void setActionPermissionResolver(ActionPermissionResolver actionPermissionResolver) {
        this.actionPermissionResolver = actionPermissionResolver;
    }

    /**
     * Returns the {@code Subject} associated with the specified connection using a
     * {@link org.apache.activemq.shiro.subject.ConnectionSubjectResolver}.
     *
     * @param ctx the connection context
     * @return the {@code Subject} associated with the specified connection.
     */
    protected Subject getSubject(ConnectionContext ctx) {
        return new ConnectionSubjectResolver(ctx).getSubject();
    }

    protected String toString(Subject subject) {
        PrincipalCollection pc = subject.getPrincipals();
        if (pc != null && !pc.isEmpty()) {
            return "[" + pc.toString() + "] ";
        }
        return "";
    }

    protected void assertAuthorized(DestinationAction action) {
        assertAuthorized(action, action.getVerb());
    }

    //ActiveMQ internals will create a ConnectionContext with a SecurityContext that is not
    //Shiro specific.  We need to allow actions for internal system operations:
    protected boolean isSystemBroker(DestinationAction action) {
        ConnectionContext context = action.getConnectionContext();
        SecurityContext securityContext = context.getSecurityContext();
        return securityContext != null && securityContext.isBrokerContext();
    }

    protected void assertAuthorized(DestinationAction action, String verbText) {
        if (!isEnabled() || isSystemBroker(action)) {
            return;
        }

        final Subject subject = getSubject(action.getConnectionContext());

        Collection<Permission> perms = this.actionPermissionResolver.getPermissions(action);

        if (!subject.isPermittedAll(perms)) {
            String msg = createUnauthorizedMessage(subject, action, verbText);
            throw new UnauthorizedException(msg);
        }
    }

    protected String createUnauthorizedMessage(Subject subject, DestinationAction action, String verbDisplayText) {
        return "Subject " + toString(subject) + "is not authorized to " + verbDisplayText + " destination: " + action.getDestination();
    }

    @Override
    public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {

        DestinationAction action = new DestinationAction(context, info.getDestination(), "create");
        assertAuthorized(action);

        super.addDestinationInfo(context, info);
    }

    @Override
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination, boolean create) throws Exception {

        DestinationAction action = new DestinationAction(context, destination, "create");
        assertAuthorized(action);

        return super.addDestination(context, destination, create);
    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {

        DestinationAction action = new DestinationAction(context, destination, "remove");
        assertAuthorized(action);

        super.removeDestination(context, destination, timeout);
    }

    @Override
    public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {

        DestinationAction action = new DestinationAction(context, info.getDestination(), "remove");
        assertAuthorized(action);

        super.removeDestinationInfo(context, info);
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {

        //Unlike when adding a producer, consumers must specify the destination at creation time, so we can rely on
        //a destination being available to perform the authz check:
        DestinationAction action = new DestinationAction(context, info.getDestination(), "read");
        assertAuthorized(action, "read from");

        return super.addConsumer(context, info);
    }

    @Override
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {

        // JMS allows producers to be created without first specifying a destination.  In these cases, every send
        // operation must specify a destination.  Because of this, we only authorize 'addProducer' if a destination is
        // specified. If not specified, the authz check in the 'send' method below will ensure authorization.
        if (info.getDestination() != null) {
            DestinationAction action = new DestinationAction(context, info.getDestination(), "write");
            assertAuthorized(action, "write to");
        }

        super.addProducer(context, info);
    }

    @Override
    public void send(ProducerBrokerExchange exchange, Message message) throws Exception {

        DestinationAction action = new DestinationAction(exchange.getConnectionContext(), message.getDestination(), "write");
        assertAuthorized(action, "write to");

        super.send(exchange, message);
    }

}
