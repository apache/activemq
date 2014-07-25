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
package org.apache.activemq.shiro.authc;

import org.apache.activemq.shiro.ConnectionReference;
import org.apache.activemq.shiro.subject.SubjectConnectionReference;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.apache.shiro.subject.Subject;

import java.util.Collection;

/**
 * @since 5.10.0
 */
public class DefaultAuthenticationPolicy implements AuthenticationPolicy {

    private boolean vmConnectionAuthenticationRequired = false;
    private String systemAccountUsername = "system";
    private String systemAccountRealmName = "iniRealm";

    private boolean anonymousAccessAllowed = false;
    private String anonymousAccountUsername = "anonymous";
    private String anonymousAccountRealmName = "iniRealm";

    public boolean isVmConnectionAuthenticationRequired() {
        return vmConnectionAuthenticationRequired;
    }

    public void setVmConnectionAuthenticationRequired(boolean vmConnectionAuthenticationRequired) {
        this.vmConnectionAuthenticationRequired = vmConnectionAuthenticationRequired;
    }

    public String getSystemAccountUsername() {
        return systemAccountUsername;
    }

    public void setSystemAccountUsername(String systemAccountUsername) {
        this.systemAccountUsername = systemAccountUsername;
    }

    public String getSystemAccountRealmName() {
        return systemAccountRealmName;
    }

    public void setSystemAccountRealmName(String systemAccountRealmName) {
        this.systemAccountRealmName = systemAccountRealmName;
    }

    public boolean isAnonymousAccessAllowed() {
        return anonymousAccessAllowed;
    }

    public void setAnonymousAccessAllowed(boolean anonymousAccessAllowed) {
        this.anonymousAccessAllowed = anonymousAccessAllowed;
    }

    public String getAnonymousAccountUsername() {
        return anonymousAccountUsername;
    }

    public void setAnonymousAccountUsername(String anonymousAccountUsername) {
        this.anonymousAccountUsername = anonymousAccountUsername;
    }

    public String getAnonymousAccountRealmName() {
        return anonymousAccountRealmName;
    }

    public void setAnonymousAccountRealmName(String anonymousAccountRealmName) {
        this.anonymousAccountRealmName = anonymousAccountRealmName;
    }

    /**
     * Returns {@code true} if the client connection has supplied credentials to authenticate itself, {@code false}
     * otherwise.
     *
     * @param conn the client's connection context
     * @return {@code true} if the client connection has supplied credentials to authenticate itself, {@code false}
     *         otherwise.
     */
    protected boolean credentialsAvailable(ConnectionReference conn) {
        return conn.getConnectionInfo().getUserName() != null || conn.getConnectionInfo().getPassword() != null;
    }

    @Override
    public boolean isAuthenticationRequired(SubjectConnectionReference conn) {
        Subject subject = conn.getSubject();

        if (subject.isAuthenticated()) {
            //already authenticated:
            return false;
        }
        //subject is not authenticated.  Authentication is required by default for all accounts other than
        //the anonymous user (if enabled) or the vm account (if enabled)
        if (isAnonymousAccessAllowed()) {
            if (isAnonymousAccount(subject)) {
                return false;
            }
        }

        if (!isVmConnectionAuthenticationRequired()) {
            if (isSystemAccount(subject)) {
                return false;
            }
        }

        return true;
    }

    protected boolean isAnonymousAccount(Subject subject) {
        PrincipalCollection pc = subject.getPrincipals();
        return pc != null && matches(pc, anonymousAccountUsername, anonymousAccountRealmName);
    }

    protected boolean isSystemAccount(Subject subject) {
        PrincipalCollection pc = subject.getPrincipals();
        return pc != null && matches(pc, systemAccountUsername, systemAccountRealmName);
    }

    protected boolean matches(PrincipalCollection principals, String username, String realmName) {
        Collection realmPrincipals = principals.fromRealm(realmName);
        if (realmPrincipals != null && !realmPrincipals.isEmpty()) {
            if (realmPrincipals.iterator().next().equals(username)) {
                return true;
            }
        }
        return false;
    }

    protected boolean isSystemConnection(ConnectionReference conn) {
        String remoteAddress = conn.getConnectionContext().getConnection().getRemoteAddress();
        return remoteAddress.startsWith("vm:");
    }

    @Override
    public void customizeSubject(Subject.Builder subjectBuilder, ConnectionReference conn) {
        // We only need to specify a custom identity or authentication state if a normal authentication will not occur.
        // If the client supplied connection credentials, the AuthenticationFilter will perform a normal authentication,
        // so we should exit immediately:
        if (credentialsAvailable(conn)) {
            return;
        }

        //The connection cannot be authenticated, potentially implying a system or anonymous connection.  Check if so:
        if (isAssumeIdentity(conn)) {
            PrincipalCollection assumedIdentity = createAssumedIdentity(conn);
            subjectBuilder.principals(assumedIdentity);
        }
    }

    /**
     * Returns {@code true} if an unauthenticated connection should still assume a specific identity, {@code false}
     * otherwise.  This method will <em>only</em> be called if there are no connection
     * {@link #credentialsAvailable(ConnectionReference) credentialsAvailable}.
     * If a client supplies connection credentials, they will always be used to authenticate the client with that
     * identity.
     * <p/>
     * If {@code true} is returned, the assumed identity will be returned by
     * {@link #createAssumedIdentity(ConnectionReference) createAssumedIdentity}.
     * <h3>Warning</h3>
     * This method exists primarily to support the system and anonymous accounts - it is probably unsafe to return
     * {@code true} in most other scenarios.
     *
     * @param conn a reference to the client's connection
     * @return {@code true} if an unauthenticated connection should still assume a specific identity, {@code false}
     *         otherwise.
     */
    protected boolean isAssumeIdentity(ConnectionReference conn) {
        return isAnonymousAccessAllowed() ||
                (isSystemConnection(conn) && !isVmConnectionAuthenticationRequired());
    }

    /**
     * Returns a Shiro {@code PrincipalCollection} representing the identity to assume (without true authentication) for
     * the specified Connection.
     * <p/>
     * This method is <em>only</em> called if {@link #isAssumeIdentity(ConnectionReference)} is {@code true}.
     *
     * @param conn a reference to the client's connection
     * @return a Shiro {@code PrincipalCollection} representing the identity to assume (without true authentication) for
     *         the specified Connection.
     */
    protected PrincipalCollection createAssumedIdentity(ConnectionReference conn) {

        //anonymous by default:
        String username = anonymousAccountUsername;
        String realmName = anonymousAccountRealmName;

        //vm connections are special and should assume the system account:
        if (isSystemConnection(conn)) {
            username = systemAccountUsername;
            realmName = systemAccountRealmName;
        }

        return new SimplePrincipalCollection(username, realmName);
    }
}
