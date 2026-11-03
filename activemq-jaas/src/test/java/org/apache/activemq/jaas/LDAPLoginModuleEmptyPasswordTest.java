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
package org.apache.activemq.jaas;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;

import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.security.auth.Subject;

import org.junit.Test;

/**
 * An empty or null password is not a valid credential, so bindUser must report a failed
 * bind for such input rather than treating the directory's response as a successful login.
 * Some directories answer a bind that carries a DN and an empty password as an anonymous
 * bind that does not check credentials, so the check belongs in bindUser itself.
 */
public class LDAPLoginModuleEmptyPasswordTest {

    private static final String USER_DN = "uid=jdoe,ou=users,ou=system";

    @Test
    public void emptyPasswordDoesNotBind() throws Exception {
        var module = newInitializedModule();
        assertFalse("empty password must not produce a successful bind",
                module.bindUser(acceptingContext(), USER_DN, ""));
    }

    @Test
    public void nullPasswordDoesNotBind() throws Exception {
        var module = newInitializedModule();
        assertFalse("null password must not produce a successful bind",
                module.bindUser(acceptingContext(), USER_DN, null));
    }

    @Test
    public void nonEmptyPasswordStillBinds() throws Exception {
        var module = newInitializedModule();
        assertTrue("a non-empty password must still bind when the directory accepts it",
                module.bindUser(acceptingContext(), USER_DN, "secret"));
    }

    private static LDAPLoginModule newInitializedModule() {
        var module = new LDAPLoginModule();
        module.initialize(new Subject(), null, null, new HashMap<String, String>());
        return module;
    }

    // A DirContext whose bind attempt (getAttributes) always succeeds, so the test
    // exercises bindUser's own handling of the password value.
    private static DirContext acceptingContext() {
        InvocationHandler handler = new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) {
                switch (method.getName()) {
                case "getAttributes":
                    return new BasicAttributes();
                case "toString":
                    return "acceptingContext";
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "equals":
                    return proxy == args[0];
                default:
                    return null;
                }
            }
        };
        return (DirContext) Proxy.newProxyInstance(
                LDAPLoginModule.class.getClassLoader(),
                new Class[]{DirContext.class}, handler);
    }
}
