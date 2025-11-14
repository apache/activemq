/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.compat.sm;

import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;

import javax.security.auth.Subject;

/*
 * SecurityManager related shim.
 * This specific class uses replacement APIs, direct-executes actions,
 * or no-ops as appropriate towards usage on Java 24+.
 *
 * The API of this class must be kept the same as the Java 17-23 implementation
 * variant of this class which can be found at:
 * src/main/java/org/apache/activemq/artemis/utils/sm/SecurityManagerShim.java
 *
 * The javadoc there should also be kept in sync, it covers both classes behaviour.
 *
 * Did not use a shared interface since the shim seems unlikely to be changed much,
 * other than future removals, and doing so would need singleton instance indirection
 * at every call site to access the purely static onward methods being called.
 */
public class SecurityManagerShim {

   public static Subject currentSubject() {
      return Subject.current();
   }

   public static <T> T callAs(final Subject subject, final Callable<T> callable) throws CompletionException {
      // Subject is allowed to be null
      Objects.requireNonNull(callable);

      return Subject.callAs(subject, callable);
   }

   public static boolean isSecurityManagerEnabled() {
      // Can never be enabled, it was removed in Java 24+.
      return false;
   }

   public static Object getAccessControlContext() {
      // AccessControlContext is now only useful with a SecurityManager,
      // which can never be used in Java 24+. Return null so calling
      // code can determine there is nothing to be do re: SecurityManager.
      return null;
   }

   public static<T> T doPrivileged(final PrivilegedAction<T> action) {
      Objects.requireNonNull(action, "action must be provided");

      return action.run();
   }

   public static<T> T doPrivileged(final PrivilegedAction<T> action, final Object accessControlContext) {
      // We ignore the accessControlContext parameter as it was only useful
      // with a SecurityManager, which can never be used in Java 24+.
      Objects.requireNonNull(action, "action must be provided");

      return action.run();
   }

   public static <T> T doPrivileged(final PrivilegedExceptionAction<T> exceptionAction) throws PrivilegedActionException {
      Objects.requireNonNull(exceptionAction, "exceptionAction must be provided");

      try {
         return exceptionAction.run();
      } catch (RuntimeException re) {
         // RuntimeExceptions were re-thrown directly by doPrivileged, only checked
         // Exceptions were wrapped in PrivilegedActionException to throw for intercept.
         throw re;
      } catch (Exception e) {
         throw new PrivilegedActionException(e);
      }
   }

}