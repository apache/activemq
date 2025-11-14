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

import javax.security.auth.Subject;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;

/*
 * SecurityManager related shim.
 * This specific class uses legacy methods toward usage on Java 17 - 23.
 *
 * The API of this class must be kept the same as the Java 24+ implementation
 * variant of this class which can be found at:
 * src/main/java24/org/apache/activemq/artemis/utils/sm/SecurityManagerShim.java
 *
 * Did not use a shared interface since the shim seems unlikely to be changed much,
 * other than future removals, and doing so would need singleton instance indirection
 * at every call site to access the purely static onward methods being called.
 */
@SuppressWarnings("removal")
public class SecurityManagerShim {

   /**
    * Returns the current associated subject.
    * <p>
    * On Java 17-23, retrieves the current AccessControlContext by calling
    * {@code AccessController.getContext()} and then returns result of method
    * {@code Subject.getSubject(accessControlContext)}.
    * <p>
    * On Java 24+, returns result of method {@code Subject.current()}.
    *
    * @return the current associated subject, or null if none was found.
    */
   public static Subject currentSubject() {
      AccessControlContext accessControlContext = AccessController.getContext();
      if (accessControlContext != null) {
         return Subject.getSubject(accessControlContext);
      }
      return null;
   }

   /**
    * Perform work as a particular {@code Subject}.
    * <p>
    * On Java 17-23, wraps the given {@code callable} as a {@code PrivilegedExceptionAction} and executes it
    * via {@code Subject.doAs(final Subject subject, final java.security.PrivilegedExceptionAction<T> action)}.
    * <p>
    * On Java 24+, returns result of calling {@code Subject.callAs(final Subject subject,
    * final Callable<T> action)}.
    * <p>
    * Any exceptions thrown by the {@code callable.call()} will result in a {@code CompletionException} being
    * thrown with the original exception as its cause.
    *
    * @param subject  the {@code Subject} that the given {@code callable} will run as, may be null.
    * @param callable  the {@code Callable} to be run, must not be {@code null}.
    * @param <T>  the type of value returned by the {@code callable}.
    * @return the value returned by the {@code callable}.
    * @throws NullPointerException  if {@code callable} is {@code null}.
    * @throws CompletionException  if {@code callable.call()} throws an exception.
    *                              The cause is set to the exception thrown by {@code callable.call()}.
    */
   public static <T> T callAs(final Subject subject, final Callable<T> callable) throws CompletionException {
      // Subject is allowed to be null
      Objects.requireNonNull(callable, "callable must be provided");

      try {
         final PrivilegedExceptionAction<T> pa = () -> callable.call();

         return Subject.doAs(subject, pa);
      } catch (PrivilegedActionException e) {
         throw new CompletionException(e.getCause());
      } catch (Exception e) {
         throw new CompletionException(e);
      }
   }

   /**
    * Returns whether a SecurityManager is enabled.
    * <p>
    * On Java 17-23, returns whether result of check: {@code System.getSecurityManager() != null}.
    * <p>
    * On Java 24+, returns false as a SecurityManager can never be present.
    *
    * @return true if a SecurityManager is present, or false otherwise.
    */
   public static boolean isSecurityManagerEnabled() {
      return System.getSecurityManager() != null;
   }

   /**
    * Returns the current AccessControlContext.
    * <p>
    * On Java 17-23, returns the result of {@code AccessController.getContext()}.
    * <p>
    * On Java 24+, always returns null.
    *
    * @return the current AccessControlContext, or null if none.
    */
   public static Object getAccessControlContext() {
      return AccessController.getContext();
   }

   /**
    * Performs the specified {@code PrivilegedAction}.
    * <p>
    * On Java 17-23, returns the result of passing the given {@code action} to the
    * {@code AccessController.doPrivileged(PrivilegedAction<T> action)} method.
    * <p>
    * On Java 24+, returns the result of running {@code action.run()} directly.
    * <p>
    * If the action's {@code run} method throws an unchecked exception it will
    * propagate through this method.
    *
    * @param action  the {@code PrivilegedAction} to be run, must not be {@code null}.
    * @param <T>  the type of value returned by the {@code action}.
    * @return the value returned by the {@code action}.
    * @throws NullPointerException  if {@code action} is {@code null}.
    */
   public static<T> T doPrivileged(final PrivilegedAction<T> action) {
      Objects.requireNonNull(action, "action must be provided");

      return AccessController.doPrivileged(action);
   }

   /**
    * Performs the specified {@code PrivilegedAction}.
    * <p>
    * On Java 17-23, returns the result of calling the
    * {@code AccessController.doPrivileged(PrivilegedAction<T> action, AccessControlContext context)}
    * method with the given {@code action} and {@code accessControlContext}.
    * <p>
    * On Java 24+, returns the result of running {@code action.run()} directly,
    * ignoring the accessControlContext parameter.
    *
    * If the action's {@code run} method throws an unchecked exception it will
    * propagate through this method.
    *
    * @param action  the {@code PrivilegedAction} to be run, must not be {@code null}.
    * @param accessControlContext  the {@code AccessControlContext} object, may be null.
    * @param <T>  the type of value returned by the {@code action}.
    * @return the value returned by the {@code action}.
    * @throws NullPointerException  if {@code action} is {@code null}.
    */
   public static<T> T doPrivileged(final PrivilegedAction<T> action, final Object accessControlContext) {
      // AccessControlContext may be null
      Objects.requireNonNull(action, "action must be provided");

      final AccessControlContext acc = AccessControlContext.class.cast(accessControlContext);

      return AccessController.doPrivileged(action, acc);
   }

   /**
    * Performs the specified {@code PrivilegedExceptionAction}.
    * <p>
    * On Java 17-23, returns the result of calling
    * {@code AccessController.doPrivileged(PrivilegedExceptionAction<T> action)}
    * with the given {@code exceptionAction}.
    * <p>
    * On Java 24+, returns the result of running {@code exceptionAction.run()} directly.
    * <p>
    * If the action's {@code run} method throws an unchecked exception it will
    * propagate through this method.
    *
    * @param exceptionAction  the {@code PrivilegedExceptionAction} to be run, must not be {@code null}.
    * @param <T>  the type of value returned by the {@code exceptionAction}.
    * @return the value returned by the {@code action}.
    * @throws NullPointerException  if {@code exceptionAction} is {@code null}.
    * @throws PrivilegedActionException  if {@code exceptionAction.run()} throws a checked exception.
    *                                    The cause is set to the exception thrown {@code callable.call()}.
    */
   public static <T> T doPrivileged(final PrivilegedExceptionAction<T> exceptionAction) throws PrivilegedActionException {
      Objects.requireNonNull(exceptionAction, "exceptionAction must be provided");

      return AccessController.doPrivileged(exceptionAction);
   }

}