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
package org.apache.activemq.web.util;

public class ExceptionUtils {

    /**
     * Finds the root cause of an exception.  Will return the original
     * exception if the first getCause returns null.
     *
     * @param e
     * @return
     */
    public static Throwable getRootCause(final Throwable e) {
        Throwable result = e;

        //loop over to find the root cause while guarding against cycles
        while(result != null && result.getCause() != null
                && e != result.getCause() && result != result.getCause() ) {
            result = result.getCause();
        }

        return result;
    }

    /**
     * Returns true if the passed in class is the root cause of the exception
     *
     * @param e
     * @param clazz
     * @return
     */
    public static boolean isRootCause(final Throwable e, final Class<?> clazz) {
        if (clazz == null || e == null) {
            return false;
        }
        return clazz.isInstance(getRootCause(e));
    }

}
