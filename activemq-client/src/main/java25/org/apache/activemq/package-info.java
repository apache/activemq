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

/**
 * Java 27 variants of client classes, packaged under {@code META-INF/versions/27}
 * of the multi-release jar. The {@code jdk27-plus} profile in the module pom
 * compiles this root with {@code --release 27} only when the build runs on
 * JDK 27 or newer.
 *
 * <p>A class placed here with the same name as one in {@code src/main/java}
 * replaces it at run time on JDK 27 and later, as {@code SubjectShim} does in
 * activemq-broker; a class that exists only here must be reached by name, as
 * {@code TaskRunnerFactory} does for the Java 21 virtual thread classes.
 * The first intended occupants are the TLS 1.3 hybrid key exchange settings
 * that JEP 527 introduced in JDK 27.
 */
package org.apache.activemq;
