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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;

class PrincipalProperties {
    private final Properties principals;
    private final long reloadTime;

    PrincipalProperties(final String type, final File source, final Logger log) {
        Properties props = new Properties();
        long reloadTime = 0;
        try {
            load(source, props);
            reloadTime = System.currentTimeMillis();
        } catch (IOException ioe) {
            log.warn("Unable to load " + type + " properties file " + source);
        }
        this.reloadTime = reloadTime;
        this.principals = props;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    Set<Map.Entry<String, String>> entries() {
        return (Set) principals.entrySet();
    }

    String getProperty(String name) {
        return principals.getProperty(name);
    }

    long getReloadTime() {
        return reloadTime;
    }

    private void load(final File source, Properties props) throws FileNotFoundException, IOException {
        FileInputStream in = new FileInputStream(source);
        try {
            props.load(in);
        } finally {
            in.close();
        }
    }
}
