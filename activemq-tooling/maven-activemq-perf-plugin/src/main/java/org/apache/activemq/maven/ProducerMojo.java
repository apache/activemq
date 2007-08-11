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
package org.apache.activemq.maven;

import org.apache.activemq.tool.JmsProducerSystem;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;

/**
 * Goal which touches a timestamp file.
 *
 * @goal producer
 * @phase process
 */
public class ProducerMojo extends AbstractMojo {

    private String[] validPrefix = {
        "sysTest.",
        "factory.",
        "producer.",
        "tpSampler.",
        "cpuSampler."
    };

    public void execute() throws MojoExecutionException {
        JmsProducerSystem.main(createArgument());
    }

    protected String[] createArgument() {
        List args = new ArrayList();
        Properties sysProps = System.getProperties();
        Set keys = new HashSet(sysProps.keySet());

        for (Iterator i=keys.iterator(); i.hasNext();) {
            String key = (String)i.next();
            if (isRecognizedProperty(key)) {
                args.add(key + "=" + sysProps.remove(key));
            }
        }
        return (String[])args.toArray(new String[0]);
    }

    protected boolean isRecognizedProperty(String key) {
        for (int j=0; j<validPrefix.length; j++) {
            if (key.startsWith(validPrefix[j])) {
                return true;
            }
        }
        return false;
    }
}
