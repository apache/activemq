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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.activemq.tool.JmsConsumerSystem;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * Goal which touches a timestamp file.
 * 
 * @goal consumer
 * @phase process-sources
 */
public class ConsumerMojo extends AbstractMojo {

    private String[] validPrefix = {
        "sysTest.", "factory.", "consumer.", "tpSampler.", "cpuSampler."
    };

    public void execute() throws MojoExecutionException {
        JmsConsumerSystem.main(createArgument());
    }

    protected String[] createArgument() {
        List args = new ArrayList();
        Properties sysProps = System.getProperties();
        Set keys = new HashSet(sysProps.keySet());

        for (Iterator i = keys.iterator(); i.hasNext();) {
            String key = (String)i.next();
            if (isRecognizedProperty(key)) {
                args.add(key + "=" + sysProps.remove(key));
            }
        }
        return (String[])args.toArray(new String[0]);
    }

    protected boolean isRecognizedProperty(String key) {
        for (int j = 0; j < validPrefix.length; j++) {
            if (key.startsWith(validPrefix[j])) {
                return true;
            }
        }
        return false;
    }
}
