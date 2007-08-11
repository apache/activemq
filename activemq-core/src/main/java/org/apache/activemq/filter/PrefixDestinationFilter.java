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

package org.apache.activemq.filter;

import org.apache.activemq.command.ActiveMQDestination;


/**
 * Matches messages which match a prefix like "A.B.>"
 *
 * @version $Revision: 1.2 $
 */
public class PrefixDestinationFilter extends DestinationFilter {

    private String[] prefixes;

    /**
     * An array of paths, the last path is '>'
     *
     * @param prefixes
     */
    public PrefixDestinationFilter(String[] prefixes) {
        this.prefixes = prefixes;
    }

    public boolean matches(ActiveMQDestination destination) {
        String[] path = DestinationPath.getDestinationPaths(destination.getPhysicalName());
        int length = prefixes.length;
        if (path.length >= length) {
            int size = length - 1;
            for (int i = 0; i < size; i++) {
                if (!prefixes[i].equals(path[i])) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public String getText() {
        return DestinationPath.toString(prefixes);
    }

    public String toString() {
        return super.toString() + "[destination: " + getText() + "]";
    }

    public boolean isWildcard() {
        return true;
    }
}
