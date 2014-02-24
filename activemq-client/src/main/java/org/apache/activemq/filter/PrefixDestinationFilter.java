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
 * 
 */
public class PrefixDestinationFilter extends DestinationFilter {

    private String[] prefixes;
    private byte destinationType;

    /**
     * An array of paths, the last path is '>'
     *
     * @param prefixes
     */
    public PrefixDestinationFilter(String[] prefixes, byte destinationType) {
        // collapse duplicate '>' at the end of the path
        int lastIndex = prefixes.length - 1;
        while (lastIndex >= 0 && ANY_DESCENDENT.equals(prefixes[lastIndex])) {
            lastIndex--;
        }
        this.prefixes = new String[lastIndex + 2];
        System.arraycopy(prefixes, 0, this.prefixes, 0, this.prefixes.length);
        this.destinationType = destinationType;
    }


    public boolean matches(ActiveMQDestination destination) {
        if (destination.getDestinationType() != destinationType) return false;
        String[] path = DestinationPath.getDestinationPaths(destination.getPhysicalName());


        int length = prefixes.length;
        if (path.length >= length) {
            int size = length - 1;
            for (int i = 0; i < size; i++) {
                if (!matches(prefixes[i],path[i])) {
                    return false;
                }
            }
            return true;
        }else{
            //want to look for the case where A matches A.>
            boolean match = true;
            for (int i = 0; (i < path.length && match); i++){
                   match = matches(prefixes[i], path[i]);
            }
            //paths get compacted - e.g. A.*.> will be compacted to A.> and by definition - the last element on
            //the prefix will be >
            if (match && prefixes.length == (path.length + 1)) {
                return true;
            }
        }
        return false;
    }

    private boolean matches(String prefix,String path){
        return path.equals(ANY_CHILD) || prefix.equals(ANY_CHILD) || prefix.equals(path);
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
