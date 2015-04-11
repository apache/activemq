/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.filter;

import org.apache.activemq.command.ActiveMQDestination;


/**
 * Matches messages which match a suffix like "<.A.B"
 *
 *
 */
public class SuffixDestinationFilter extends DestinationFilter {

    private String[] suffixes;
    private byte destinationType;

    /**
     * An array of paths, the first path is '<'
     *
     * @param suffixes
     */
    public SuffixDestinationFilter(String[] suffixes, byte destinationType) {
        int firstIndex = 0;
        while (firstIndex < suffixes.length && ANY_ANCESTOR.equals(suffixes[firstIndex])) {
            firstIndex++;
        }
        this.suffixes = new String[suffixes.length - firstIndex];
        System.arraycopy(suffixes, firstIndex, this.suffixes, 0, this.suffixes.length);
        this.destinationType = destinationType;
    }

    private String pathFromEnd(String[] path, int offset) {
        return path[path.length - offset];
    }

    public boolean matches(ActiveMQDestination destination) {
        if (destination.getDestinationType() != destinationType) {
            return false;
        }
        String[] path = DestinationPath.getDestinationPaths(destination.getPhysicalName());
        for (int offset = 1; offset <= suffixes.length; ++offset) {
            if (!matches(pathFromEnd(path,offset), pathFromEnd(suffixes,offset))) {
                return false;
            }
        }
        return true;
    }

    private boolean matches(String pattern, String path) {
        return path.equals(ANY_CHILD) || pattern.equals(ANY_CHILD) || pattern.equals(path);
    }

    public String getText() {
        return DestinationPath.toString(suffixes);
    }

    public String toString() {
        return super.toString() + "[destination: " + (suffixes.length == 0 ? ANY_ANCESTOR : ANY_ANCESTOR + ".") + getText() + "]";
    }

    public boolean isWildcard() {
        return true;
    }
}
