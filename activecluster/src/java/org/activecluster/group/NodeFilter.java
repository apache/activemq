/** 
 * 
 * Copyright 2005 LogicBlaze, Inc. (http://www.logicblaze.com)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 * 
 **/
package org.activecluster.group;

import org.activecluster.Node;

/**
 * Represents a filter on a Node to allow a pluggable
 * Strategy Pattern to decide which nodes can be master nodes etc.
 *
 * @version $Revision: 1.2 $
 */
public interface NodeFilter {

    /**
     * Returns true if the given node matches the filter
     */
    public boolean evaluate(Node node);
}
