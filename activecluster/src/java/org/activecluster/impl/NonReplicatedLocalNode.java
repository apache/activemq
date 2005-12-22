/**
 *
 * Copyright 2004 The Apache Software Foundation
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
 */
package org.activecluster.impl;

import java.util.Map;
import org.activecluster.LocalNode;

/**
 * Default implementation of a local Node which doesn't
 * have its state replicated
 * 
 * @version $Revision: 1.4 $
 */
public class NonReplicatedLocalNode extends NodeImpl implements LocalNode {
    private static final long serialVersionUID=2525565639637967143L;

    /**
     * Create a Non-replicated local node
     * @param name
     * @param destination
     */
    public NonReplicatedLocalNode(String name, String destination) {
        super(name,destination);
    }

    /**
     * Set the local state
     * @param state 
     */
    public void setState(Map state) {
        super.setState(state);
    }

    /**
     * Shouldn't be called for non-replicated local nodes
     */
    public void pingRemoteNodes() {
      throw new RuntimeException("Non-Replicated Local Node should not distribute it's state!");
    }
}