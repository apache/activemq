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
package org.apache.activecluster.group;

/**
 * Represents the membership of a Group for a Node
 *
 * @version $Revision: 1.2 $
 */
public class Membership {
    public static final int STATUS_REQUESTED = 1;
    public static final int STATUS_SYNCHONIZING = 2;
    public static final int STATUS_FAILED = 3;
    public static final int STATUS_OK = 4;

    private Group group;
    private int index;
    private int status = STATUS_REQUESTED;

    public Membership(Group group, int index) {
        this.group = group;
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    /**
     * @return the weighting of this membership
     */
    public int getWeighting() {
        // lets make master heavy and the further from the end of the
        // list of slaves, the lighter we become
        return group.getMaximumMemberCount() - getIndex();
    }
}
