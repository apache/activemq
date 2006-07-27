/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
package org.apache.activemq.broker.region.virtual;

import org.apache.activemq.broker.region.Destination;

import java.util.Collection;

/**
 * 
 * @version $Revision$
 */
public abstract class CompositeDestination implements VirtualDestination {

    private String name;
    private Collection forwardDestinations;
    private boolean forwardOnly = true;

    public Destination intercept(Destination destination) {
        return new CompositeDestinationInterceptor(destination, getForwardDestinations(), isForwardOnly());
    }

    public String getName() {
        return name;
    }

    /**
     * Sets the name of this composite destination
     */
    public void setName(String name) {
        this.name = name;
    }

    public Collection getForwardDestinations() {
        return forwardDestinations;
    }

    /**
     * Sets the list of destinations to forward to
     */
    public void setForwardDestinations(Collection forwardDestinations) {
        this.forwardDestinations = forwardDestinations;
    }

    public boolean isForwardOnly() {
        return forwardOnly;
    }

    /**
     * Sets if the virtual destination is forward only (and so there is no
     * physical queue to match the virtual queue) or if there is also a physical
     * queue with the same name).
     */
    public void setForwardOnly(boolean forwardOnly) {
        this.forwardOnly = forwardOnly;
    }
}
