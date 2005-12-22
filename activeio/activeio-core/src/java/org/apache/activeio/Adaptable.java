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

package org.apache.activeio;


/**
 * Provides an Adaptable interface inspired by eclipse's IAdaptable class.  Highly used in ActiveIO since Channel and Packet
 * implementations may be layered and application code may want request the higher level layers/abstractions to adapt to give access
 * to the lower layer implementation details.
 * 
 * @version $Revision$
 */
public interface Adaptable {
    
    /**
     *  @Return object that is an instance of requested type and is associated this this object.  May return null if no 
     *  object of that type is associated.
     */
    Object getAdapter(Class target);
}
