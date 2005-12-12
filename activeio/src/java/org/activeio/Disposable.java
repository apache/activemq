/**
 *
 * Copyright 2004 Hiram Chirino
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.activeio;

/**
 * The Disposable interface is implemented by objects the aquire resources whoes life cycle must be
 * managed.  Once a Disposable has been disposed, it cannot be un-disposed. 
 *  
 * @version $Revision$
 */
public interface Disposable {

	/**
	 * This method should not throw any exceptions.  Cleaning up a Disposable object
	 * should be easy of an end user therefore do not make him have to handle an Exception. 
	 */
    void dispose();
        
}
