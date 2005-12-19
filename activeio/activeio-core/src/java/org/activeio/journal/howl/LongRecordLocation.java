/**
 *
 * Copyright 2004 Hiram Chirino
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
package org.activeio.journal.howl;

import org.activeio.journal.RecordLocation;

/**
 * Provides a RecordLocation implementation for the long based
 * location pointers that HOWL uses.
 * 
 * @version $Revision: 1.1 $
 */
public class LongRecordLocation implements RecordLocation {

	final private long location;

	public LongRecordLocation(long l) {
		this.location = l;
	}

	/**
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(Object o) {
		return (int) (location - ((LongRecordLocation) o).location);
	}

	/**
	 * @return the original long location provided by HOWL
	 */
	public long getLongLocation() {
		return location;
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		int lowPart = (int) (0xFFFFFFFF & location);
		int highPart = (int) (0xFFFFFFFF & (location >> 4));
		return lowPart ^ highPart;
	}

	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object o) {
		if (o == null || o.getClass() != LongRecordLocation.class)
			return false;
		return ((LongRecordLocation) o).location == location;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return "0x" + Long.toHexString(location);
	}
}