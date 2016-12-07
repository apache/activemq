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
package org.apache.activemq.util;

import java.io.File;

/**
 * @author wcrowell
 *
 * LargeFile is used to simulate a large file system (e.g. exabytes in size).
 * The getTotalSpace() method is intentionally set to exceed the largest
 * value of a primitive long which is 9,223,372,036,854,775,807.  A negative
 * number will be returned when getTotalSpace() is called.  This class is for
 * test purposes only.  Using a mocking framework to mock the behavior of
 * java.io.File was a lot of work.
 *
 */
public class LargeFile extends File {
	public LargeFile(File parent, String child) {
		super(parent, child);
	}

	@Override
	public long getTotalSpace() {
		return Long.MAX_VALUE + 4193L;
	}

	@Override
	public long getUsableSpace() {
		return getTotalSpace() - 1024L;
	}
}
