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
