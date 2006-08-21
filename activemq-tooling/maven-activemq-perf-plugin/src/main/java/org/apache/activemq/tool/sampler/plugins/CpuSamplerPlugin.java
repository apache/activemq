/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.tool.sampler.plugins;

public interface CpuSamplerPlugin {
	public final static String WINDOWS_2000 = "Windows 2000";
	public final static String WINDOWS_NT   = "Windows NT";
	public final static String WINDOWS_XP   = "Windows XP";
	public final static String WINDOWS_95   = "Windows 95";
	public final static String WINDOWS_CE   = "Windows CE";
	public final static String LINUX        = "Linux";
	public final static String SOLARIS      = "Solaris";
	public final static String AIX          = "AIX";
	public final static String FREEBSD      = "FreeBSD";
	public final static String MAC_OS       = "Mac OS";
	public final static String MAC_OS_X     = "Mac OS X";
	public final static String POWERPC      = "PowerPC";
	public final static String OS_2         = "OS/2";

	public String getCpuUtilizationStats();
    public void start();
    public void stop();
}
