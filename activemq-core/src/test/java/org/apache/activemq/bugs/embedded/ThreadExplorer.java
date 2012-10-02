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
package org.apache.activemq.bugs.embedded;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;

public class ThreadExplorer
{
    static Logger logger = Logger.getLogger(ThreadExplorer.class);

    public static Thread[] listThreads()
    {

        int nThreads = Thread.activeCount();
        Thread ret[] = new Thread[nThreads];

        Thread.enumerate(ret);

        return ret;

    }

    /**
     * Helper function to access a thread per name (ignoring case)
     * 
     * @param name
     * @return
     */
    public static Thread fetchThread(String name)
    {
        Thread[] threadArray = listThreads();
        // for (Thread t : threadArray)
        for (int i = 0; i < threadArray.length; i++)
        {
            Thread t = threadArray[i];
            if (t.getName().equalsIgnoreCase(name))
                return t;
        }
        return null;
    }

    /**
     * Allow for killing threads
     * 
     * @param threadName
     * @param isStarredExp
     *            (regular expressions with *)
     */
    @SuppressWarnings("deprecation")
    public static int kill(String threadName, boolean isStarredExp, String motivation)
    {
        String me = "ThreadExplorer.kill: ";
        if (logger.isDebugEnabled())
        {
            logger.debug("Entering " + me + " with " + threadName + " isStarred: " + isStarredExp);
        }
        int ret = 0;
        Pattern mypattern = null;
        if (isStarredExp)
        {
            String realreg = threadName.toLowerCase().replaceAll("\\*", "\\.\\*");
            mypattern = Pattern.compile(realreg);

        }
        Thread[] threads = listThreads();
        for (int i = 0; i < threads.length; i++)
        {
            Thread thread = threads[i];
            if (thread == null)
                continue;
            // kill the thread unless it is not current thread
            boolean matches = false;

            if (isStarredExp)
            {
                Matcher matcher = mypattern.matcher(thread.getName().toLowerCase());
                matches = matcher.matches();
            }
            else
            {
                matches = (thread.getName().equalsIgnoreCase(threadName));
            }
            if (matches && (Thread.currentThread() != thread) && !thread.getName().equals("main"))
            {
                if (logger.isInfoEnabled())
                    logger.info("Killing thread named [" + thread.getName() + "]"); // , removing its uncaught
                // exception handler to
                // avoid ThreadDeath
                // exception tracing
                // "+motivation );

                ret++;

                // PK leaving uncaught exception handler otherwise master push
                // cannot recover from this error
                // thread.setUncaughtExceptionHandler(null);
                try
                {
                    thread.stop();
                }
                catch (ThreadDeath e)
                {
                    logger.warn("Thread already death.", e);
                }

            }
        }
        return ret;
    }

    public static String show(String title)
    {
        StringBuffer out = new StringBuffer();
        Thread[] threadArray = ThreadExplorer.listThreads();

        out.append(title + "\n");
        for (int i = 0; i < threadArray.length; i++)
        {
            Thread thread = threadArray[i];

            if (thread != null)
            {
                out.append("* [" + thread.getName() + "] " + (thread.isDaemon() ? "(Daemon)" : "")
                        + " Group: " + thread.getThreadGroup().getName() + "\n");
            }
            else
            {
                out.append("* ThreadDeath: " + thread + "\n");
            }

        }
        return out.toString();
    }

    public static int active()
    {
        int count = 0;
        Thread[] threadArray = ThreadExplorer.listThreads();

        for (int i = 0; i < threadArray.length; i++)
        {
            Thread thread = threadArray[i];
            if (thread != null)
            {
                count++;
            }
        }

        return count;
    }

}
