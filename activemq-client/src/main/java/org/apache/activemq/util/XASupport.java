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

import static javax.transaction.xa.XAResource.TMENDRSCAN;
import static javax.transaction.xa.XAResource.TMFAIL;
import static javax.transaction.xa.XAResource.TMJOIN;
import static javax.transaction.xa.XAResource.TMNOFLAGS;
import static javax.transaction.xa.XAResource.TMONEPHASE;
import static javax.transaction.xa.XAResource.TMRESUME;
import static javax.transaction.xa.XAResource.TMSTARTRSCAN;
import static javax.transaction.xa.XAResource.TMSUCCESS;
import static javax.transaction.xa.XAResource.TMSUSPEND;

public class XASupport {

    public static String toString(int flags) {
        if (flags == TMNOFLAGS) {
            return "TMNOFLAGS";
        }

        StringBuilder result = new StringBuilder();
        if (hasFlag(flags, TMENDRSCAN)) {
            add(result, "TMENDRSCAN");
        }
        if (hasFlag(flags, TMFAIL)) {
            add(result, "TMFAIL");
        }
        if (hasFlag(flags, TMJOIN)) {
            add(result, "TMJOIN");
        }
        if (hasFlag(flags, TMONEPHASE)) {
            add(result, "TMONEPHASE");
        }
        if (hasFlag(flags, TMRESUME)) {
            add(result, "TMRESUME");
        }
        if (hasFlag(flags, TMSTARTRSCAN)) {
            add(result, "TMSTARTRSCAN");
        }
        if (hasFlag(flags, TMSUCCESS)) {
            add(result, "TMSUCCESS");
        }
        if (hasFlag(flags, TMSUSPEND)) {
            add(result, "TMSUSPEND");
        }

        int nonStandardFlags = flags
                & ~TMENDRSCAN
                & ~TMFAIL
                & ~TMJOIN
                & ~TMONEPHASE
                & ~TMRESUME
                & ~TMSTARTRSCAN
                & ~TMSUCCESS
                & ~TMSUSPEND;

        if (nonStandardFlags != 0) {
            add(result, String.format("0x%08x", nonStandardFlags));
        }

        return result.toString();
    }

    private static boolean hasFlag(int flags, int flag) {
        return (flags & flag) == flag;
    }

    private static void add(StringBuilder result, String string) {
        if (result.length() > 0) {
            result.append(" | ");
        }
        result.append(string);
    }
}
