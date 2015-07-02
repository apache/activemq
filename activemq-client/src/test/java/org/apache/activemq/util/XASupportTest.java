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
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class XASupportTest {

    private final int flags;
    private final String expectedResult;

    public XASupportTest(int flags, String expectedResult) {
        this.flags = flags;
        this.expectedResult = expectedResult;
    }

    @Parameters
    public static Iterable<Object[]> parameters() {
        List<Object[]> p = new LinkedList<Object[]>();

        // single values from XAResource
        add(p, TMENDRSCAN, "TMENDRSCAN");
        add(p, TMFAIL, "TMFAIL");
        add(p, TMJOIN, "TMJOIN");
        add(p, TMNOFLAGS, "TMNOFLAGS");
        add(p, TMONEPHASE, "TMONEPHASE");
        add(p, TMRESUME, "TMRESUME");
        add(p, TMSTARTRSCAN, "TMSTARTRSCAN");
        add(p, TMSUCCESS, "TMSUCCESS");
        add(p, TMSUSPEND, "TMSUSPEND");

        // combination of multiple flags
        add(p, TMONEPHASE | TMSUCCESS, "TMONEPHASE | TMSUCCESS");

        // flags not specified in XAResource
        add(p, TMSUCCESS | 0x00400000, "TMSUCCESS | 0x00400000");

        return p;
    }

    private static void add(List<Object[]> p, int flags, String expectedResult) {
        p.add(new Object[] { flags, expectedResult });
    }

    @Test
    public void test() throws Exception {
        assertThat(XASupport.toString(flags), is(expectedResult));
    }
}