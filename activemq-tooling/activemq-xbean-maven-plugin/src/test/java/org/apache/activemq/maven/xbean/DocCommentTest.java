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
package org.apache.activemq.maven.xbean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Test;

public class DocCommentTest {

    /** what Elements.getDocComment returns: asterisks gone, the rest of each line intact */
    private static final String RAW = " First line of body.\n"
            + "    indented continuation {@link List} here  \n"
            + "\n"
            + " second paragraph\n"
            + "\n"
            + " @org.apache.xbean.XBean element=\"sample\"\n"
            + "                         description=\"Provides a simple\n"
            + "                         wrapped description\"\n"
            + "   @see Something\n"
            + " @param name the name\n"
            + " @param size how big\n";

    @Test
    public void bodyIsTextBeforeTheFirstTagMinusOneLeadingSpacePerLine() {
        DocComment doc = DocComment.parse(RAW);
        assertEquals("First line of body.\n   indented continuation {@link List} here  \n\nsecond paragraph", doc.body());
    }

    @Test
    public void tagsAreFoundHoweverIndentedAndOwnTheirContinuationLines() {
        DocComment doc = DocComment.parse(RAW);
        DocComment.Tag xbean = doc.tag("org.apache.xbean.XBean");
        assertEquals("sample", xbean.parameter("element"));
        assertEquals("a quoted value ends at the line break", "Provides a simple", xbean.parameter("description"));
        assertEquals("Something", doc.tag("see").value());
        assertEquals(List.of("name the name", "size how big"), doc.tags("param").stream().map(DocComment.Tag::value).toList());
        assertNull(doc.tag("missing"));
    }

    @Test
    public void tagOnlyCommentHasEmptyBodyAndNoCommentIsNull() {
        assertEquals("", DocComment.parse("@org.apache.xbean.InitMethod ").body());
        assertNull(DocComment.parse(null).body());
        assertNull(DocComment.parse(null).tag("anything"));
    }

    @Test
    public void tagParametersFollowQdoxTokenizing() {
        Map<String, String> parameters = TagParameters.parseNamedParameters("element=\"x-y\" flag=true other='single' nestedType=java.util.Map");
        assertEquals("x-y", parameters.get("element"));
        assertEquals("true", parameters.get("flag"));
        assertEquals("single", parameters.get("other"));
        assertEquals("java.util.Map", parameters.get("nestedType"));
        assertTrue("parsing stops at the first token that is not key=value",
                TagParameters.parseNamedParameters("loose element=\"x\"").isEmpty());
        assertEquals(List.of("size", "how", "big"), TagParameters.parseWords("size how  big"));
    }
}
