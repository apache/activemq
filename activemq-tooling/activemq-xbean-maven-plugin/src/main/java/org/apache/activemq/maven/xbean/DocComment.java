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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A javadoc comment split the way QDox split it. The body is the text before
 * the first block tag; each block tag owns the rest of its first line and every
 * continuation line. On every line one space after the asterisks goes and the
 * rest, trailing whitespace included, stays; lines are joined with newlines,
 * blank lines survive, and the whole text is trimmed at both ends. A tag is
 * recognised however far it is indented. Inline tags such as {@code {@link}}
 * stay in the text.
 */
public final class DocComment {

    private static final DocComment NONE = new DocComment(null, Collections.emptyList());

    private final String body;
    private final List<Tag> tags;

    private DocComment(String body, List<Tag> tags) {
        this.body = body;
        this.tags = tags;
    }

    /**
     * @param raw the text as returned by {@code Elements.getDocComment}, with the
     *            leading asterisks already removed, or null when the element has
     *            no javadoc comment
     */
    public static DocComment parse(String raw) {
        if (raw == null) {
            return NONE;
        }
        var body = new StringBuilder();
        var tags = new ArrayList<Tag>();
        String tagName = null;
        StringBuilder tagValue = null;
        for (String line : raw.split("\n", -1)) {
            // javac leaves everything after the leading asterisks; QDox also ate one space,
            // so an indented example keeps the rest of its indentation
            String text = line.startsWith(" ") ? line.substring(1) : line;
            String stripped = text.stripLeading();
            if (stripped.startsWith("@")) {
                if (tagName != null) {
                    tags.add(new Tag(tagName, tagValue.toString()));
                }
                int end = 1;
                while (end < stripped.length() && !Character.isWhitespace(stripped.charAt(end))) {
                    end++;
                }
                tagName = stripped.substring(1, end);
                tagValue = new StringBuilder(stripped.substring(end).stripLeading()).append('\n');
            } else if (tagName != null) {
                tagValue.append(text).append('\n');
            } else {
                body.append(text).append('\n');
            }
        }
        if (tagName != null) {
            tags.add(new Tag(tagName, tagValue.toString()));
        }
        return new DocComment(body.toString().trim(), Collections.unmodifiableList(tags));
    }

    /** the comment text before the first block tag, or null when there is no comment at all */
    public String body() {
        return body;
    }

    /** the first block tag with this name, or null */
    public Tag tag(String name) {
        for (Tag tag : tags) {
            if (tag.name.equals(name)) {
                return tag;
            }
        }
        return null;
    }

    /** every block tag with this name, in order */
    public List<Tag> tags(String name) {
        var result = new ArrayList<Tag>();
        for (Tag tag : tags) {
            if (tag.name.equals(name)) {
                result.add(tag);
            }
        }
        return result;
    }

    /** One block tag: the name after the {@code @} and the trimmed value text. */
    public static final class Tag {
        private final String name;
        private final String value;
        private Map<String, String> parameters;

        Tag(String name, String value) {
            this.name = name;
            this.value = value.trim();
        }

        public String name() {
            return name;
        }

        public String value() {
            return value;
        }

        /** {@code key="value"} pairs of the value, parsed as QDox did */
        public String parameter(String key) {
            if (parameters == null) {
                parameters = TagParameters.parseNamedParameters(value);
            }
            return parameters.get(key);
        }

        public boolean booleanParameter(String key) {
            String v = parameter(key);
            return v != null && Boolean.parseBoolean(v);
        }
    }
}
