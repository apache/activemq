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
package org.apache.activemq.shiro.authz;

import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.WildcardPermission;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

/**
 * @since 5.10.0
 */
public class ActiveMQWildcardPermission extends WildcardPermission {

    private final boolean caseSensitive;

    public ActiveMQWildcardPermission(String wildcardString) {
        this(wildcardString, true);
    }

    public ActiveMQWildcardPermission(String wildcardString, boolean caseSensitive) {
        super(wildcardString, caseSensitive);
        this.caseSensitive = caseSensitive;
    }

    @Override
    public boolean implies(Permission p) {
        // By default only supports comparisons with other WildcardPermissions
        if (!(p instanceof WildcardPermission)) {
            return false;
        }

        WildcardPermission wp = (WildcardPermission) p;

        List<Set<String>> otherParts = getParts(wp);

        int i = 0;
        for (Set<String> otherPart : otherParts) {
            // If this permission has less parts than the other permission, everything after the number of parts contained
            // in this permission is automatically implied, so return true
            if (getParts().size() - 1 < i) {
                return true;
            } else {
                Set<String> thisPart = getParts().get(i);

                // all tokens from otherPart must pass at least one token from thisPart
                for (String otherToken : otherPart) {
                    if (!caseSensitive) {
                        otherToken = otherToken.toLowerCase();
                    }
                	boolean otherIsMatched = false;
                	for (String token : thisPart) {
                        if (token.equals(WILDCARD_TOKEN)) {
                        	otherIsMatched = true;
                        	break;
                        }
                        if (matches(token, otherToken)) {
                        	otherIsMatched = true;
                        	break;
                        }
                	}
                	if (!otherIsMatched) {
                		return false;
                	}
                }
                i++;
            }
        }

        // If this permission has more parts than the other parts, only imply it if all of the other parts are wildcards
        for (; i < getParts().size(); i++) {
            Set<String> part = getParts().get(i);
            if (!part.contains(WILDCARD_TOKEN)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Tests whether or not a string matches against a pattern.
     * The pattern may contain two special characters:<br>
     * '*' means zero or more characters<br>
     * '?' means one and only one character
     *
     * @param pattern pattern to match against.
     *                Must not be <code>null</code>.
     * @param value   string which must be matched against the pattern.
     *                Must not be <code>null</code>.
     * @return <code>true</code> if the string matches against the
     *         pattern, or <code>false</code> otherwise.
     */
    protected boolean matches(String pattern, String value) {

        char[] patArr = pattern.toCharArray();
        char[] valArr = value.toCharArray();
        int patIndex = 0;
        int patEndIndex = patArr.length - 1;
        int valIndex = 0;
        int valEndIndex = valArr.length - 1;
        char ch;

        boolean patternContainsStar = false;
        for (char patternChar : patArr) {
            if (patternChar == '*') {
                patternContainsStar = true;
                break;
            }
        }

        if (!patternContainsStar) {
            // No '*'s, so we make a shortcut
            if (patEndIndex != valEndIndex) {
                return false; // Pattern and string do not have the same size
            }
            for (int i = 0; i <= patEndIndex; i++) {
                ch = patArr[i];
                if (ch != '?') {
                    if (ch != valArr[i]) {
                        return false;// Character mismatch
                    }
                }
            }
            return true; // String matches against pattern
        }


        // Process characters before first star
        while ((ch = patArr[patIndex]) != '*' && valIndex <= valEndIndex) {
            if (ch != '?') {
                if (ch != valArr[valIndex]) {
                    return false;// Character mismatch
                }
            }
            patIndex++;
            valIndex++;
        }
        if (valIndex > valEndIndex) {
            // All characters in the value are used. Check if only '*'s remain
            // in the pattern. If so, we succeeded. Otherwise failure.
            for (int i = patIndex; i <= patEndIndex; i++) {
                if (patArr[i] != '*') {
                    return false;
                }
            }
            return true;
        }

        // Process characters after last star
        while ((ch = patArr[patEndIndex]) != '*' && valIndex <= valEndIndex) {
            if (ch != '?') {
                if (ch != valArr[valEndIndex]) {
                    return false;// Character mismatch
                }
            }
            patEndIndex--;
            valEndIndex--;
        }
        if (valIndex > valEndIndex) {
            // All characters in the value are used. Check if only '*'s remain
            // in the pattern. If so, we succeeded. Otherwise failure.
            for (int i = patIndex; i <= patEndIndex; i++) {
                if (patArr[i] != '*') {
                    return false;
                }
            }
            return true;
        }

        // process pattern between stars. patIndex and patEndIndex always point to a '*'.
        while (patIndex != patEndIndex && valIndex <= valEndIndex) {
            int innerPatternIndex = -1;
            for (int i = patIndex + 1; i <= patEndIndex; i++) {
                if (patArr[i] == '*') {
                    innerPatternIndex = i;
                    break;
                }
            }
            if (innerPatternIndex == patIndex + 1) {
                // Two stars next to each other, skip the first one.
                patIndex++;
                continue;
            }
            // Find the pattern between patIndex & innerPatternIndex in the value between
            // valIndex and valEndIndex
            int innerPatternLength = (innerPatternIndex - patIndex - 1);
            int innerValueLength = (valEndIndex - valIndex + 1);
            int foundIndex = -1;
            innerValueLoop:
            for (int i = 0; i <= innerValueLength - innerPatternLength; i++) {
                for (int j = 0; j < innerPatternLength; j++) {
                    ch = patArr[patIndex + j + 1];
                    if (ch != '?') {
                        if (ch != valArr[valIndex + i + j]) {
                            continue innerValueLoop;
                        }
                    }
                }

                foundIndex = valIndex + i;
                break;
            }

            if (foundIndex == -1) {
                return false;
            }

            patIndex = innerPatternIndex;
            valIndex = foundIndex + innerPatternLength;
        }

        // All characters in the string are used. Check if only '*'s are left
        // in the pattern. If so, we succeeded. Otherwise failure.
        for (int i = patIndex; i <= patEndIndex; i++) {
            if (patArr[i] != '*') {
                return false;
            }
        }

        return true;
    }

    protected List<Set<String>> getParts(WildcardPermission wp) {
        if (wp instanceof ActiveMQWildcardPermission) {
            return ((ActiveMQWildcardPermission) wp).getParts();
        } else {
            return getPartsByReflection(wp);
        }
    }

    protected List<Set<String>> getPartsByReflection(WildcardPermission wp) {
        try {
            return doGetPartsByReflection(wp);
        } catch (Exception e) {
            String msg = "Unable to obtain WildcardPermission instance's 'parts' value.";
            throw new IllegalStateException(msg, e);
        }
    }

    @SuppressWarnings("unchecked")
    protected List<Set<String>> doGetPartsByReflection(WildcardPermission wp) throws Exception {
        Method getParts = WildcardPermission.class.getDeclaredMethod("getParts");
        getParts.setAccessible(true);
        return (List<Set<String>>) getParts.invoke(wp);
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        for (Set<String> part : getParts()) {
            if (buffer.length() > 0) {
                buffer.append(":");
            }
            boolean first = true;
            for (String token : part) {
                if (!first) {
                    buffer.append(",");
                }
                buffer.append(token);
                first = false;
            }
        }
        return buffer.toString();
    }
}
