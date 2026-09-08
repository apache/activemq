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

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Tokenizes javadoc tag text exactly as QDox 1.x's
 * {@code com.thoughtworks.qdox.model.util.TagParser} did, so that the
 * generated schema does not change when the parser does. One consequence worth
 * knowing: {@link StreamTokenizer} ends a quoted string at a line break, so a
 * quoted value that wraps onto the next source line keeps only its first line.
 */
public final class TagParameters {

    private TagParameters() {
    }

    static StreamTokenizer makeTokenizer(String tagValue) {
        var tokenizer = new StreamTokenizer(new StringReader(tagValue));
        tokenizer.resetSyntax();
        tokenizer.wordChars('A', 'Z');
        tokenizer.wordChars('a', 'z');
        tokenizer.wordChars('0', '9');
        tokenizer.wordChars('-', '-');
        tokenizer.wordChars('_', '_');
        tokenizer.wordChars('.', '.');
        tokenizer.wordChars('<', '<');
        tokenizer.wordChars('>', '>');
        tokenizer.quoteChar('\'');
        tokenizer.quoteChar('"');
        tokenizer.whitespaceChars(' ', ' ');
        tokenizer.whitespaceChars('\t', '\t');
        tokenizer.whitespaceChars('\n', '\n');
        tokenizer.whitespaceChars('\r', '\r');
        tokenizer.eolIsSignificant(false);
        return tokenizer;
    }

    /** the {@code key=value} pairs of a tag, in order; parsing stops at the first token that is not a pair */
    public static Map<String, String> parseNamedParameters(String tagValue) {
        var parameters = new LinkedHashMap<String, String>();
        StreamTokenizer tokenizer = makeTokenizer(tagValue);
        try {
            while (tokenizer.nextToken() == StreamTokenizer.TT_WORD) {
                String key = tokenizer.sval;
                if (tokenizer.nextToken() != '=') {
                    break;
                }
                switch (tokenizer.nextToken()) {
                case StreamTokenizer.TT_WORD:
                case '"':
                case '\'':
                    parameters.put(key, tokenizer.sval);
                    break;
                default:
                    break;
                }
            }
        } catch (IOException e) {
            // a StringReader does not throw
        }
        return parameters;
    }

    /** the whitespace separated words of a tag value, quotes removed */
    public static List<String> parseWords(String tagValue) {
        var words = new ArrayList<String>();
        StreamTokenizer tokenizer = makeTokenizer(tagValue);
        try {
            while (tokenizer.nextToken() != StreamTokenizer.TT_EOF) {
                if (tokenizer.sval == null) {
                    words.add(Character.toString((char) tokenizer.ttype));
                } else {
                    words.add(tokenizer.sval);
                }
            }
        } catch (IOException e) {
            // a StringReader does not throw
        }
        return words;
    }
}
