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
package org.apache.activemq.protobuf.compiler;

import org.apache.activemq.protobuf.compiler.TextFormat.InvalidEscapeSequence;
import org.apache.activemq.protobuf.compiler.parser.ParseException;
import org.apache.activemq.protobuf.compiler.parser.Token;

public class ParserSupport {

    public static String decodeString(Token token) throws ParseException {
        
//        StringBuilder sb = new StringBuilder();
//        for (int i = 1; i < value.length() - 1; i++) {
//            char c = value.charAt(i);
//            if (c == '\'') {
//                if( i+1 < (value.length() - 1) ) {
//                    char e = value.charAt(i+1);
//                    switch(e) {
//                    case 'a': 
//                        sb.append((char)0x07);
//                        break;
//                    case 'b':
//                        sb.append("\b");
//                        break;
//                    case 'f':
//                        sb.append("\f");
//                        break;
//                    case 'n': 
//                        sb.append("\n");
//                        break;
//                    case 'r':
//                        sb.append("\r");
//                        break;
//                    case 't':
//                        sb.append("\t");
//                        break;
//                    case 'v':
//                        sb.append((char)0x0b);
//                        break;
//                    case '\\':
//                        sb.append("\\");
//                        break;
//                    case '\'':
//                        sb.append("'");
//                        break;
//                    case '\"':
//                        sb.append("\"");
//                        break;
//                    default:
//                        sb.append(e);
//                        break;
//                    }
//                } else {
//                    throw new RuntimeException("Invalid string litteral: "+value);
//                }
//            }
//            sb.append(c);
//        }
//        return sb.toString();
        
        try {
            return TextFormat.unescapeText(token.image.substring(1, token.image.length()-1));
        } catch (InvalidEscapeSequence e) {
            throw new ParseException("Invalid string litteral at line " + token.next.beginLine + ", column " + token.next.beginColumn+": "+e.getMessage());
        }
    }

}
