/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activeio.stats;

import java.io.PrintWriter;

/**
 * A helper class for printing indented text
 *
 * @version $Revision: 1.1 $
 */
public class IndentPrinter {

    private int indentLevel;
    private String indent;
    private PrintWriter out;

    public IndentPrinter() {
        this(new PrintWriter(System.out), "  ");
    }

    public IndentPrinter(PrintWriter out) {
        this(out, "  ");
    }

    public IndentPrinter(PrintWriter out, String indent) {
        this.out = out;
        this.indent = indent;
    }

    public void println(Object value) {
        out.print(value.toString());
        out.println();
    }

    public void println(String text) {
        out.print(text);
        out.println();
    }

    public void print(String text) {
        out.print(text);
    }

    public void printIndent() {
        for (int i = 0; i < indentLevel; i++) {
            out.print(indent);
        }
    }

    public void println() {
        out.println();
    }

    public void incrementIndent() {
        ++indentLevel;
    }

    public void decrementIndent() {
        --indentLevel;
    }

    public int getIndentLevel() {
        return indentLevel;
    }

    public void setIndentLevel(int indentLevel) {
        this.indentLevel = indentLevel;
    }

    public void flush() {
        out.flush();
    }
}
