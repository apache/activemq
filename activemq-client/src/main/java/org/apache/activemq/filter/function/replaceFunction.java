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
package org.apache.activemq.filter.function;

import org.apache.activemq.filter.FunctionCallExpression;
import org.apache.activemq.filter.MessageEvaluationContext;

/**
 * Function which replaces regular expression matches in a source string to a replacement literal.
 * <p/>
 * For Example:
 * REPLACE('1,2/3', '[,/]', ';') returns '1;2;3'
 */

public class replaceFunction implements FilterFunction {
    /**
     * Check whether the given expression is valid for this function.
     *
     * @param    expr - the expression consisting of a call to this function.
     * @return true - if three arguments are passed to the function; false - otherwise.
     */

    public boolean isValid(FunctionCallExpression expr) {
        if (expr.getNumArguments() == 3)
            return true;

        return false;
    }


    /**
     * Indicate that this function does not return a boolean value.
     *
     * @param    expr - the expression consisting of a call to this function.
     * @return false - this filter function always evaluates to a string.
     */

    public boolean returnsBoolean(FunctionCallExpression expr) {
        return false;
    }


    /**
     * Evaluate the given expression for this function in the given context.  The result of the evaluation is a
     * string with all matches of the regular expression, from the evaluation of the second argument, replaced by
     * the string result from the evaluation of the third argument.  Replacement is performed by
     * String#replaceAll().
     * <p/>
     * Note that all three arguments must be Strings.
     *
     * @param    expr - the expression consisting of a call to this function.
     * @return String - the result of the replacement.
     */

    public Object evaluate(FunctionCallExpression expr, MessageEvaluationContext message_ctx)
            throws javax.jms.JMSException {
        String src;
        String match_regex;
        String repl_lit;
        String result;

        src = (String) expr.getArgument(0).evaluate(message_ctx);
        match_regex = (String) expr.getArgument(1).evaluate(message_ctx);
        repl_lit = (String) expr.getArgument(2).evaluate(message_ctx);

        result = src.replaceAll(match_regex, repl_lit);

        return result;
    }
}
