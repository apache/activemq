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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.activemq.filter.FunctionCallExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.util.LRUCache;

/**
 * Filter function that matches a value against a regular expression.
 * <p/>
 * <p style="margin-left: 4em">
 * REGEX( 'A.B', 'A-B' )
 * </p>
 * <p/>
 * Note that the regular expression is not anchored; use the anchor characters, ^ and $, as-needed.  For example,
 * REGEX( 'AA', 'XAAX' ) evaluates to true while REGEX( '^AA$' , 'XAAX' ) evaluates to false.
 */

public class regexMatchFunction implements FilterFunction {
    protected static final LRUCache<String, Pattern> compiledExprCache = new LRUCache(100);

    /**
     * Check whether the given expression is a valid call of this function.  Two arguments are required.  When
     * evaluated, the arguments are converted to strings if they are not already strings.
     *
     * @param    expr - the expression consisting of a call to this function.
     * @return true - if the expression is valid; false - otherwise.
     */

    public boolean isValid(FunctionCallExpression expr) {
        if (expr.getNumArguments() == 2)
            return true;

        return false;
    }


    /**
     * Indicate that this Filter Function evaluates to a Boolean result.
     *
     * @param    expr - the expression consisting of a call to this function.
     * @return true - this function always evaluates to a Boolean result.
     */

    public boolean returnsBoolean(FunctionCallExpression expr) {
        return true;
    }

    /**
     * Evalutate the given expression, which consists of a call to this function, in the context given.  Returns
     * an indication of whether the second argument matches the regular expression in the first argument.
     *
     * @param    expr - the expression consisting of a call to this function.
     * @param    message_ctx - the context in which the call is being evaluated.
     * @return true - if the value matches the regular expression; false - otherwise.
     */

    public Object evaluate(FunctionCallExpression expr, MessageEvaluationContext message)
            throws javax.jms.JMSException {
        Object reg;
        Object cand;
        String reg_str;
        String cand_str;
        Pattern pat;
        Matcher match_eng;

        //
        // Evaluate the first argument (the regular expression).
        //
        reg = expr.getArgument(0).evaluate(message);

        if (reg != null) {
            // Convert to a string, if it's not already a string.
            if (reg instanceof String)
                reg_str = (String) reg;
            else
                reg_str = reg.toString();


            //
            // Evaluate the second argument (the candidate to match against the regular
            //  expression).
            //
            cand = expr.getArgument(1).evaluate(message);

            if (cand != null) {
                // Convert to a string, if it's not already a string.
                if (cand instanceof String)
                    cand_str = (String) cand;
                else
                    cand_str = cand.toString();


                //
                // Obtain the compiled regular expression and match it.
                //

                pat = getCompiledPattern(reg_str);
                match_eng = pat.matcher(cand_str);


                //
                // Return an indication of whether the regular expression matches at any
                //  point in the candidate (see Matcher#find()).
                //

                return Boolean.valueOf(match_eng.find());
            }
        }

        return Boolean.FALSE;
    }


    /**
     * Retrieve a compiled pattern for the given pattern string.  A cache of recently used strings is maintained to
     * improve performance.
     *
     * @param    reg_ex_str - the string specifying the regular expression.
     * @return Pattern - compiled form of the regular expression.
     */

    protected Pattern getCompiledPattern(String reg_ex_str) {
        Pattern result;

        //
        // Look for the compiled pattern in the cache.
        //

        synchronized (compiledExprCache) {
            result = compiledExprCache.get(reg_ex_str);
        }


        //
        // If it was not found, compile it and add it to the cache.
        //

        if (result == null) {
            result = Pattern.compile(reg_ex_str);

            synchronized (compiledExprCache) {
                compiledExprCache.put(reg_ex_str, result);
            }
        }

        return result;
    }
}
