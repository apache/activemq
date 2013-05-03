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
package org.apache.activemq.filter;

import java.util.List;

/**
 * Function call expression that evaluates to a boolean value.  Selector parsing requires BooleanExpression objects for
 * Boolean expressions, such as operands to AND, and as the final result of a selector.  This provides that interface
 * for function call expressions that resolve to Boolean values.
 * <p/>
 * If a function can return different types at evaluation-time, the function implementation needs to decide whether it
 * supports casting to Boolean at parse-time.
 *
 * @see    FunctionCallExpression#createFunctionCall
 */

public class BooleanFunctionCallExpr extends FunctionCallExpression implements BooleanExpression {
    /**
     * Constructs a function call expression with the named filter function and arguments, which returns a boolean
     * result.
     *
     * @param    func_name - Name of the filter function to be called when evaluated.
     * @param    args - List of argument expressions passed to the function.
     */

    public BooleanFunctionCallExpr(String func_name, List<Expression> args)
            throws invalidFunctionExpressionException {
        super(func_name, args);
    }


    /**
     * Evaluate the function call expression, in the given context, and return an indication of whether the
     * expression "matches" (i.e.&nbsp;evaluates to true).
     *
     * @param    message_ctx - message context against which the expression will be evaluated.
     * @return the boolean evaluation of the function call expression.
     */

    public boolean matches(MessageEvaluationContext message_ctx) throws javax.jms.JMSException {
        Boolean result;

        result = (Boolean) evaluate(message_ctx);

        if (result != null)
            return result.booleanValue();

        return false;
    }
}

