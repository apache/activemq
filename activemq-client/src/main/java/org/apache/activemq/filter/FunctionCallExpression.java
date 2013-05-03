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

import java.util.HashMap;
import java.util.List;
import org.apache.activemq.filter.function.FilterFunction;

/**
 * Function call expression for use in selector expressions.  Includes an extensible interface to allow custom
 * functions to be added without changes to the core.
 * <p/>
 * Use registerFunction() to register new function implementations for use in selectors.
 */

public class FunctionCallExpression implements Expression {
    protected static final HashMap<String, functionRegistration> functionRegistry = new HashMap();

    protected String functionName;
    protected java.util.ArrayList arguments;
    protected FilterFunction filterFunc;

    static {
        // Register the built-in functions.  It would be nice to just have each function class register
        //  itself, but that only works when the classes are loaded, which may be never.

        org.apache.activemq.filter.function.BuiltinFunctionRegistry.register();
    }


    /**
     * Register the function with the specified name.
     *
     * @param    name - the function name, as used in selector expressions.  Case Sensitive.
     * @param    impl - class which implements the function interface, including parse-time and evaluation-time
     * operations.
     * @return true - if the function is successfully registered; false - if a function with the same name is
     * already registered.
     */

    public static boolean registerFunction(String name, FilterFunction impl) {
        boolean result;

        result = true;

        synchronized (functionRegistry) {
            if (functionRegistry.containsKey(name))
                result = false;
            else
                functionRegistry.put(name, new functionRegistration(impl));
        }

        return result;
    }

    /**
     * Remove the registration of the function with the specified name.
     * <p/>
     * Note that parsed expressions using this function will still access its implementation after this call.
     *
     * @param    name - name of the function to remove.
     */

    public static void deregisterFunction(String name) {
        synchronized (functionRegistry) {
            functionRegistry.remove(name);
        }
    }


    /**
     * Constructs a function call expression with the named function and argument list.
     * <p/>
     * Use createFunctionCall() to create instances.
     *
     * @exception invalidFunctionExpressionException - if the function name is not valid.
     */

    protected FunctionCallExpression(String func_name, List<Expression> args)
            throws invalidFunctionExpressionException {
        functionRegistration func_reg;

        synchronized (functionRegistry) {
            func_reg = functionRegistry.get(func_name);
        }

        if (func_reg != null) {
            this.arguments = new java.util.ArrayList();
            this.arguments.addAll(args);
            this.functionName = func_name;
            this.filterFunc = func_reg.getFilterFunction();
        } else {
            throw new invalidFunctionExpressionException("invalid function name, \"" + func_name + "\"");
        }
    }


    /**
     * Create a function call expression for the named function and argument list, returning a Boolean function
     * call expression if the function returns a boolean value so that it may be used in boolean contexts.
     * Used by the parser when a function call is identified.  Note that the function call is created after all
     * argument expressions so that the function call can properly detect whether it evaluates to a Boolean value.
     *
     * @param    func_name - name of the function, as used in selectors.
     * @param    args - list of argument expressions passed to the function.
     * @return an instance of a BooleanFunctionCallExpr if the function returns a boolean value in this call,
     * or a FunctionCallExpression otherwise.
     * @exception invalidFunctionExpression - if the function name is not valid, or the given argument list is
     * not valid for the function.
     */

    public static FunctionCallExpression createFunctionCall(String func_name, List<Expression> args)
            throws invalidFunctionExpressionException {
        FunctionCallExpression result;

        //
        // Create a function call expression by default to use with validating the function call
        //  expression and checking whether it returns a boolean result.
        //

        result = new FunctionCallExpression(func_name, args);


        //
        // Check wether the function accepts this expression.  I.E. are the arguments valid?
        //

        if (result.filterFunc.isValid(result)) {
            //
            // If the result of the call is known to alwyas return a boolean value, wrap this
            //  expression as a valid BooleanExpression so it will be accepted as a boolean result
            //  by the selector grammar.
            //

            if (result.filterFunc.returnsBoolean(result))
                result = new BooleanFunctionCallExpr(func_name, args);
        } else {
            //
            // Function does not like this expression.
            //

            throw new invalidFunctionExpressionException("invalid call of function " + func_name);
        }

        return result;
    }


    /**
     * Retrieve the number of arguments for the function call defined in this expression.
     *
     * @return the number of arguments being passed to the function.
     */

    public int getNumArguments() {
        return arguments.size();
    }


    /**
     * Retrieve the argument at the specified index; the first argument is index 0.  Used by implementations of
     * FilterFunction objects to check arguments and evaluate them, as needed.
     *
     * @param    which - number of the argument to retrieve; the first is 0.
     */

    public Expression getArgument(int which) {
        return (Expression) arguments.get(which);
    }


    /**
     * Evaluate the function call expression in the context given.
     *
     * @see    Expression#evaluate
     */

    public Object evaluate(MessageEvaluationContext message_ctx)
            throws javax.jms.JMSException {
        return this.filterFunc.evaluate(this, message_ctx);
    }


    /**
     * Translate the expression back into text in a form similar to the input to the selector parser.
     */

    @Override
    public String toString() {
        StringBuilder result;
        boolean first_f;

        result = new StringBuilder();

        result.append(functionName);
        result.append("(");
        first_f = true;

        for (Object arg : arguments) {
            if (first_f)
                first_f = false;
            else
                result.append(", ");

            result.append(arg.toString());
        }

        result.append(")");

        return result.toString();
    }


    ////                         ////
    ////  FUNCTION REGISTRATION  ////
    ////                         ////

    /**
     * Maintain a single function registration.
     */

    protected static class functionRegistration {
        protected FilterFunction filterFunction;

        /**
         * Constructs a function registration for the given function implementation.
         */

        public functionRegistration(FilterFunction func) {
            this.filterFunction = func;
        }


        /**
         * Retrieve the filter function implementation.
         */

        public FilterFunction getFilterFunction() {
            return filterFunction;
        }


        /**
         * Set the filter function implementation for this registration.
         */

        public void setFilterFunction(FilterFunction func) {
            filterFunction = func;
        }
    }


    /**
     * Exception indicating that an invalid function call expression was created, usually by the selector parser.
     * Conditions include invalid function names and invalid function arguments.
     */

    public static class invalidFunctionExpressionException extends java.lang.Exception {
        public invalidFunctionExpressionException(String msg) {
            super(msg);
        }

        public invalidFunctionExpressionException(String msg, java.lang.Throwable cause) {
            super(msg, cause);
        }
    }
}
