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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import javax.jms.JMSException;

/**
 * A MultiExpressionEvaluator is used to evaluate multiple expressions in single
 * method call. <p/> Multiple Expression/ExpressionListener pairs can be added
 * to a MultiExpressionEvaluator object. When the MultiExpressionEvaluator
 * object is evaluated, all the registed Expressions are evaluated and then the
 * associated ExpressionListener is invoked to inform it of the evaluation
 * result. <p/> By evaluating multiple expressions at one time, some
 * optimizations can be made to reduce the number of computations normally
 * required to evaluate all the expressions. <p/> When this class adds an
 * Expression it wrapps each node in the Expression's AST with a CacheExpression
 * object. Then each CacheExpression object (one for each node) is placed in the
 * cachedExpressions map. The cachedExpressions map allows us to find the sub
 * expressions that are common across two different expressions. When adding an
 * Expression in, if a sub Expression of the Expression is allready in the
 * cachedExpressions map, then instead of wrapping the sub expression in a new
 * CacheExpression object, we reuse the CacheExpression allready int the map.
 * <p/> To help illustrate what going on, lets try to give an exmample: If we
 * denote the AST of a Expression as follows:
 * [AST-Node-Type,Left-Node,Right-Node], then A expression like: "3*5+6" would
 * result in "[*,3,[+,5,6]]" <p/> If the [*,3,[+,5,6]] expression is added to
 * the MultiExpressionEvaluator, it would really be converted to:
 * [c0,[*,3,[c1,[+,5,6]]]] where c0 and c1 represent the CacheExpression
 * expression objects that cache the results of the * and the + operation.
 * Constants and Property nodes are not cached. <p/> If later on we add the
 * following expression [=,11,[+,5,6]] ("11=5+6") to the
 * MultiExpressionEvaluator it would be converted to: [c2,[=,11,[c1,[+,5,6]]]],
 * where c2 is a new CacheExpression object but c1 is the same CacheExpression
 * used in the previous expression. <p/> When the expressions are evaluated, the
 * c1 CacheExpression object will only evaluate the [+,5,6] expression once and
 * cache the resulting value. Hence evauating the second expression costs less
 * because that [+,5,6] is not done 2 times. <p/> Problems: - cacheing the
 * values introduces overhead. It may be possible to be smarter about WHICH
 * nodes in the AST are cached and which are not. - Current implementation is
 * not thread safe. This is because you need a way to invalidate all the cached
 * values so that the next evaluation re-evaluates the nodes. By going single
 * threaded, chache invalidation is done quickly by incrementing a 'view'
 * counter. When a CacheExpressionnotices it's last cached value was generated
 * in an old 'view', it invalidates its cached value.
 * 
 * @version $Revision: 1.2 $ $Date: 2005/08/27 03:52:36 $
 */
public class MultiExpressionEvaluator {

    HashMap rootExpressions = new HashMap();
    HashMap cachedExpressions = new HashMap();

    int view;

    /**
     * A UnaryExpression that caches the result of the nested expression. The
     * cached value is valid if the
     * CacheExpression.cview==MultiExpressionEvaluator.view
     */
    public class CacheExpression extends UnaryExpression {
        short refCount;
        int cview = view - 1;
        Object cachedValue;
        int cachedHashCode;

        public CacheExpression(Expression realExpression) {
            super(realExpression);
            cachedHashCode = realExpression.hashCode();
        }

        /**
         * @see org.apache.activemq.filter.Expression#evaluate(MessageEvaluationContext)
         */
        public Object evaluate(MessageEvaluationContext message) throws JMSException {
            if (view == cview) {
                return cachedValue;
            }
            cachedValue = right.evaluate(message);
            cview = view;
            return cachedValue;
        }

        public int hashCode() {
            return cachedHashCode;
        }

        public boolean equals(Object o) {
            if (o == null)
                return false;
            return ((CacheExpression)o).right.equals(right);
        }

        public String getExpressionSymbol() {
            return null;
        }

        public String toString() {
            return right.toString();
        }

    }

    /**
     * Multiple listeners my be interested in the results of a single
     * expression.
     */
    static class ExpressionListenerSet {
        Expression expression;
        ArrayList listeners = new ArrayList();
    }

    /**
     * Objects that are interested in the results of an expression should
     * implement this interface.
     */
    static interface ExpressionListener {
        void evaluateResultEvent(Expression selector, MessageEvaluationContext message, Object result);
    }

    /**
     * Adds an ExpressionListener to a given expression. When evaluate is
     * called, the ExpressionListener will be provided the results of the
     * Expression applied to the evaluated message.
     */
    public void addExpressionListner(Expression selector, ExpressionListener c) {
        ExpressionListenerSet data = (ExpressionListenerSet)rootExpressions.get(selector.toString());
        if (data == null) {
            data = new ExpressionListenerSet();
            data.expression = addToCache(selector);
            rootExpressions.put(selector.toString(), data);
        }
        data.listeners.add(c);
    }

    /**
     * Removes an ExpressionListener from receiving the results of a given
     * evaluation.
     */
    public boolean removeEventListner(String selector, ExpressionListener c) {
        String expKey = selector;
        ExpressionListenerSet d = (ExpressionListenerSet)rootExpressions.get(expKey);
        if (d == null) // that selector had not been added.
        {
            return false;
        }
        if (!d.listeners.remove(c)) // that selector did not have that listner..
        {
            return false;
        }

        // If there are no more listners for this expression....
        if (d.listeners.size() == 0) {
            // Uncache it...
            removeFromCache((CacheExpression)d.expression);
            rootExpressions.remove(expKey);
        }
        return true;
    }

    /**
     * Finds the CacheExpression that has been associated with an expression. If
     * it is the first time the Expression is being added to the Cache, a new
     * CacheExpression is created and associated with the expression. <p/> This
     * method updates the reference counters on the CacheExpression to know when
     * it is no longer needed.
     */
    private CacheExpression addToCache(Expression expr) {

        CacheExpression n = (CacheExpression)cachedExpressions.get(expr);
        if (n == null) {
            n = new CacheExpression(expr);
            cachedExpressions.put(expr, n);
            if (expr instanceof UnaryExpression) {

                // Cache the sub expressions too
                UnaryExpression un = (UnaryExpression)expr;
                un.setRight(addToCache(un.getRight()));

            } else if (expr instanceof BinaryExpression) {

                // Cache the sub expressions too.
                BinaryExpression bn = (BinaryExpression)expr;
                bn.setRight(addToCache(bn.getRight()));
                bn.setLeft(addToCache(bn.getLeft()));

            }
        }
        n.refCount++;
        return n;
    }

    /**
     * Removes an expression from the cache. Updates the reference counters on
     * the CacheExpression object. When the refernce counter goes to zero, the
     * entry int the Expression to CacheExpression map is removed.
     * 
     * @param cn
     */
    private void removeFromCache(CacheExpression cn) {
        cn.refCount--;
        Expression realExpr = cn.getRight();
        if (cn.refCount == 0) {
            cachedExpressions.remove(realExpr);
        }
        if (realExpr instanceof UnaryExpression) {
            UnaryExpression un = (UnaryExpression)realExpr;
            removeFromCache((CacheExpression)un.getRight());
        }
        if (realExpr instanceof BinaryExpression) {
            BinaryExpression bn = (BinaryExpression)realExpr;
            removeFromCache((CacheExpression)bn.getRight());
        }
    }

    /**
     * Evaluates the message against all the Expressions added to this object.
     * The added ExpressionListeners are notified of the result of the
     * evaluation.
     * 
     * @param message
     */
    public void evaluate(MessageEvaluationContext message) {
        Collection expressionListeners = rootExpressions.values();
        for (Iterator iter = expressionListeners.iterator(); iter.hasNext();) {
            ExpressionListenerSet els = (ExpressionListenerSet)iter.next();
            try {
                Object result = els.expression.evaluate(message);
                for (Iterator iterator = els.listeners.iterator(); iterator.hasNext();) {
                    ExpressionListener l = (ExpressionListener)iterator.next();
                    l.evaluateResultEvent(els.expression, message, result);
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }
}
