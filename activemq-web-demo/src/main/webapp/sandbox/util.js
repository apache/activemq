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

/// -----------------
// Original code by Joe Walnes
// -----------------

/*** Convenience methods, added as mixins to standard classes (object prototypes) ***/

/**
 * Return number as fixed number of digits. 
 */
//Number.prototype.fixedDigits = function(digits) {
function fixedDigits(t, digits) {
    return (t.toFixed) ? t.toFixed(digits) : this
}

/** 
 * Find direct child of an element, by id. 
 */
// Element.prototype.find = function(id) {
function find(t, id) {
    for (i = 0; i < t.childNodes.length; i++) {
        var child = t.childNodes[i]
        if (child.id == id) {
            return child
        }
    }
    return null
}

/**
 * Return the text contents of an element as a floating point number. 
 */
//Element.prototype.asFloat = function() {
function asFloat(t) {
    return parseFloat(t.innerHTML)
}
