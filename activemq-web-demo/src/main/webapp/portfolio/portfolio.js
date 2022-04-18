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

var priceHandler = 
{
  _price: function(message) 
  {
    if (message != null) {
		
      var price = parseFloat(message.getAttribute('bid'))
      var symbol = message.getAttribute('stock')
      var movement = message.getAttribute('movement')
      if (movement == null) {
	        movement = 'up'
      }
	    
      var row = document.getElementById(symbol)
      if (row) {
		    // perform portfolio calculations
		    var value = asFloat(find(row, 'amount')) * price
		    var pl = value - asFloat(find(row, 'cost'))
		    
		    // now lets update the HTML DOM
		    find(row, 'price').innerHTML = fixedDigits(price, 2)
		    find(row, 'value').innerHTML = fixedDigits(value, 2)
		    find(row, 'pl').innerHTML    = fixedDigits(pl, 2)
		    find(row, 'price').className = movement
		    find(row, 'pl').className    = pl >= 0 ? 'up' : 'down'
      }
    }
  }
};


function portfolioPoll(first)
{
   if (first)
   {
     amq.addListener('stocks','topic://STOCKS.*',priceHandler._price);
   }
}

amq.addPollHandler(portfolioPoll);


/// -----------------
// Original code by Joe Walnes
// -----------------

/*** Convenience methods, added as mixins to standard classes (object prototypes) ***/

/**
 * Return number as fixed number of digits. 
 */
function fixedDigits(t, digits) {
    return (t.toFixed) ? t.toFixed(digits) : this
}

/** 
 * Find direct child of an element, by id. 
 */
function find(t, id) {
    for (let i = 0; i < t.childNodes.length; i++) {
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
function asFloat(t) {
    return parseFloat(t.innerHTML)
}
