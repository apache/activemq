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

// updates the portfolio row for a given message and symbol
function updatePortfolioRow(message, destination) {

    var priceMessage = message.documentElement

    var price = parseFloat(priceMessage.getAttribute('bid'))
    var symbol = priceMessage.getAttribute('stock')
    var movement = priceMessage.getAttribute('movement')
    if (movement == null) {
        movement = 'up'
    }

    var row = document.getElementById(symbol)
    
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

var connection = new Connection("jms/STOCKS/*")

function subscribe() {
    connection.addMessageListener(/^STOCKS\..*$/, updatePortfolioRow)
}

