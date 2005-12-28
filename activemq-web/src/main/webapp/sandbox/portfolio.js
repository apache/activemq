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

