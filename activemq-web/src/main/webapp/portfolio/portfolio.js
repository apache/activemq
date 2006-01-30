


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

function portfolioInit()
{
  amq.addPollHandler(portfolioPoll);
}

Behaviour.addLoadEvent(portfolioInit);  

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
function asFloat(t) {
    return parseFloat(t.innerHTML)
}
