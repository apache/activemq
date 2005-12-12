
var PollHandler = 
{
  ajaxUpdate: function(ajaxResponse) 
  {
     // Poll again for events
     ajaxEngine.sendRequest('getEvents');
     
  }
};

var PriceHandler = 
{
  ajaxUpdate: function(ajaxResponse) 
  {
    var priceMessage = firstElement(ajaxResponse)
	if (priceMessage != null) {
		
	    var price = parseFloat(priceMessage.getAttribute('bid'))
	    var symbol = priceMessage.getAttribute('stock')
	    var movement = priceMessage.getAttribute('movement')
	    if (movement == null) {
	        movement = 'up'
	    }
	    
	  	//alert('Received price ' + priceMessage + ' for price: ' + price + ' symbol: ' + symbol + ' movement: ' + movement)
	
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

function initPage()
{
  ajaxEngine.registerRequest('getEvents', '/jms/STOCKS/*?rico=true&id=priceChange'); 
  
  ajaxEngine.registerAjaxObject('poll', PollHandler);

  ajaxEngine.registerAjaxObject('priceChange', PriceHandler);
  
  ajaxEngine.sendRequest('getEvents');
}

Behaviour.addLoadEvent(initPage);  

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

function firstElement(t) {
    for (i = 0; i < t.childNodes.length; i++) {
        var child = t.childNodes[i]
        if (child.nodeType == 1) {
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
