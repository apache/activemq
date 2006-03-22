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
