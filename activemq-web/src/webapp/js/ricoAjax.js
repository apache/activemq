/**
  *
  *  Copyright 2005 Sabre Airline Solutions
  *
  *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
  *  file except in compliance with the License. You may obtain a copy of the License at
  *
  *         http://www.apache.org/licenses/LICENSE-2.0
  *
  *  Unless required by applicable law or agreed to in writing, software distributed under the
  *  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
  *  either express or implied. See the License for the specific language governing permissions
  *  and limitations under the License.
  **/


//-------------------- rico.js
var Rico = {
  Version: '1.1-beta2'
}

Rico.ArrayExtensions = new Array();

if (Object.prototype.extend) {
   // in prototype.js...
   Rico.ArrayExtensions[ Rico.ArrayExtensions.length ] = Object.prototype.extend;
}

if (Array.prototype.push) {
   // in prototype.js...
   Rico.ArrayExtensions[ Rico.ArrayExtensions.length ] = Array.prototype.push;
}

if (!Array.prototype.remove) {
   Array.prototype.remove = function(dx) {
      if( isNaN(dx) || dx > this.length )
         return false;
      for( var i=0,n=0; i<this.length; i++ )
         if( i != dx )
            this[n++]=this[i];
      this.length-=1;
   };
  Rico.ArrayExtensions[ Rico.ArrayExtensions.length ] = Array.prototype.remove;
}

if (!Array.prototype.removeItem) {
   Array.prototype.removeItem = function(item) {
      for ( var i = 0 ; i < this.length ; i++ )
         if ( this[i] == item ) {
            this.remove(i);
            break;
         }
   };
  Rico.ArrayExtensions[ Rico.ArrayExtensions.length ] = Array.prototype.removeItem;
}

if (!Array.prototype.indices) {
   Array.prototype.indices = function() {
      var indexArray = new Array();
      for ( index in this ) {
         var ignoreThis = false;
         for ( var i = 0 ; i < Rico.ArrayExtensions.length ; i++ ) {
            if ( this[index] == Rico.ArrayExtensions[i] ) {
               ignoreThis = true;
               break;
            }
         }
         if ( !ignoreThis )
            indexArray[ indexArray.length ] = index;
      }
      return indexArray;
   }
  Rico.ArrayExtensions[ Rico.ArrayExtensions.length ] = Array.prototype.indices;
}

// Create the loadXML method and xml getter for Mozilla
if ( window.DOMParser &&
	  window.XMLSerializer &&
	  window.Node && Node.prototype && Node.prototype.__defineGetter__ ) {

   if (!Document.prototype.loadXML) {
      Document.prototype.loadXML = function (s) {
         var doc2 = (new DOMParser()).parseFromString(s, "text/xml");
         while (this.hasChildNodes())
            this.removeChild(this.lastChild);

         for (var i = 0; i < doc2.childNodes.length; i++) {
            this.appendChild(this.importNode(doc2.childNodes[i], true));
         }
      };
	}

	Document.prototype.__defineGetter__( "xml",
	   function () {
		   return (new XMLSerializer()).serializeToString(this);
	   }
	 );
}

document.getElementsByTagAndClassName = function(tagName, className) {
  if ( tagName == null )
     tagName = '*';

  var children = document.getElementsByTagName(tagName) || document.all;
  var elements = new Array();

  if ( className == null )
    return children;

  for (var i = 0; i < children.length; i++) {
    var child = children[i];
    var classNames = child.className.split(' ');
    for (var j = 0; j < classNames.length; j++) {
      if (classNames[j] == className) {
        elements.push(child);
        break;
      }
    }
  }

  return elements;
}


//-------------------- ricoAjaxEngine.js

Rico.AjaxEngine = Class.create();

Rico.AjaxEngine.prototype = {

   initialize: function() {
      this.ajaxElements = new Array();
      this.ajaxObjects  = new Array();
      this.requestURLS  = new Array();
   },

   registerAjaxElement: function( anId, anElement ) {
      if ( arguments.length == 1 )
         anElement = $(anId);
      this.ajaxElements[anId] = anElement;
   },

   registerAjaxObject: function( anId, anObject ) {
      this.ajaxObjects[anId] = anObject;
   },

   registerRequest: function (requestLogicalName, requestURL) {
      this.requestURLS[requestLogicalName] = requestURL;
   },

   sendRequest: function(requestName) {
      var requestURL = this.requestURLS[requestName];
      if ( requestURL == null )
         return;

      var queryString = "";
      if ( arguments.length > 1 )
         queryString = this._createQueryString(arguments, 1);

      new Ajax.Request(requestURL, this._requestOptions(queryString));
   },

   sendRequestWithData: function(requestName, xmlDocument) {
      var requestURL = this.requestURLS[requestName];
      if ( requestURL == null )
         return;

      var queryString = "";
      if ( arguments.length > 2 )
         queryString = this._createQueryString(arguments, 2);

      new Ajax.Request(requestURL + (requestURL.indexOf("?")<0?"?":"&") + queryString, this._requestOptions(null,xmlDocument));
   },

   sendRequestAndUpdate: function(requestName,container,options) {
      var requestURL = this.requestURLS[requestName];
      if ( requestURL == null )
         return;

      var queryString = "";
      if ( arguments.length > 3 )
         queryString = this._createQueryString(arguments, 3);

      var updaterOptions = this._requestOptions(queryString);
      updaterOptions.onComplete = null;
      updaterOptions.extend(options);

      new Ajax.Updater(container, requestURL, updaterOptions);
   },

   sendRequestWithDataAndUpdate: function(requestName,xmlDocument,container,options) {
      var requestURL = this.requestURLS[requestName];
      if ( requestURL == null )
         return;

      var queryString = "";
      if ( arguments.length > 4 )
         queryString = this._createQueryString(arguments, 4);


      var updaterOptions = this._requestOptions(queryString,xmlDocument);
      updaterOptions.onComplete = null;
      updaterOptions.extend(options);

      new Ajax.Updater(container, requestURL + (requestURL.indexOf("?")<0?"?":"&") + queryString, updaterOptions);
   },

   // Private -- not part of intended engine API --------------------------------------------------------------------

   _requestOptions: function(queryString,xmlDoc) {
      var self = this;

      var requestHeaders = ['X-Rico-Version', Rico.Version ];
      var sendMethod = "post"
      if ( arguments[1] )
         requestHeaders.push( 'Content-type', 'text/xml' );
      else
         sendMethod = "get";

      return { requestHeaders: requestHeaders,
               parameters:     queryString,
               postBody:       arguments[1] ? xmlDoc : null,
               method:         sendMethod,
               onComplete:     self._onRequestComplete.bind(self) };
   },

   _createQueryString: function( theArgs, offset ) {
      var queryString = ""
      for ( var i = offset ; i < theArgs.length ; i++ ) {
          if ( i != offset )
            queryString += "&";

          var anArg = theArgs[i];

          if ( anArg.name != undefined && anArg.value != undefined ) {
            queryString += anArg.name +  "=" + escape(anArg.value);
          }
          else {
             var ePos  = anArg.indexOf('=');
             var argName  = anArg.substring( 0, ePos );
             var argValue = anArg.substring( ePos + 1 );
             queryString += argName + "=" + escape(argValue);
          }
      }

      return queryString;
   },

   _onRequestComplete : function(request) {

      //!!TODO: error handling infrastructure?? 
      if (request.status != 200)
        return;
      
      var response = request.responseXML.getElementsByTagName("ajax-response");
            
      if (response == null || response.length != 1)
         return;
      this._processAjaxResponse( response[0].childNodes );
   },

   _processAjaxResponse: function( xmlResponseElements ) {
      for ( var i = 0 ; i < xmlResponseElements.length ; i++ ) {
         var responseElement = xmlResponseElements[i];

         // only process nodes of type element.....
         if ( responseElement.nodeType != 1 )
            continue;

         var responseType = responseElement.getAttribute("type");
         var responseId   = responseElement.getAttribute("id");

         if ( responseType == "object" )
            this._processAjaxObjectUpdate( this.ajaxObjects[ responseId ], responseElement );
         else if ( responseType == "element" )
            this._processAjaxElementUpdate( this.ajaxElements[ responseId ], responseElement );
         else
            alert('unrecognized AjaxResponse type : ' + responseType );
      }
   },

   _processAjaxObjectUpdate: function( ajaxObject, responseElement ) {
      ajaxObject.ajaxUpdate( responseElement );
   },

   _processAjaxElementUpdate: function( ajaxElement, responseElement ) {
   	  if (ajaxElement) {
      	ajaxElement.innerHTML = RicoUtil.getContentAsString(responseElement);
  	  }
   }

}

var ajaxEngine = new Rico.AjaxEngine();


//-------------------- ricoUtil.js

var RicoUtil = {

   getElementsComputedStyle: function ( htmlElement, cssProperty, mozillaEquivalentCSS) {
      if ( arguments.length == 2 )
         mozillaEquivalentCSS = cssProperty;

      var el = $(htmlElement);
      if ( el.currentStyle )
         return el.currentStyle[cssProperty];
      else
         return document.defaultView.getComputedStyle(el, null).getPropertyValue(mozillaEquivalentCSS);
   },

   createXmlDocument : function() {
      if (document.implementation && document.implementation.createDocument) {
         var doc = document.implementation.createDocument("", "", null);

         if (doc.readyState == null) {
            doc.readyState = 1;
            doc.addEventListener("load", function () {
               doc.readyState = 4;
               if (typeof doc.onreadystatechange == "function")
                  doc.onreadystatechange();
            }, false);
         }

         return doc;
      }

      if (window.ActiveXObject)
          return Try.these(
            function() { return new ActiveXObject('MSXML2.DomDocument')   },
            function() { return new ActiveXObject('Microsoft.DomDocument')},
            function() { return new ActiveXObject('MSXML.DomDocument')    },
            function() { return new ActiveXObject('MSXML3.DomDocument')   }
          ) || false;

      return null;
   },

   getContentAsString: function( parentNode ) {
      return parentNode.xml != undefined ? 
         this._getContentAsStringIE(parentNode) :
         this._getContentAsStringMozilla(parentNode);
   },

   _getContentAsStringIE: function(parentNode) {
      var contentStr = "";
      for ( var i = 0 ; i < parentNode.childNodes.length ; i++ )
         contentStr += parentNode.childNodes[i].xml;
      return contentStr;
   },

   _getContentAsStringMozilla: function(parentNode) {
      var xmlSerializer = new XMLSerializer();
      var contentStr = "";
      for ( var i = 0 ; i < parentNode.childNodes.length ; i++ )
         contentStr += xmlSerializer.serializeToString(parentNode.childNodes[i]);
      return contentStr;
   },

   toViewportPosition: function(element) {
      return this._toAbsolute(element,true);
   },

   toDocumentPosition: function(element) {
      return this._toAbsolute(element,false);
   },

   /**
    *  Compute the elements position in terms of the window viewport
    *  so that it can be compared to the position of the mouse (dnd)
    *  This is additions of all the offsetTop,offsetLeft values up the
    *  offsetParent hierarchy, ...taking into account any scrollTop,
    *  scrollLeft values along the way...
    *
    * IE has a bug reporting a correct offsetLeft of elements within a
    * a relatively positioned parent!!!
    **/
   _toAbsolute: function(element,accountForDocScroll) {

      if ( navigator.userAgent.toLowerCase().indexOf("msie") == -1 )
         return this._toAbsoluteMozilla(element,accountForDocScroll);

      var x = 0;
      var y = 0;
      var parent = element;
      while ( parent ) {

         var borderXOffset = 0;
         var borderYOffset = 0;
         if ( parent != element ) {
            var borderXOffset = parseInt(this.getElementsComputedStyle(parent, "borderLeftWidth" ));
            var borderYOffset = parseInt(this.getElementsComputedStyle(parent, "borderTopWidth" ));
            borderXOffset = isNaN(borderXOffset) ? 0 : borderXOffset;
            borderYOffset = isNaN(borderYOffset) ? 0 : borderYOffset;
         }

         x += parent.offsetLeft - parent.scrollLeft + borderXOffset;
         y += parent.offsetTop - parent.scrollTop + borderYOffset;
         parent = parent.offsetParent;
      }

      if ( accountForDocScroll ) {
         x -= this.docScrollLeft();
         y -= this.docScrollTop();
      }

      return { x:x, y:y };
   },

   /**
    *  Mozilla did not report all of the parents up the hierarchy via the
    *  offsetParent property that IE did.  So for the calculation of the
    *  offsets we use the offsetParent property, but for the calculation of
    *  the scrollTop/scrollLeft adjustments we navigate up via the parentNode
    *  property instead so as to get the scroll offsets...
    *
    **/
   _toAbsoluteMozilla: function(element,accountForDocScroll) {
      var x = 0;
      var y = 0;
      var parent = element;
      while ( parent ) {
         x += parent.offsetLeft;
         y += parent.offsetTop;
         parent = parent.offsetParent;
      }

      parent = element;
      while ( parent &&
              parent != document.body &&
              parent != document.documentElement ) {
         if ( parent.scrollLeft  )
            x -= parent.scrollLeft;
         if ( parent.scrollTop )
            y -= parent.scrollTop;
         parent = parent.parentNode;
      }

      if ( accountForDocScroll ) {
         x -= this.docScrollLeft();
         y -= this.docScrollTop();
      }

      return { x:x, y:y };
   },

   docScrollLeft: function() {
      if ( window.pageXOffset )
         return window.pageXOffset;
      else if ( document.documentElement && document.documentElement.scrollLeft )
         return document.documentElement.scrollLeft;
      else if ( document.body )
         return document.body.scrollLeft;
      else
         return 0;
   },

   docScrollTop: function() {
      if ( window.pageYOffset )
         return window.pageYOffset;
      else if ( document.documentElement && document.documentElement.scrollTop )
         return document.documentElement.scrollTop;
      else if ( document.body )
         return document.body.scrollTop;
      else
         return 0;
   }

};
