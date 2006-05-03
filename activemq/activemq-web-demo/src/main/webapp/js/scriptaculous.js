// Copyright (c) 2005 Thomas Fuchs (http://script.aculo.us, http://mir.aculo.us)
// 
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
// 
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


Element.collectTextNodesIgnoreClass = function(element, ignoreclass) {
  var children = $(element).childNodes;
  var text     = "";
  var classtest = new RegExp("^([^ ]+ )*" + ignoreclass+ "( [^ ]+)*$","i");
  
  for (var i = 0; i < children.length; i++) {
    if(children[i].nodeType==3) {
      text+=children[i].nodeValue;
    } else {
      if((!children[i].className.match(classtest)) && children[i].hasChildNodes())
        text += Element.collectTextNodesIgnoreClass(children[i], ignoreclass);
    }
  }
  
  return text;
}

Ajax.Autocompleter = Class.create();
Ajax.Autocompleter.prototype = (new Ajax.Base()).extend({
  initialize: function(element, update, url, options) {
    this.element     = $(element); 
    this.update      = $(update);  
    this.has_focus   = false; 
    this.changed     = false; 
    this.active      = false; 
    this.index       = 0;     
    this.entry_count = 0;    
    this.url         = url;

    this.setOptions(options);
    this.options.asynchronous = true;
    this.options.onComplete   = this.onComplete.bind(this)
    this.options.frequency    = this.options.frequency || 0.4;
    this.options.min_chars    = this.options.min_chars || 1;
    this.options.method       = 'post';
    
    this.options.onShow = this.options.onShow || 
      function(element, update){ 
        if(!update.style.position || update.style.position=='absolute') {
          update.style.position = 'absolute';
          var offsets = Position.cumulativeOffset(element);
          update.style.left = offsets[0] + 'px';
          update.style.top  = (offsets[1] + element.offsetHeight) + 'px';
          update.style.width = element.offsetWidth + 'px';
        }
        new Effect.Appear(update,{duration:0.3});
      };
    this.options.onHide = this.options.onHide || 
      function(element, update){ new Effect.Fade(update,{duration:0.3}) };
    
    
    if(this.options.indicator)
      this.indicator = $(this.options.indicator);
       
    this.observer = null;
    
    Element.hide(this.update);
    
    Event.observe(this.element, "blur", this.onBlur.bindAsEventListener(this));
    Event.observe(this.element, "keypress", this.onKeyPress.bindAsEventListener(this));
  },
  
  show: function() {
    if(this.update.style.display=='none') this.options.onShow(this.element, this.update);
    if(!this.iefix && (navigator.appVersion.indexOf('MSIE')>0) && this.update.style.position=='absolute') {
      new Insertion.After(this.update, 
       '<iframe id="' + this.update.id + '_iefix" '+
       'style="display:none;filter:progid:DXImageTransform.Microsoft.Alpha(apacity=0);" ' +
       'src="javascript:;" frameborder="0" scrolling="no"></iframe>');
      this.iefix = $(this.update.id+'_iefix');
    }
    if(this.iefix) {
      Position.clone(this.update, this.iefix);
      this.iefix.style.zIndex = 1;
      this.update.style.zIndex = 2;
      Element.show(this.iefix);
    }
  },
  
  hide: function() {
    if(this.update.style.display=='') this.options.onHide(this.element, this.update);
    if(this.iefix) Element.hide(this.iefix);
  },
  
  startIndicator: function() {
    if(this.indicator) Element.show(this.indicator);
  },
  
  stopIndicator: function() {
    if(this.indicator) Element.hide(this.indicator);
  },
  
  onObserverEvent: function() {
    this.changed = false;   
    if(this.element.value.length>=this.options.min_chars) {
      this.startIndicator();
      this.options.parameters = this.options.callback ?
        this.options.callback(this.element, Form.Element.getValue(this.element)) :
          Form.Element.serialize(this.element);
      new Ajax.Request(this.url, this.options);
    } else {
      this.active = false;
      this.hide();
    }
  },
  
  addObservers: function(element) {
    Event.observe(element, "mouseover", this.onHover.bindAsEventListener(this));
    Event.observe(element, "click", this.onClick.bindAsEventListener(this));
  },
  
  onComplete: function(request) {
    if(!this.changed && this.has_focus) {
      this.update.innerHTML = request.responseText;
      Element.cleanWhitespace(this.update);
      Element.cleanWhitespace(this.update.firstChild);

      if(this.update.firstChild && this.update.firstChild.childNodes) {
        this.entry_count = 
          this.update.firstChild.childNodes.length;
        for (var i = 0; i < this.entry_count; i++) {
          entry = this.get_entry(i);
          entry.autocompleteIndex = i;
          this.addObservers(entry);
        }
      } else { 
        this.entry_count = 0;
      }
      
      this.stopIndicator();
      
      this.index = 0;
      this.render();
    }
  },
  
  onKeyPress: function(event) {
    if(this.active)
      switch(event.keyCode) {
       case Event.KEY_TAB:
       case Event.KEY_RETURN:
         this.select_entry();
         Event.stop(event);
       case Event.KEY_ESC:
         this.hide();
         this.active = false;
         return;
       case Event.KEY_LEFT:
       case Event.KEY_RIGHT:
         return;
       case Event.KEY_UP:
         this.mark_previous();
         this.render();
         if(navigator.appVersion.indexOf('AppleWebKit')>0) Event.stop(event);
         return;
       case Event.KEY_DOWN:
         this.mark_next();
         this.render();
         if(navigator.appVersion.indexOf('AppleWebKit')>0) Event.stop(event);
         return;
      }
     else 
      if(event.keyCode==Event.KEY_TAB || event.keyCode==Event.KEY_RETURN) 
        return;
    
    this.changed = true;
    this.has_focus = true;
    
    if(this.observer) clearTimeout(this.observer);
      this.observer = 
        setTimeout(this.onObserverEvent.bind(this), this.options.frequency*1000);
  },
  
  onHover: function(event) {
    var element = Event.findElement(event, 'LI');
    if(this.index != element.autocompleteIndex) 
    {
        this.index = element.autocompleteIndex;
        this.render();
    }
    Event.stop(event);
  },
  
  onClick: function(event) {
    var element = Event.findElement(event, 'LI');
    this.index = element.autocompleteIndex;
    this.select_entry();
    Event.stop(event);
  },
  
  onBlur: function(event) {
    // needed to make click events working
    setTimeout(this.hide.bind(this), 250);
    this.has_focus = false;
    this.active = false;     
  }, 
  
  render: function() {
    if(this.entry_count > 0) {
      for (var i = 0; i < this.entry_count; i++)
        this.index==i ? 
          Element.addClassName(this.get_entry(i),"selected") : 
          Element.removeClassName(this.get_entry(i),"selected");
        
      if(this.has_focus) { 
        if(this.get_current_entry().scrollIntoView) 
          this.get_current_entry().scrollIntoView(false);
        
        this.show();
        this.active = true;
      }
    } else this.hide();
  },
  
  mark_previous: function() {
    if(this.index > 0) this.index--
      else this.index = this.entry_count-1;
  },
  
  mark_next: function() {
    if(this.index < this.entry_count-1) this.index++
      else this.index = 0;
  },
  
  get_entry: function(index) {
    return this.update.firstChild.childNodes[index];
  },
  
  get_current_entry: function() {
    return this.get_entry(this.index);
  },
  
  select_entry: function() {
    this.active = false;
    value = Element.collectTextNodesIgnoreClass(this.get_current_entry(), 'informal').unescapeHTML();
    this.element.value = value;
    this.element.focus();
  }
});// Copyright (c) 2005 Thomas Fuchs (http://script.aculo.us, http://mir.aculo.us)
// 
// Element.Class part Copyright (c) 2005 by Rick Olson
// 
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
// 
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Element.Class = {
    // Element.toggleClass(element, className) toggles the class being on/off
    // Element.toggleClass(element, className1, className2) toggles between both classes,
    //   defaulting to className1 if neither exist
    toggle: function(element, className) {
      if(Element.Class.has(element, className)) {
        Element.Class.remove(element, className);
        if(arguments.length == 3) Element.Class.add(element, arguments[2]);
      } else {
        Element.Class.add(element, className);
        if(arguments.length == 3) Element.Class.remove(element, arguments[2]);
      }
    },

    // gets space-delimited classnames of an element as an array
    get: function(element) {
      element = $(element);
      return element.className.split(' ');
    },

    // functions adapted from original functions by Gavin Kistner
    remove: function(element) {
      element = $(element);
      var regEx;
      for(var i = 1; i < arguments.length; i++) {
        regEx = new RegExp("^" + arguments[i] + "\\b\\s*|\\s*\\b" + arguments[i] + "\\b", 'g');
        element.className = element.className.replace(regEx, '')
      }
    },

    add: function(element) {
      element = $(element);
      for(var i = 1; i < arguments.length; i++) {
        Element.Class.remove(element, arguments[i]);
        element.className += (element.className.length > 0 ? ' ' : '') + arguments[i];
      }
    },

    // returns true if all given classes exist in said element
    has: function(element) {
      element = $(element);
      if(!element || !element.className) return false;
      var regEx;
      for(var i = 1; i < arguments.length; i++) {
        regEx = new RegExp("\\b" + arguments[i] + "\\b");
        if(!regEx.test(element.className)) return false;
      }
      return true;
    },
    
    // expects arrays of strings and/or strings as optional paramters
    // Element.Class.has_any(element, ['classA','classB','classC'], 'classD')
    has_any: function(element) {
      element = $(element);
      if(!element || !element.className) return false;
      var regEx;
      for(var i = 1; i < arguments.length; i++) {
        if((typeof arguments[i] == 'object') && 
          (arguments[i].constructor == Array)) {
          for(var j = 0; j < arguments[i].length; j++) {
            regEx = new RegExp("\\b" + arguments[i][j] + "\\b");
            if(regEx.test(element.className)) return true;
          }
        } else {
          regEx = new RegExp("\\b" + arguments[i] + "\\b");
          if(regEx.test(element.className)) return true;
        }
      }
      return false;
    },
    
    childrenWith: function(element, className) {
      var children = $(element).getElementsByTagName('*');
      var elements = new Array();
      
      for (var i = 0; i < children.length; i++) {
        if (Element.Class.has(children[i], className)) {
          elements.push(children[i]);
          break;
        }
      }
      
      return elements;
    }
}

/*--------------------------------------------------------------------------*/

var Droppables = {
  drops: false,
  
  add: function(element) {
    var element = $(element);
    var options = {
      greedy:     true,
      hoverclass: null  
    }.extend(arguments[1] || {});
    
    // cache containers
    if(options.containment) {
      options._containers = new Array();
      var containment = options.containment;
      if((typeof containment == 'object') && 
        (containment.constructor == Array)) {
        for(var i=0; i<containment.length; i++)
          options._containers.push($(containment[i]));
      } else {
        options._containers.push($(containment));
      }
      options._containers_length = 
        options._containers.length-1;
    }
    
    if(element.style.position=='') //fix IE
      element.style.position = 'relative'; 
    
    // activate the droppable
    element.droppable = options;
    
    if(!this.drops) this.drops = [];
    this.drops.push(element);
  },
  
  is_contained: function(element, drop) {
    var containers = drop.droppable._containers;
    var parentNode = element.parentNode;
    var i = drop.droppable._containers_length;
    do { if(parentNode==containers[i]) return true; } while (i--);
    return false;
  },
  
  is_affected: function(pX, pY, element, drop) {
    return (
      (drop!=element) &&
      ((!drop.droppable._containers) ||
        this.is_contained(element, drop)) &&
      ((!drop.droppable.accept) ||
        (Element.Class.has_any(element, drop.droppable.accept))) &&
      Position.within(drop, pX, pY) );
  },
  
  deactivate: function(drop) {
    Element.Class.remove(drop, drop.droppable.hoverclass);
    this.last_active = null;
  },
  
  activate: function(drop) {
    if(this.last_active) this.deactivate(this.last_active);
    if(drop.droppable.hoverclass) {
      Element.Class.add(drop, drop.droppable.hoverclass);
      this.last_active = drop;
    }
  },
  
  show: function(event, element) {
    if(!this.drops) return;
    var pX = Event.pointerX(event);
    var pY = Event.pointerY(event);
    Position.prepare();
    
    var i = this.drops.length-1; do {
      var drop = this.drops[i];
      if(this.is_affected(pX, pY, element, drop)) {
        if(drop.droppable.onHover)
           drop.droppable.onHover(
            element, drop, Position.overlap(drop.droppable.overlap, drop));
        if(drop.droppable.greedy) { 
          this.activate(drop);
          return;
        }
      }
    } while (i--);
  },
  
  fire: function(event, element) {
    if(!this.drops) return;
    var pX = Event.pointerX(event);
    var pY = Event.pointerY(event);
    Position.prepare();
    
    var i = this.drops.length-1; do {
      var drop = this.drops[i];
      if(this.is_affected(pX, pY, element, drop))
        if(drop.droppable.onDrop)
           drop.droppable.onDrop(element);
    } while (i--);
  },
  
  reset: function() {
    if(this.last_active)
      this.deactivate(this.last_active);
  }
}

Draggables = {
  observers: new Array(),
  addObserver: function(observer) {
    this.observers.push(observer);    
  },
  notify: function(eventName, draggable) {  // 'onStart', 'onEnd'
    for(var i = 0; i < this.observers.length; i++)
      this.observers[i][eventName](draggable);
  }
}

/*--------------------------------------------------------------------------*/

Draggable = Class.create();
Draggable.prototype = {
  initialize: function(element) {
    var options = {
      handle: false,
      starteffect: function(element) { 
        new Effect.Opacity(element, {duration:0.2, from:1.0, to:0.7}); 
      },
      reverteffect: function(element, top_offset, left_offset) {
        new Effect.MoveBy(element, -top_offset, -left_offset, {duration:0.4});
      },
      endeffect: function(element) { 
         new Effect.Opacity(element, {duration:0.2, from:0.7, to:1.0}); 
      },
      zindex: 1000,
      revert: false
    }.extend(arguments[1] || {});
    
    this.element      = $(element);
    this.element.drag = this;
    this.handle       = options.handle ? $(options.handle) : this.element;
    
    // fix IE
    if(!this.element.style.position)
      this.element.style.position = 'relative';
    
    this.offsetX      = 0;
    this.offsetY      = 0;
    this.originalLeft = this.currentLeft();
    this.originalTop  = this.currentTop();
    this.originalX    = this.element.offsetLeft;
    this.originalY    = this.element.offsetTop;
    this.originalZ    = parseInt(this.element.style.zIndex || "0");
    
    this.options      = options;
    
    this.active       = false;
    this.dragging     = false;   
    
    Event.observe(this.handle, "mousedown", this.startDrag.bindAsEventListener(this));
    Event.observe(document, "mouseup", this.endDrag.bindAsEventListener(this));
    Event.observe(document, "mousemove", this.update.bindAsEventListener(this));
  },
  currentLeft: function() {
    return parseInt(this.element.style.left || '0');
  },
  currentTop: function() {
    return parseInt(this.element.style.top || '0')
  },
  startDrag: function(event) {
    if(Event.isLeftClick(event)) {
      this.active = true;
      
      var style = this.element.style;
      this.originalY = this.element.offsetTop  - this.currentTop()  - this.originalTop;
      this.originalX = this.element.offsetLeft - this.currentLeft() - this.originalLeft;
      this.offsetY =  event.clientY - this.originalY - this.originalTop;
      this.offsetX =  event.clientX - this.originalX - this.originalLeft;
      
      Event.stop(event);
    }
  },
  endDrag: function(event) {
    if(this.active && this.dragging) {
      this.active = false;
      this.dragging = false;
      
      Droppables.fire(event, this.element);
      Draggables.notify('onEnd', this);
      
      var revert = this.options.revert;
      if(revert && typeof revert == 'function') revert = revert(this.element);
      
      if(revert && this.options.reverteffect) {
        this.options.reverteffect(this.element, 
          this.currentTop()-this.originalTop,
          this.currentLeft()-this.originalLeft);
      } else {
        this.originalLeft = this.currentLeft();
        this.originalTop  = this.currentTop();
      }
      this.element.style.zIndex = this.originalZ;
     
      if(this.options.endeffect) 
        this.options.endeffect(this.element);
      
      Droppables.reset();
      Event.stop(event);
    }
    this.active = false;
    this.dragging = false;
  },
  draw: function(event) {
    var style = this.element.style;
    this.originalX = this.element.offsetLeft - this.currentLeft() - this.originalLeft;
    this.originalY = this.element.offsetTop  - this.currentTop()  - this.originalTop;
    if((!this.options.constraint) || (this.options.constraint=='horizontal'))
      style.left = ((event.clientX - this.originalX) - this.offsetX) + "px";
    if((!this.options.constraint) || (this.options.constraint=='vertical'))
      style.top  = ((event.clientY - this.originalY) - this.offsetY) + "px";
    if(style.visibility=="hidden") style.visibility = ""; // fix gecko rendering
  },
  update: function(event) {
   if(this.active) {
      if(!this.dragging) {
        var style = this.element.style;
        this.dragging = true;
        if(style.position=="") style.position = "relative";
        style.zIndex = this.options.zindex;
        Draggables.notify('onStart', this);
        if(this.options.starteffect) this.options.starteffect(this.element);
      }
      
      Droppables.show(event, this.element);
      this.draw(event);
      if(this.options.change) this.options.change(this);
      
      // fix AppleWebKit rendering
      if(navigator.appVersion.indexOf('AppleWebKit')>0) window.scrollBy(0,0); 
      
      Event.stop(event);
   }
  }
}

/*--------------------------------------------------------------------------*/

SortableObserver = Class.create();
SortableObserver.prototype = {
  initialize: function(element, observer) {
    this.element   = $(element);
    this.observer  = observer;
    this.lastValue = Sortable.serialize(this.element);
  },
  onStart: function() {
    this.lastValue = Sortable.serialize(this.element);
  },
  onEnd: function() {    
    if(this.lastValue != Sortable.serialize(this.element))
      this.observer(this.element)
  }
}

Sortable = {
  create: function(element) {
    var element = $(element);
    var options = { 
      tag:         'li',       // assumes li children, override with tag: 'tagname'
      overlap:     'vertical', // one of 'vertical', 'horizontal'
      constraint:  'vertical', // one of 'vertical', 'horizontal', false
      containment: element,    // also takes array of elements (or id's); or false
      handle:      false,      // or a CSS class
      only:        false,
      hoverclass:  null,
      onChange:    function() {},
      onUpdate:    function() {}
    }.extend(arguments[1] || {});
    element.sortable = options;
    
    // build options for the draggables
    var options_for_draggable = {
      revert:      true,
      constraint:  options.constraint,
      handle:      handle };
    if(options.starteffect)
      options_for_draggable.starteffect = options.starteffect;
    if(options.reverteffect)
      options_for_draggable.reverteffect = options.reverteffect;
    if(options.endeffect)
      options_for_draggable.endeffect = options.endeffect;
    if(options.zindex)
      options_for_draggable.zindex = options.zindex;
    
    // build options for the droppables  
    var options_for_droppable = {
      overlap:     options.overlap,
      containment: options.containment,
      hoverclass:  options.hoverclass,
      onHover: function(element, dropon, overlap) { 
        if(overlap>0.5) {
          if(dropon.previousSibling != element) {
            var oldParentNode = element.parentNode;
            element.style.visibility = "hidden"; // fix gecko rendering
            dropon.parentNode.insertBefore(element, dropon);
            if(dropon.parentNode!=oldParentNode && oldParentNode.sortable) 
              oldParentNode.sortable.onChange(element);
            if(dropon.parentNode.sortable)
              dropon.parentNode.sortable.onChange(element);
          }
        } else {                
          var nextElement = dropon.nextSibling || null;
          if(nextElement != element) {
            var oldParentNode = element.parentNode;
            element.style.visibility = "hidden"; // fix gecko rendering
            dropon.parentNode.insertBefore(element, nextElement);
            if(dropon.parentNode!=oldParentNode && oldParentNode.sortable) 
              oldParentNode.sortable.onChange(element);
            if(dropon.parentNode.sortable)
              dropon.parentNode.sortable.onChange(element);
          }
        }
      }
    }

    // fix for gecko engine
    Element.cleanWhitespace(element); 
    
    // for onupdate
    Draggables.addObserver(new SortableObserver(element, options.onUpdate));
    
    // make it so 
    var elements = element.childNodes;
    for (var i = 0; i < elements.length; i++) 
      if(elements[i].tagName && elements[i].tagName==options.tag.toUpperCase() &&
        (!options.only || (Element.Class.has(elements[i], options.only)))) {
        
        // handles are per-draggable
        var handle = options.handle ? 
          Element.Class.childrenWith(elements[i], options.handle)[0] : elements[i];
        
        new Draggable(elements[i], options_for_draggable.extend({ handle: handle }));
        Droppables.add(elements[i], options_for_droppable);
      }
      
  },
  serialize: function(element) {
    var element = $(element);
    var options = {
      tag:  element.sortable.tag,
      only: element.sortable.only,
      name: element.id
    }.extend(arguments[1] || {});
    
    var items = $(element).childNodes;
    var queryComponents = new Array();
 
    for(var i=0; i<items.length; i++)
      if(items[i].tagName && items[i].tagName==options.tag.toUpperCase() &&
        (!options.only || (Element.Class.has(items[i], options.only))))
        queryComponents.push(
          encodeURIComponent(options.name) + "[]=" + 
          encodeURIComponent(items[i].id.split("_")[1]));

    return queryComponents.join("&");
  }
} // Copyright (c) 2005 Thomas Fuchs (http://script.aculo.us, http://mir.aculo.us)
//
// Parts (c) 2005 Justin Palmer (http://encytemedia.com/)
// Parts (c) 2005 Mark Pilgrim (http://diveintomark.org/)
// 
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
// 
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


Effect = {}
Effect2 = Effect; // deprecated

/* ------------- transitions ------------- */

Effect.Transitions = {}

Effect.Transitions.linear = function(pos) {
  return pos;
}
Effect.Transitions.sinoidal = function(pos) {
  return (-Math.cos(pos*Math.PI)/2) + 0.5;
}
Effect.Transitions.reverse  = function(pos) {
  return 1-pos;
}
Effect.Transitions.flicker = function(pos) {
  return ((-Math.cos(pos*Math.PI)/4) + 0.75) + Math.random(0.25);
}
Effect.Transitions.wobble = function(pos) {
  return (-Math.cos(pos*Math.PI*(9*pos))/2) + 0.5;
}
Effect.Transitions.pulse = function(pos) {
   return (Math.floor(pos*10) % 2 == 0 ? 
    (pos*10-Math.floor(pos*10)) : 1-(pos*10-Math.floor(pos*10)));
}

Effect.Transitions.none = function(pos) {
    return 0;
}
Effect.Transitions.full = function(pos) {
    return 1;
}

/* ------------- core effects ------------- */

Effect.Base = function() {};
Effect.Base.prototype = {
  setOptions: function(options) {
    this.options = {
      transition: Effect.Transitions.sinoidal,
      duration:   1.0,   // seconds
      fps:        25.0,  // max. 100fps
      sync:       false, // true for combining
      from:       0.0,
      to:         1.0
    }.extend(options || {});
  },
  start: function(options) {
    this.setOptions(options || {});
    this.currentFrame    = 0;
    this.startOn  = new Date().getTime();
    this.finishOn = this.startOn + (this.options.duration*1000);
    if(this.options.beforeStart) this.options.beforeStart(this);
    if(!this.options.sync) this.loop();  
  },
  loop: function() {
    timePos = new Date().getTime();
    if(timePos >= this.finishOn) {
      this.render(this.options.to);
      if(this.finish) this.finish(); 
      if(this.options.afterFinish) this.options.afterFinish(this);
      return;  
    }
    pos   = (timePos - this.startOn) / (this.finishOn - this.startOn);
    frame = Math.round(pos * this.options.fps * this.options.duration);
    if(frame > this.currentFrame) {
      this.render(pos);
      this.currentFrame = frame;
    }
    this.timeout = setTimeout(this.loop.bind(this), 10);
  },
  render: function(pos) {
    if(this.options.transition) pos = this.options.transition(pos);
    pos  = pos * (this.options.to-this.options.from);
    pos += this.options.from; 
    if(this.options.beforeUpdate) this.options.beforeUpdate(this);
    if(this.update) this.update(pos);
    if(this.options.afterUpdate) this.options.afterUpdate(this);  
  },
  cancel: function() {
    if(this.timeout) clearTimeout(this.timeout);
  }
}

Effect.Parallel = Class.create();
  Effect.Parallel.prototype = (new Effect.Base()).extend({
    initialize: function(effects) {
      this.effects = effects || [];
       this.start(arguments[1]);
    },
    update: function(position) {
       for (var i = 0; i < this.effects.length; i++)
        this.effects[i].render(position);  
    },
    finish: function(position) {
       for (var i = 0; i < this.effects.length; i++)
          if(this.effects[i].finish) this.effects[i].finish(position);
    }
  });

// Internet Explorer caveat: works only on elements the have
// a 'layout', meaning having a given width or height. 
// There is no way to safely set this automatically.
Effect.Opacity = Class.create();
Effect.Opacity.prototype = (new Effect.Base()).extend({
  initialize: function(element) {
    this.element = $(element);
    options = {
      from: 0.0,
      to:   1.0
    }.extend(arguments[1] || {});
    this.start(options);
  },
  update: function(position) {
    this.setOpacity(position);
  }, 
  setOpacity: function(opacity) {
    opacity = (opacity == 1) ? 0.99999 : opacity;
    this.element.style.opacity = opacity;
    this.element.style.filter = "alpha(opacity:"+opacity*100+")";
  }
});

Effect.MoveBy = Class.create();
 Effect.MoveBy.prototype = (new Effect.Base()).extend({
   initialize: function(element, toTop, toLeft) {
     this.element      = $(element);
     this.originalTop  = parseFloat(this.element.style.top || '0');
     this.originalLeft = parseFloat(this.element.style.left || '0');
     this.toTop        = toTop;
     this.toLeft       = toLeft;
     if(this.element.style.position == "")
       this.element.style.position = "relative";
     this.start(arguments[3]);
   },
   update: function(position) {
     topd  = this.toTop  * position + this.originalTop;
     leftd = this.toLeft * position + this.originalLeft;
     this.setPosition(topd, leftd);
   },
   setPosition: function(topd, leftd) {
     this.element.style.top  = topd  + "px";
     this.element.style.left = leftd + "px";
   }
});

Effect.Scale = Class.create();
Effect.Scale.prototype = (new Effect.Base()).extend({
  initialize: function(element, percent) {
    this.element = $(element)
    options = {
      scaleX: true,
      scaleY: true,
      scaleContent: true,
      scaleFromCenter: false,
      scaleMode: 'box',        // 'box' or 'contents' or {} with provided values
      scaleFrom: 100.0
    }.extend(arguments[2] || {});
    this.originalTop    = this.element.offsetTop;
    this.originalLeft   = this.element.offsetLeft;
    if (this.element.style.fontSize=="") this.sizeEm = 1.0;
    if (this.element.style.fontSize && this.element.style.fontSize.indexOf("em")>0)
       this.sizeEm      = parseFloat(this.element.style.fontSize);
    this.factor = (percent/100.0) - (options.scaleFrom/100.0);
    if(options.scaleMode=='box') {
      this.originalHeight = this.element.clientHeight;
      this.originalWidth  = this.element.clientWidth; 
    } else 
    if(options.scaleMode=='contents') {
      this.originalHeight = this.element.scrollHeight;
      this.originalWidth  = this.element.scrollWidth;
    } else {
      this.originalHeight = options.scaleMode.originalHeight;
      this.originalWidth  = options.scaleMode.originalWidth;
    }
    this.start(options);
  },

  update: function(position) {
    currentScale = (this.options.scaleFrom/100.0) + (this.factor * position);
    if(this.options.scaleContent && this.sizeEm) 
      this.element.style.fontSize = this.sizeEm*currentScale + "em";
    this.setDimensions(
     this.originalWidth * currentScale, 
     this.originalHeight * currentScale);
  },

  setDimensions: function(width, height) {
    if(this.options.scaleX) this.element.style.width = width + 'px';
    if(this.options.scaleY) this.element.style.height = height + 'px';
    if(this.options.scaleFromCenter) {
      topd  = (height - this.originalHeight)/2;
      leftd = (width  - this.originalWidth)/2;
      if(this.element.style.position=='absolute') {
        if(this.options.scaleY) this.element.style.top = this.originalTop-topd + "px";
        if(this.options.scaleX) this.element.style.left = this.originalLeft-leftd + "px";
      } else {
        if(this.options.scaleY) this.element.style.top = -topd + "px";
        if(this.options.scaleX) this.element.style.left = -leftd + "px";
      }
    }
  }
});

Effect.Highlight = Class.create();
Effect.Highlight.prototype = (new Effect.Base()).extend({
  initialize: function(element) {
    this.element = $(element);
    
    // try to parse current background color as default for endcolor
    // browser stores this as: "rgb(255, 255, 255)", convert to "#ffffff" format
    var endcolor = "#ffffff";
    var current = this.element.style.backgroundColor;
    if(current && current.slice(0,4) == "rgb(") {
      endcolor = "#";
      var cols = current.slice(4,current.length-1).split(',');
      var i=0; do { endcolor += parseInt(cols[i]).toColorPart() } while (++i<3); }
      
    var options = {
      startcolor: "#ffff99",
      endcolor:   endcolor
    }.extend(arguments[1] || {});
    
    // init color calculations
    this.colors_base = [
      parseInt(options.startcolor.slice(1,3),16),
      parseInt(options.startcolor.slice(3,5),16),
      parseInt(options.startcolor.slice(5),16) ];
    this.colors_delta = [
      parseInt(options.endcolor.slice(1,3),16)-this.colors_base[0],
      parseInt(options.endcolor.slice(3,5),16)-this.colors_base[1],
      parseInt(options.endcolor.slice(5),16)-this.colors_base[2] ];

    this.start(options);
  },
  update: function(position) {
    var colors = [
      Math.round(this.colors_base[0]+(this.colors_delta[0]*position)),
      Math.round(this.colors_base[1]+(this.colors_delta[1]*position)),
      Math.round(this.colors_base[2]+(this.colors_delta[2]*position)) ];
    this.element.style.backgroundColor = "#" +
      colors[0].toColorPart() + colors[1].toColorPart() + colors[2].toColorPart();
  }
});


/* ------------- prepackaged effects ------------- */

Effect.Fade =  function(element) {
  options = {
  from: 1.0,
  to:   0.0,
  afterFinish: function(effect) 
    { Element.hide(effect.element);
      effect.setOpacity(1); } 
  }.extend(arguments[1] || {});
  new Effect.Opacity(element,options);
}

Effect.Appear =  function(element) {
  options = {
  from: 0.0,
  to:   1.0,
  beforeStart: function(effect)  
    { effect.setOpacity(0);
      Element.show(effect.element); },
  afterUpdate: function(effect)  
    { Element.show(effect.element); }
  }.extend(arguments[1] || {});
  new Effect.Opacity(element,options);
}

Effect.Puff = function(element) {
  new Effect.Parallel(
   [ new Effect.Scale(element, 200, { sync: true, scaleFromCenter: true }), 
     new Effect.Opacity(element, { sync: true, to: 0.0, from: 1.0 } ) ], 
     { duration: 1.0, 
      afterUpdate: function(effect) 
       { effect.effects[0].element.style.position = 'absolute'; },
      afterFinish: function(effect)
       { Element.hide(effect.effects[0].element); }
     }
   );
}

Effect.BlindUp = function(element) {
  $(element)._overflow = $(element).style.overflow || 'visible';
  $(element).style.overflow = 'hidden';
  new Effect.Scale(element, 0, 
    { scaleContent: false, 
      scaleX: false, 
      afterFinish: function(effect) 
        { 
          Element.hide(effect.element);
          effect.element.style.overflow = effect.element._overflow;
        } 
    }.extend(arguments[1] || {})
  );
}

Effect.BlindDown = function(element) {
  $(element).style.height   = '0px';
  $(element)._overflow = $(element).style.overflow || 'visible';
  $(element).style.overflow = 'hidden';
  Element.show(element);
  new Effect.Scale(element, 100, 
    { scaleContent: false, 
      scaleX: false, 
      scaleMode: 'contents',
      scaleFrom: 0,
      afterFinish: function(effect) {
        effect.element.style.overflow = effect.element._overflow;
      }
    }.extend(arguments[1] || {})
  );
}

Effect.SwitchOff = function(element) {
  new Effect.Appear(element,
    { duration: 0.4,
     transition: Effect.Transitions.flicker,
     afterFinish: function(effect)
      {  effect.element.style.overflow = 'hidden';
        new Effect.Scale(effect.element, 1, 
         { duration: 0.3, scaleFromCenter: true,
          scaleX: false, scaleContent: false,
          afterUpdate: function(effect) { 
           if(effect.element.style.position=="")
             effect.element.style.position = 'relative'; },
          afterFinish: function(effect) { Element.hide(effect.element); }
         } )
      }
    } )
}

Effect.DropOut = function(element) {
  new Effect.Parallel(
    [ new Effect.MoveBy(element, 100, 0, { sync: true }), 
      new Effect.Opacity(element, { sync: true, to: 0.0, from: 1.0 } ) ], 
    { duration: 0.5, 
     afterFinish: function(effect)
       { Element.hide(effect.effects[0].element); } 
    });
}

Effect.Shake = function(element) {
  new Effect.MoveBy(element, 0, 20, 
    { duration: 0.05, afterFinish: function(effect) {
  new Effect.MoveBy(effect.element, 0, -40, 
    { duration: 0.1, afterFinish: function(effect) { 
  new Effect.MoveBy(effect.element, 0, 40, 
    { duration: 0.1, afterFinish: function(effect) {  
  new Effect.MoveBy(effect.element, 0, -40, 
    { duration: 0.1, afterFinish: function(effect) {  
  new Effect.MoveBy(effect.element, 0, 40, 
    { duration: 0.1, afterFinish: function(effect) {  
  new Effect.MoveBy(effect.element, 0, -20, 
    { duration: 0.05, afterFinish: function(effect) {  
  }}) }}) }}) }}) }}) }});
}

Effect.SlideDown = function(element) {
  $(element)._overflow = $(element).style.overflow || 'visible';
  $(element).style.height   = '0px';
  $(element).style.overflow = 'hidden';
  $(element).firstChild.style.position = 'relative';
  Element.show(element);
  new Effect.Scale(element, 100, 
   { scaleContent: false, 
    scaleX: false, 
    scaleMode: 'contents',
    scaleFrom: 0,
    afterUpdate: function(effect) 
      { effect.element.firstChild.style.bottom = 
          (effect.originalHeight - effect.element.clientHeight) + 'px'; },
    afterFinish: function(effect) 
      {  effect.element.style.overflow = effect.element._overflow; }
    }.extend(arguments[1] || {})
  );
}
  
Effect.SlideUp = function(element) {
  $(element)._overflow = $(element).style.overflow || 'visible';
  $(element).style.overflow = 'hidden';
  $(element).firstChild.style.position = 'relative';
  Element.show(element);
  new Effect.Scale(element, 0, 
   { scaleContent: false, 
    scaleX: false, 
    afterUpdate: function(effect) 
      { effect.element.firstChild.style.bottom = 
          (effect.originalHeight - effect.element.clientHeight) + 'px'; },
    afterFinish: function(effect)
      { 
        Element.hide(effect.element);
        effect.element.style.overflow = effect.element._overflow; 
      }
   }.extend(arguments[1] || {})
  );
}

Effect.Squish = function(element) {
 new Effect.Scale(element, 0, 
   { afterFinish: function(effect) { Element.hide(effect.element); } });
}

Effect.Grow = function(element) {
  element = $(element);
  var options = arguments[1] || {};
  
  var originalWidth = element.clientWidth;
  var originalHeight = element.clientHeight;
  element.style.overflow = 'hidden';
  Element.show(element);
  
  var direction = options.direction || 'center';
  var moveTransition = options.moveTransition || Effect.Transitions.sinoidal;
  var scaleTransition = options.scaleTransition || Effect.Transitions.sinoidal;
  var opacityTransition = options.opacityTransition || Effect.Transitions.full;
  
  var initialMoveX, initialMoveY;
  var moveX, moveY;
  
  switch (direction) {
    case 'top-left':
      initialMoveX = initialMoveY = moveX = moveY = 0; 
      break;
    case 'top-right':
      initialMoveX = originalWidth;
      initialMoveY = moveY = 0;
      moveX = -originalWidth;
      break;
    case 'bottom-left':
      initialMoveX = moveX = 0;
      initialMoveY = originalHeight;
      moveY = -originalHeight;
      break;
    case 'bottom-right':
      initialMoveX = originalWidth;
      initialMoveY = originalHeight;
      moveX = -originalWidth;
      moveY = -originalHeight;
      break;
    case 'center':
      initialMoveX = originalWidth / 2;
      initialMoveY = originalHeight / 2;
      moveX = -originalWidth / 2;
      moveY = -originalHeight / 2;
      break;
  }
  
  new Effect.MoveBy(element, initialMoveY, initialMoveX, { 
    duration: 0.01, 
    beforeUpdate: function(effect) { $(element).style.height = '0px'; },
    afterFinish: function(effect) {
      new Effect.Parallel(
        [ new Effect.Opacity(element, { sync: true, to: 1.0, from: 0.0, transition: opacityTransition }),
          new Effect.MoveBy(element, moveY, moveX, { sync: true, transition: moveTransition }),
          new Effect.Scale(element, 100, { 
            scaleMode: { originalHeight: originalHeight, originalWidth: originalWidth }, 
            sync: true, scaleFrom: 0, scaleTo: 100, transition: scaleTransition })],
        options); }
    });
}

Effect.Shrink = function(element) {
  element = $(element);
  var options = arguments[1] || {};
  
  var originalWidth = element.clientWidth;
  var originalHeight = element.clientHeight;
  element.style.overflow = 'hidden';
  Element.show(element);

  var direction = options.direction || 'center';
  var moveTransition = options.moveTransition || Effect.Transitions.sinoidal;
  var scaleTransition = options.scaleTransition || Effect.Transitions.sinoidal;
  var opacityTransition = options.opacityTransition || Effect.Transitions.none;
  
  var moveX, moveY;
  
  switch (direction) {
    case 'top-left':
      moveX = moveY = 0;
      break;
    case 'top-right':
      moveX = originalWidth;
      moveY = 0;
      break;
    case 'bottom-left':
      moveX = 0;
      moveY = originalHeight;
      break;
    case 'bottom-right':
      moveX = originalWidth;
      moveY = originalHeight;
      break;
    case 'center':  
      moveX = originalWidth / 2;
      moveY = originalHeight / 2;
      break;
  }
  
  new Effect.Parallel(
    [ new Effect.Opacity(element, { sync: true, to: 0.0, from: 1.0, transition: opacityTransition }),
      new Effect.Scale(element, 0, { sync: true, transition: moveTransition }),
      new Effect.MoveBy(element, moveY, moveX, { sync: true, transition: scaleTransition }) ],
    options);
}

Effect.Pulsate = function(element) {
  var options    = arguments[1] || {};
  var transition = options.transition || Effect.Transitions.sinoidal;
  var reverser   = function(pos){ return transition(1-Effect.Transitions.pulse(pos)) };
  reverser.bind(transition);
  new Effect.Opacity(element, 
    {  duration: 3.0,
       afterFinish: function(effect) { Element.show(effect.element); }
    }.extend(options).extend({transition: reverser}));
}

Effect.Fold = function(element) {
 $(element).style.overflow = 'hidden';
 new Effect.Scale(element, 5, {   
   scaleContent: false,
   scaleTo: 100,
   scaleX: false,
   afterFinish: function(effect) {
   new Effect.Scale(element, 1, { 
     scaleContent: false, 
     scaleTo: 0,
     scaleY: false,
     afterFinish: function(effect) { Element.hide(effect.element) } });
 }}.extend(arguments[1] || {}));
}

// old: new Effect.ContentZoom(element, percent)
// new: Element.setContentZoom(element, percent) 

Element.setContentZoom = function(element, percent) {
  var element = $(element);
  element.style.fontSize = (percent/100) + "em";  
  if(navigator.appVersion.indexOf('AppleWebKit')>0) window.scrollBy(0,0);
}
