// Technique borrowed from scriptaculous to do includes.

var DefaultJS = {
  Version: 'Jetty Test',
  script: function(libraryName) {
    document.write('<script type="text/javascript" src="'+libraryName+'"></script>');
  },
  load: function() {
    var scriptTags = document.getElementsByTagName("script");
    for(var i=0;i<scriptTags.length;i++) {
      if(scriptTags[i].src && scriptTags[i].src.match(/default\.js$/)) {
        var path = scriptTags[i].src.replace(/default\.js$/,'');
        this.script(path + 'prototype.js');
        this.script(path + 'behaviour.js');
        
        /* Ignore non-UI parts of Rico
        this.script(path + 'rico.js');
        */
        this.script(path + 'ricoAjax.js');
        
        this.script(path + 'scriptaculous.js');
        break;
      }
    }
  }
}

DefaultJS.load();