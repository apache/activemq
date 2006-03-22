// Technique borrowed from scriptaculous to do includes.

var DefaultJS = {
  Version: 'AMQ default JS',
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
        // this.script(path + 'scriptaculous.js');
        this.script(path + 'amq.js');
        break;
      }
    }
  }
}

DefaultJS.load();

