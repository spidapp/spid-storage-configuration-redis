'use strict';

var _ = require('lodash');

function Configuration(){
  _.bindAll(this);
  this.test = {};
}

Configuration.prototype.stub = function(params, f){
  this.test.params = params;
  this.test.f = f;
};

module.exports = {
  get: function(){
    var configuration = new Configuration();
    var f = configuration.stub;
    f.test = configuration.test;
    return f;
  }
};
