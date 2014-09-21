'use strict';

var redis                         = require('redis');
var async                         = require('async');
var ConfigurationStorageInterface = require('spid-storage-configuration-interface');
var _                             = require('lodash');

function RedisStorageConfiguration() {
  this._client     = null;

  this._publishKey = null;

  /**
   * Array of listeners
   * @type {Array} array of object {keys:Array[String], f(key: String, newValue: String):Function}
   */
  this._listeners  = [];

  /**
   * Redis client for regular commands
   * @type {RedisClient}
   */
  this._client     = null;

  /**
   * Redis client for subscribing commands
   * @type {RedisClient}
   */
  this._clientSub = null;
}

/**
 * [init description]
 * @param  {Function} f(err)
 */
RedisStorageConfiguration.prototype.init = function (configuration, f) {

  configuration({
    /**
     * @type {Number} redis port number
     */
    port: 6379,

    /**
     * @type {String} host name, default localhost
     */
    host: '127.0.0.1',

    /**
     * [password description]
     * @type {String} password
     */
    password: null,

    /**
     * @type {String} publish key prefix to use while publishing change event in redis
     */
    publishKey: [RedisStorageConfiguration.name, 'changed'].join(':')
  }, _.partialRight(this.applyConfiguration.bind(this), f));
};

/**
 * Apply a configuration that can change at runtime
 * @param  {Null|Object} stale
 *                       old configuration if defined, `null` otherwise
 * @param  {Null|Object} fresh
 *                       new configuration if defined, `null` otherwise
 * @param {Function}     f(err)
 *
 */
RedisStorageConfiguration.prototype.applyConfiguration = function(stale, fresh, f){
  if(stale && this._client){
    this._client.quit();
    this._clientSub.quit();
  }

  if(fresh){
    f = _.once(f); // it can only be called once (either from 'ready', or from 'error')

    this._publishKey = fresh.publishKey;

    try {
      var options = {
        max_attempts: 1,
        auth_pass: fresh.password
      };

      this._client    = redis.createClient(fresh.port, fresh.host, options);
      this._clientSub = redis.createClient(fresh.port, fresh.host, options);

      var fDone = _.after(2, f);
      // Setup client
      this._client.once('ready', fDone);

      // Setup subscriber client
      this._clientSub.on('message', this.parseNotification.bind(this));
      this._clientSub.once('ready', function(){
        this._clientSub.subscribe(this._publishKey);
        fDone();
      }.bind(this));


      this._client.on('error', _.compose(f, this.onRedisError.bind(this)));
      this._clientSub.on('error', _.compose(f, this.onRedisError.bind(this)));
    } catch (e) {
      f(e);
    }
    return;
  }

  // `fresh` was falsy, just call the callback
  f();
};

RedisStorageConfiguration.prototype.onRedisError = function(err){
  console.log(err);
  // what should we do in case of redis error ?
  // currently we only print it to the default logger
  return err;
};

/**
 * [dispose description]
 * @param  {Function} f(err)
 */
RedisStorageConfiguration.prototype.dispose = function (f) {
  if (this._client.connected) {
    this._client.quit();
    this._clientSub.quit();
    return this.unwatch(null, f);
  }

  f(new Error(RedisStorageConfiguration.name + ' was not connected'));
};

/**
 * [read description]
 * @param  {[type]} key  [description]
 * @param  {[type]} value [description]
 * @param  {Function} f(err, value)
 */
RedisStorageConfiguration.prototype.read = function (key, f) {
  this._client.get(key, function (err, reply) {
    if (err) {
      f(err);
      return;
    }

    f(null, reply);
  }.bind(this));
};

/**
 * [write description]
 * @param  {[type]} key  [description]
 * @param  {[type]} value [description]
 * @param  {Function} f(err)
 */
RedisStorageConfiguration.prototype.write = function (key, value, f) {
  this._client.set(key, value, function(err){
    if(err){return f(err);}
    this.notifyChange(key, value);
    f(null);
  }.bind(this));
};

/**
 * [write description]
 * @param  {[type]} key  [description]
 * @param  {Function} f(err)
 */
RedisStorageConfiguration.prototype.remove = function (key, f) {
  this._client.del(key, function(err){
    if(err){return f(err);}
    this.notifyChange(key, null);
    f(null);
  }.bind(this));
};


/**
 * Watch keys for change
 * @param {Array[String]} keys array of keys to watch
 * @param {Function} f(key: String, newValue: String)
 */
RedisStorageConfiguration.prototype.watch = function (keys, f) {
  this._listeners.push({keys: keys, f: f});
};

/**
 * Unwatch keys
 * @param  {[type]} keys [description]
 * @param  {Function} f(err)
 * @return {[type]}     [description]
 */
RedisStorageConfiguration.prototype.unwatch = function (keys, f) {
  if(!keys){
    this._listeners = [];
    return f();
  }

  var sizeBefore = this._listeners.length;
  this._listeners = _.remove(this._listeners, function(listener){
    return listener.f === f && _.difference(keys, listener.f).length === 0;
  });
  f(sizeBefore - this._listeners.length !== 1 ? new Error('Listener not found') : null);
};

// Helpers
RedisStorageConfiguration.prototype.notifyChange = function(key, value){
  this._client.publish(this._publishKey, JSON.stringify([{key: key, value: value}]));
};

RedisStorageConfiguration.prototype.parseNotification = function(channel, message){
  var changes;
  try{
    changes = JSON.parse(message);
  }catch(err){
    console.error('Invalid Message: ' + message);
    return;
  }

  changes.forEach(function(change){
    this.changeNotification(change.key, change.value);
  }.bind(this));
};

RedisStorageConfiguration.prototype.changeNotification = function(key, value){
  this._listeners.forEach(function(listener){
    if(listener.keys.indexOf(key) === -1){
      return; // skip
    }

    listener.f(key, value);
  });
};


module.exports = ConfigurationStorageInterface.ensureImplements(RedisStorageConfiguration);
