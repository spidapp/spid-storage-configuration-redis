'use strict';

var redis = require('redis');
var async = require('async');
var ConfigurationStorageInterface = require('spid-storage-configuration-interface');
var _ = require('lodash');

function RedisStorageConfiguration() {
  this._client = null;

  this._publishKey = null;

  /**
   * Array of listeners
   * @type {Array} array of object {keys:Array[String], f(key: String, newValue: String):Function}
   */
  this._listeners = [];

  /**
   * Redis client for regular commands
   * @type {RedisClient}
   */
  this._client = null;

  /**
   * Redis client for subscribing commands
   * @type {RedisClient}
   */
  this._clientSub = null;
}

/**
 * [init description]
 * @param  {Object}  configuration  base configuration
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
RedisStorageConfiguration.prototype.applyConfiguration = function (stale, fresh, f) {
  if (stale && this._client) {
    this._client.quit();
    this._clientSub.quit();
  }

  if (fresh) {
    f = _.once(f); // it can only be called once (either from 'ready', or from 'error')

    this._publishKey = fresh.publishKey;

    try {
      var options = {
        max_attempts: 1,
        auth_pass: fresh.password
      };

      this._client = redis.createClient(fresh.port, fresh.host, options);
      this._clientSub = redis.createClient(fresh.port, fresh.host, options);

      var fDone = _.after(2, f);
      // Setup client
      this._client.once('ready', fDone);

      // Setup subscriber client
      this._clientSub.on('message', this.parseNotification.bind(this));
      this._clientSub.once('ready', function () {
        this._clientSub.subscribe(this._publishKey);
        fDone();
      }.bind(this));

      this._client.on('error', _.partialRight(onRedisError, f));
      this._clientSub.on('error', _.partialRight(onRedisError, f));
    } catch (e) {
      f(e);
    }
    return;
  }

  // `fresh` was falsy, just call the callback
  f();
};


/**
 * [dispose description]
 * @param  {Function} f(err)
 */
RedisStorageConfiguration.prototype.dispose = function (f) {
  if (this._client.connected) {
    this._client.quit();
    this._clientSub.quit();
    return this.unwatch(null, null, f);
  }

  f(new Error(RedisStorageConfiguration.name + ' was not connected'));
};

/**
 * [read description]
 * @param  {String}  prefix
 * @param  {Array[String]} keys  [key1, key2, ...]
 * @param  {Function} f(err, properties) e.g. {key1: value1, key2: value2, ...}
 */
RedisStorageConfiguration.prototype.read = function (prefix, keys, f) {
  var prefixedKeys = withPrefix(keys, prefix);

  this._client.mget(prefixedKeys, function (err, reply) {
    if (err) {
      f(err);
      return;
    }

    var properties = _.reduce(prefixedKeys, function (properties, key, index) {
      properties[keys[index]] = reply[index] ? reply[index] : null;
      return properties;
    }, {});

    f(null, properties);
  }.bind(this));
};

/**
 * [write description]
 * @param  {String} prefix  [description]
 * @param  {Object} properties e.g. {key1: value1, key2: value2}
 * @param  {Function} f(err)
 */
RedisStorageConfiguration.prototype.write = function (prefix, properties, f) {
  this._client.mset(propertiesToArrayWithPrefix(prefix, properties), function (err) {
    if (err) {
      return f(err);
    }
    this.notifyChange(prefix, properties);
    f(null);
  }.bind(this));
};

/**
 * [write description]
 * @param  {String} prefix  [description]
 * @param  {Array[String]}  keys  keys to delete
 * @param  {Function} f(err)
 */
RedisStorageConfiguration.prototype.remove = function (prefix, keys, f) {
  this._client.del(withPrefix(keys, prefix), function (err) {
    if (err) {
      return f(err);
    }

    var undefinedProperties = _.reduce(keys, function (undefinedProperties, key) {
      undefinedProperties[key] = null;
      return undefinedProperties;
    }, {});

    // @FIXME: if we notify an undefined property, the object undefinedProperties will drop the key, hence lose the notification for this key
    this.notifyChange(prefix, undefinedProperties);
    f(null);
  }.bind(this));
};


/**
 * Watch keys for change
 * @param {String} prefix
 * @param {Array[String]} keys array of keys to watch
 * @param {Function} f(updatedProperties)
 */
RedisStorageConfiguration.prototype.watch = function (prefix, keys, f) {
  this._listeners.push({
    keys: withPrefixAsObject(keys, prefix),
    f: f
  });
};

/**
 * Unwatch keys
 * @param  {String} prefix [description]
 * @param  {Array[String]} keys array of keys to unwatch
 * @param  {Function} f(err)
 */
RedisStorageConfiguration.prototype.unwatch = function (prefix, keys, f) {
  if (!keys) {
    this._listeners = [];
    return f();
  }

  var sizeBefore = this._listeners.length;
  this._listeners = _.remove(this._listeners, function (listener) {
    return listener.f === f && _.difference(keys, listener.f).length === 0;
  });
  f(sizeBefore - this._listeners.length !== 1 ? new Error('Listener not found') : null);
};

// Helpers
RedisStorageConfiguration.prototype.notifyChange = function (prefix, properties) {
  this._client.publish(this._publishKey, JSON.stringify({
    'prefix': prefix,
    'properties': properties
  }));
};

RedisStorageConfiguration.prototype.parseNotification = function (channel, message) {
  var changes;
  try {
    changes = JSON.parse(message);
  } catch (err) {
    console.error('Invalid Message: ' + message);
    return;
  }

  this.changeNotification(changes.prefix, changes.properties);
};

RedisStorageConfiguration.prototype.changeNotification = function (prefix, properties) {
  var prefixedKeys = withPrefix(_.keys(properties), prefix);
  this._listeners.forEach(function (listener) {
    // diff({c, e}, {a, b, c}) -> {e} -> keep ("c" has been updated)
    // diff({e, d}, {a, b, c}) -> {e, d} -> skip
    if (_.difference(prefixedKeys, _.keys(listener.keys)).length === prefixedKeys.length) {
      return; // skip
    }

    // Take the following example :
    // diff({c, e}, {a, b, c}) -> {e} -> keep ("c" has been updated)
    // we don't want to call the listener with the "e" key/value, we just want to forward
    // "c" key/value.

    var propertiesForCurrentListener = _.intersection(prefixedKeys, _.keys(listener.keys)).reduce(function (fresh, prefixedKey) {
      fresh[listener.keys[prefixedKey]] = properties[listener.keys[prefixedKey]];
      return fresh;
    }, {});

    listener.f(propertiesForCurrentListener);
  });
};

/**
 * Convert properties object to array for mset
 * e.g. {key1: value1, key2: value2} to [prefixedKey1, value1, prefixedKey2, value2]
 * @param prefix
 * @param properties
 * @returns {*}
 */
function propertiesToArrayWithPrefix(prefix, properties) {

  return _.reduce(properties, function (propertiesArray, value, key) {
    propertiesArray.push(withPrefix(key, prefix));
    propertiesArray.push(value);
    return propertiesArray;
  }, []);
}

/**
 * @param  {Error} err
 * @param  {Function} f(err)
 */
function onRedisError(err, f) {
  // what should we do in case of redis error ?
  // currently we print it to the default logger
  console.log(err);
  // and forward it to f
  f(err);
}

/**
 * @param  {String|Array[String]} keys
 * @param  {String} prefix
 * @return {String|Array[String]}
 */
function withPrefix(keys, prefix) {
  if (_.isString(keys)) {
    return addPrefix(prefix, keys);
  }

  return keys.map(_.partial(addPrefix, prefix));
}

function addPrefix(prefix, key) {
  return prefix + '.' + key;
}

/**
 * @param  {Array[String]} keys
 * @param  {String} prefix
 * @return {Object}        e.g. {prefixedKey : rawKeyName, prefixedKey2 : rawKeyName2}
 */
function withPrefixAsObject(keys, prefix) {
  return keys.reduce(function (obj, key) {
    obj[addPrefix(prefix, key)] = key;
    return obj;
  }, {});
}

module.exports = ConfigurationStorageInterface.ensureImplements(RedisStorageConfiguration);
