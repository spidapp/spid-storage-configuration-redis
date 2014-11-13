'use strict';

var RedisConfigurationStorage = require('../');
var Configuration = require('./stub/Configuration');
var t = require('chai').assert;
var _ = require('lodash');

var redisConfig = {
  port: parseInt(process.env.REDIS_PORT, 10),
  host: process.env.REDIS_HOST,
  password: process.env.REDIS_PASSWORD
};

var KEYS = ['key', 'key1'];
var VALUES = ['value', 'value1'];
var PREFIX = 'spid-ConfigurationStorage';
var PROPERTIES = {'key': 'value', 'key1': 'value1'};

describe('RedisConfigurationStorage', function () {
  var storage, configuration;

  beforeEach(function (done) {
    storage = new RedisConfigurationStorage();
    configuration = Configuration.get();
    done();
  });

  it('default configuration should be available', function (f) {
    storage.init(configuration, function (err) {
      if (err) {
        throw err;
      }
    });

    t.strictEqual(configuration.test.params.port, 6379);
    t.strictEqual(configuration.test.params.host, '127.0.0.1');
    t.strictEqual(configuration.test.params.password, null);

    f();
  });

  it('should connect to redis', function (f) {
    storage.init(configuration, function (err) {
      t.equal(err, void 0);
      storage.dispose(f);
    });

    configuration.test.f(null, _.extend({}, configuration.test.params, redisConfig));
  });

  describe('once connected', function () {
    beforeEach(function (f) {
      // init config
      storage.init(configuration, function (err) {
        if (err) {
          throw err;
        }
        storage._client.flushdb(function () {
          f();
        });
      });
      configuration.test.f(null, _.extend({}, configuration.test.params, redisConfig));
    });

    describe('.write', function () {
      it('should be able to write to storage', function (f) {
        storage.write(PREFIX, PROPERTIES, function (err) {
          t.strictEqual(err, null);
          f();
        });
      });
    });

    describe('.read', function () {
      it('should be able to read non-existent key', function (f) {
        storage.read(PREFIX, [KEYS[0]], function (err, properties) {
          t.strictEqual(err, null);
          t.strictEqual(properties[KEYS[0]], null);
          f();
        });
      });

      it('should be able to read key', function (f) {
        storage.write(PREFIX, PROPERTIES, function (err) {
          storage.read(PREFIX, KEYS, function (err, properties) {
            t.strictEqual(properties[KEYS[0]], VALUES[0]);
            f();
          });
        });
      });
    });

    describe('.remove', function () {
      it('should be able to remove a non-existent key', function (f) {
        storage.watch(PREFIX, [KEYS[0]], function (key, newValue) {
          throw new Error("Should not be called");
        });

        storage.remove(PREFIX, [KEYS[0]], function (err) {
          t.strictEqual(err, null);
          f();
        });
      });

      it('should be able to remove a key', function (f) {

        storage.write(PREFIX, PROPERTIES, function (err) {

          storage.watch(PREFIX, [KEYS[0]], function (updatedProperties) {
            t.strictEqual(updatedProperties[KEYS[0]], null);
            f();
          });

          storage.remove(PREFIX, [KEYS[0]], function (err) {
            t.strictEqual(err, null);
          });
        });
      });
    });

    describe('.watch', function () {
      it('should be able to watch for a key change', function (f) {
        storage.watch(PREFIX, ['a', 'b', 'c'], function (updatedProperties) {
          t.strictEqual(updatedProperties['b'], 'hello world');
          f();
        });

        storage.write(PREFIX, {'b': 'hello world'}, _.noop);
      });
    });

    describe('on configuration change', function () {
      it('should reconnect', function (f) {
        storage._client.OLD = true;
        configuration.test.f(redisConfig, redisConfig, function () {
          t.strictEqual(storage._client.OLD, undefined);
          f();
        });
      });
    });

    afterEach(function (f) {
      storage.dispose(f);
    });
  });
});
