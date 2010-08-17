/**
 * OrionDOMStorageAdapter
 *
 * DOM Storage implementation for Lawnchair (specific to Orion).
 *
 * Originally authored by Joseph Pecoraro.
 *
 * window.name code courtesy Remy Sharp:
 *  http://24ways.org/2009/breaking-out-the-edges-of-the-browser
 *
 * Significant portions rewritten by Sean Eidemiller (sean@halcyon.cc) and Mike Ball
 * (mike.ball@eloqua.com) to improve performance on slower browsers (including Firefox 3.x and 4.0
 * betas).
 */
var OrionDOMStorageAdapter = function(options) {
  for (var i in LawnchairAdaptorHelpers) {
    if (LawnchairAdaptorHelpers.hasOwnProperty(i)) {
      this[i] = LawnchairAdaptorHelpers[i];
    }
  }

  this.init(options);
};

OrionDOMStorageAdapter.prototype = {

  _indexArrayName: null,

  /**
   * Initializes the adapter with the given options (hash).
   */
  init: function(options) {
    var self = this;
    this.storage = this.merge(window.localStorage, options.storage);
    this.table = this.merge('field', options.table);

    // Initialize the index table.
    this._indexArrayName = this.table + ':index';
    var indexArray = this.deserialize(this.storage.getItem(this._indexArrayName));
    if (!indexArray) {
      indexArray = [];
      this.storage.setItem(this._indexArrayName, this.serialize(indexArray));
    }
 
    // Fallback for the stupider browsers/versions.
    if (!(this.storage instanceof window.Storage)) {
      this.storage = (function () {
        // window.top.name ensures top level, and supports around 2Mb
        var data = window.top.name ? self.deserialize(window.top.name) : {};
        return {
          setItem: function (key, value) {
            data[key] = value + "";
            window.top.name = self.serialize(data);
          },
          removeItem: function (key) {
            delete data[key];
            window.top.name = self.serialize(data);
          },
          getItem: function (key) {
            return data[key] || null;
          },
          clear: function () {
            data = {};
            window.top.name = '';
          }
        };
      })();
    }
  },

  /**
   * Writes a single object or an array of objects to the local storage.
   *
   * @param {Object|Array} obj The object or array of objects to write.
   * @param {Function} callback The optional callback to invoke on completion.
   */
  save: function(obj, callback) {
    var key = obj.key;

    if (key instanceof Array) {
      // Got an array of objects.
      this._saveAll(obj, callback);
      return;
    }

    // Store the ID of the object in the index array.
    var index = this.deserialize(this.storage[this._indexArrayName]);
    var id = this.table + ':' + (key || this.uuid());

    index.push(id);
    this.storage.setItem(this._indexArrayName, this.serialize(index));

    // Store the object itself.
    this.storage.setItem(id, this.serialize(obj.record));

    // Invoke the callback.
    if (callback) callback(obj);
  },

  _saveAll: function(obj, callback) {
    var table = this.table;
    var ids = obj.key;
    var records = obj.records;
    var index = this.deserialize(this.storage[this._indexArrayName]);
    var currIndex, id;

    for (var i = 0, len = ids.length; i < len; i++) {
      // Store the ID of the record in the index array.
      id = this.table + ':' + ids[i];
      currIndex = index.indexOf(id);

      if (currIndex <= 0) {
        index.push(id);
      } else {
        index[currIndex] = id;
      }

      // Store the record itself.
      this.storage.setItem(id, this.serialize(records[i]));
    }

    // Reserialize the index array.
    this.storage.setItem(this._indexArrayName, this.serialize(index));

    // Invoke the callback.
    if (callback) callback();
  },

  /**
   * Reads a single object from the local storage.
   *
   * @param {String} id The ID of the object.
   * @param {Function} callback The optional callback to invoke on completion.
   */
  get: function(id, callback) {
    var obj = this.deserialize(this.storage.getItem(this.table + ':' + id));
    if (obj) {
      obj.key = id;
      if (callback) callback(obj);
    } else {
      if (callback) callback(null);
    }
  },

  /**
   * Reads all of the objects in this table from the local storage.
   *
   * @param {Function} callback The optional callback to invoke on completion.
   */
  all: function(callback) {
    var cb = this.terseToVerboseCallback(callback);
    var results = [];
    var table = this.table;
    var id, obj;

    // Get the index for the table and iterate over them.
    var index = this.deserialize(this.storage[this._indexArrayName]);

    for (var i = 0, len = index.length; i < len; ++i) {
      id = index[i];
      obj = this.storage.getItem(id); 
      // Push the item (as a string) onto the results array.
      if (obj) results.push(obj);
    }

    // Concatenate the entire results array and deserialize.
    var allRecords = results.join(',');
    var ret = this.deserialize('[' + allRecords + ']');

    // Invoke the callback.
    if (cb) cb(ret);
  },

  /**
   * Removes a single object from the local storage.
   *
   * @param {String} id The ID of the object.
   * @param {Function} callback The optional callback to invoke on completion.
   */
  remove: function(id, callback) {
    // TODO: [SE] Remove IDs from index array.
    var key = this.table + ':' + id;
    this.storage.removeItem(key);

    // Invoke the callback.
    if (callback) callback();
  },

  /**
   * Removes all data associated with this table from the local storage.
   *
   * @param {Function} callback The optional callback to invoke on completion.
   */
  nuke: function(callback) {
    var self = this;

    // Remove all of the objects.
    this.all(function(r) {
      for (var i = 0, l = r.length; i < l; i++) {
        self.remove(r[i]);
      }

      // Remove the index array.
      self.remove(self._indexArrayName);

      // Invoke the callback.
      if (callback) callback();
    });
  }
};
