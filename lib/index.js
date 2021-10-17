"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
  function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
  return new (P || (P = Promise))(function (resolve, reject) {
    function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
    function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
    function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
    step((generator = generator.apply(thisArg, _arguments || [])).next());
  });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
  var _ = { label: 0, sent: function () { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
  return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function () { return this; }), g;
  function verb(n) { return function (v) { return step([n, v]); }; }
  function step(op) {
    if (f) throw new TypeError("Generator is already executing.");
    while (_) try {
      if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
      if (y = 0, t) op = [op[0] & 2, t.value];
      switch (op[0]) {
        case 0: case 1: t = op; break;
        case 4: _.label++; return { value: op[1], done: false };
        case 5: _.label++; y = op[1]; op = [0]; continue;
        case 7: op = _.ops.pop(); _.trys.pop(); continue;
        default:
          if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
          if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
          if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
          if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
          if (t[2]) _.ops.pop();
          _.trys.pop(); continue;
      }
      op = body.call(thisArg, _);
    } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
    if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
  }
};
var __spreadArrays = (this && this.__spreadArrays) || function () {
  for (var s = 0, i = 0, il = arguments.length; i < il; i++) s += arguments[i].length;
  for (var r = Array(s), k = 0, i = 0; i < il; i++)
    for (var a = arguments[i], j = 0, jl = a.length; j < jl; j++, k++)
      r[k] = a[j];
  return r;
};
function __export(m) {
  for (var p in m) if (!exports.hasOwnProperty(p)) exports[p] = m[p];
}
Object.defineProperty(exports, "__esModule", { value: true });
var build_1 = require("./build");
exports.buildToInsert = build_1.buildToSave;
exports.buildToInsertBatch = build_1.buildToSaveBatch;
__export(require("./build"));
var ClientManager = (function () {
  function ClientManager(client) {
    this.client = client;
    this.exec = this.exec.bind(this);
    this.execBatch = this.execBatch.bind(this);
    this.query = this.query.bind(this);
    this.queryOne = this.queryOne.bind(this);
    this.execScalar = this.execScalar.bind(this);
    this.count = this.count.bind(this);
  }
  ClientManager.prototype.exec = function (sql, args) {
    return exec(this.client, sql, args);
  };
  ClientManager.prototype.execBatch = function (statements, batchSize, options) {
    return execBatch(this.client, statements, batchSize, options);
  };
  ClientManager.prototype.query = function (sql, args, m, bools, options) {
    return query(this.client, sql, args, m, bools, options);
  };
  ClientManager.prototype.queryOne = function (sql, args, m, bools, options) {
    return queryOne(this.client, sql, args, m, bools, options);
  };
  ClientManager.prototype.execScalar = function (sql, args) {
    return execScalar(this.client, sql, args);
  };
  ClientManager.prototype.count = function (sql, args) {
    return count(this.client, sql, args);
  };
  return ClientManager;
}());
exports.ClientManager = ClientManager;
function execOneRawBatch(client, statements, options) {
  return client.batch(statements, options ? options : { prepare: true }).then(function (result) {
    return result.rows;
  });
}
exports.execOneRawBatch = execOneRawBatch;
function execOneBatch(client, statements, options) {
  return client.batch(statements, options ? options : { prepare: true }).then(function (r) { return toLength(r); });
}
exports.execOneBatch = execOneBatch;
function execBatch(client, statements, batchSize, options) {
  return __awaiter(this, void 0, void 0, function () {
    var arr;
    return __generator(this, function (_a) {
      switch (_a.label) {
        case 0:
          if (!(batchSize && batchSize > 0 && statements.length > batchSize)) return [3, 2];
          arr = [];
          while (statements.length !== 0) {
            if (statements.length > batchSize) {
              arr.push(client.batch(statements.splice(0, batchSize), options ? options : { prepare: true }).then(function (r) { return toLength(r); }));
            }
            else {
              arr.push(client.batch(statements.splice(0, statements.length), options ? options : { prepare: true }).then(function (r) { return toLength(r); }));
            }
          }
          return [4, handlePromises(arr)];
        case 1: return [2, _a.sent()];
        case 2: return [2, client.batch(statements, options ? options : { prepare: true }).then(function (r) { return toLength(r); })];
      }
    });
  });
}
exports.execBatch = execBatch;
function handlePromises(arr) {
  return Promise.all(arr).then(function (r) {
    var c = 0;
    r.forEach(function (item) {
      if (item) {
        c = c + item;
      }
    });
    return c;
  });
}
exports.handlePromises = handlePromises;
function execRawBatch(client, statements, batchSize, options) {
  return __awaiter(this, void 0, void 0, function () {
    var arr;
    return __generator(this, function (_a) {
      switch (_a.label) {
        case 0:
          if (!(statements.length > batchSize)) return [3, 2];
          arr = [];
          while (statements.length !== 0) {
            if (statements.length > batchSize) {
              arr.push(client.batch(statements.splice(0, batchSize), options ? options : { prepare: true }));
            }
            else {
              arr.push(client.batch(statements.splice(0, statements.length), options ? options : { prepare: true }));
            }
          }
          return [4, handleArrayPromises(arr)];
        case 1: return [2, _a.sent()];
        case 2: return [2, client.batch(statements, options ? options : { prepare: true }).then(function (result) {
          return result.rows;
        })];
      }
    });
  });
}
exports.execRawBatch = execRawBatch;
function toLength(r) {
  if (!r || !r.rows) {
    return 0;
  }
  else {
    return r.rowLength;
  }
}
exports.toLength = toLength;
function handleArrayPromises(arrayPromise) {
  return Promise.all(arrayPromise).then(function (result) {
    var arrRealResult = [];
    result.forEach(function (item) {
      if (item.rows) {
        arrRealResult = __spreadArrays(arrRealResult, item.rows);
      }
    });
    return arrRealResult;
  });
}
exports.handleArrayPromises = handleArrayPromises;
function exec(client, sql, args) {
  var p = args && args.length > 0 ? toArray(args) : undefined;
  return client.execute(sql, p, { prepare: true }).then(function (r) { return 1; });
}
exports.exec = exec;
function execRaw(client, sql, args) {
  var p = args && args.length > 0 ? toArray(args) : undefined;
  return client.execute(sql, p, { prepare: true }).then(function (r) {
    if (r.rows.length >= 1) {
      return r.rows[0];
    }
    else {
      return null;
    }
  });
}
exports.execRaw = execRaw;
function query(client, sql, args, m, bools, options) {
  var p = args && args.length > 0 ? toArray(args) : undefined;
  return client.execute(sql, p, options).then(function (r) {
    return handleResults(r.rows, m, bools);
  });
}
exports.query = query;
function queryOne(client, sql, args, m, bools, options) {
  var p = args && args.length > 0 ? toArray(args) : undefined;
  return client.execute(sql, p, options).then(function (r) {
    if (!r.rows || r.rows.length === 0) {
      return null;
    }
    var x = handleResults(r.rows, m, bools);
    return x[0];
  });
}
exports.queryOne = queryOne;
function execScalar(client, sql, args) {
  return queryOne(client, sql, args).then(function (r) {
    if (!r) {
      return null;
    }
    else {
      var keys = Object.keys(r);
      return r[keys[0]];
    }
  });
}
exports.execScalar = execScalar;
function count(client, sql, args) {
  return execScalar(client, sql, args);
}
exports.count = count;
function save(client, obj, table, attrs, buildParam, i) {
  var stm = build_1.buildToSave(obj, table, attrs, buildParam, i);
  if (!stm) {
    return Promise.resolve(0);
  }
  else {
    if (typeof client === 'function') {
      return client(stm.query, stm.params);
    }
    else {
      return exec(client, stm.query, stm.params);
    }
  }
}
exports.save = save;
function saveBatch(client, objs, table, attrs, batchSize, options, buildParam) {
  var stmts = build_1.buildToSaveBatch(objs, table, attrs, buildParam);
  if (!stmts || stmts.length === 0) {
    return Promise.resolve(0);
  }
  else {
    if (typeof client === 'function') {
      return client(stmts);
    }
    else {
      return execBatch(client, stmts, batchSize, options);
    }
  }
}
exports.saveBatch = saveBatch;
exports.insert = save;
exports.insertBatch = saveBatch;
function toArray(arr) {
  if (!arr || arr.length === 0) {
    return [];
  }
  var p = [];
  var l = arr.length;
  for (var i = 0; i < l; i++) {
    if (arr[i] === undefined) {
      p.push(null);
    }
    else {
      p.push(arr[i]);
    }
  }
  return p;
}
exports.toArray = toArray;
function handleResult(r, m, bools) {
  if (r == null || r === undefined || (!m && (!bools || bools.length === 0))) {
    return r;
  }
  handleResults([r], m, bools);
  return r;
}
exports.handleResult = handleResult;
function handleResults(r, m, bools) {
  if (m) {
    var res = mapArray(r, m);
    if (bools && bools.length > 0) {
      return handleBool(res, bools);
    }
    else {
      return res;
    }
  }
  else {
    if (bools && bools.length > 0) {
      return handleBool(r, bools);
    }
    else {
      return r;
    }
  }
}
exports.handleResults = handleResults;
function handleBool(objs, bools) {
  if (!bools || bools.length === 0 || !objs) {
    return objs;
  }
  for (var _i = 0, objs_1 = objs; _i < objs_1.length; _i++) {
    var obj = objs_1[_i];
    for (var _a = 0, bools_1 = bools; _a < bools_1.length; _a++) {
      var field = bools_1[_a];
      var v = obj[field.name];
      if (typeof v !== 'boolean' && v != null && v !== undefined) {
        var b = field.true;
        if (b == null || b === undefined) {
          obj[field.name] = ('1' == v || 'T' == v || 'Y' == v);
        }
        else {
          obj[field.name] = (v == b ? true : false);
        }
      }
    }
  }
  return objs;
}
exports.handleBool = handleBool;
function map(obj, m) {
  if (!m) {
    return obj;
  }
  var mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return obj;
  }
  var obj2 = {};
  var keys = Object.keys(obj);
  for (var _i = 0, keys_1 = keys; _i < keys_1.length; _i++) {
    var key = keys_1[_i];
    var k0 = m[key];
    if (!k0) {
      k0 = key;
    }
    obj2[k0] = obj[key];
  }
  return obj2;
}
exports.map = map;
function mapArray(results, m) {
  if (!m) {
    return results;
  }
  var mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return results;
  }
  var objs = [];
  var length = results.length;
  for (var i = 0; i < length; i++) {
    var obj = results[i];
    var obj2 = {};
    var keys = Object.keys(obj);
    for (var _i = 0, keys_2 = keys; _i < keys_2.length; _i++) {
      var key = keys_2[_i];
      var k0 = m[key];
      if (!k0) {
        k0 = key;
      }
      obj2[k0] = obj[key];
    }
    objs.push(obj2);
  }
  return objs;
}
exports.mapArray = mapArray;
function getFields(fields, all) {
  if (!fields || fields.length === 0) {
    return undefined;
  }
  var ext = [];
  if (all) {
    for (var _i = 0, fields_1 = fields; _i < fields_1.length; _i++) {
      var s = fields_1[_i];
      if (all.includes(s)) {
        ext.push(s);
      }
    }
    if (ext.length === 0) {
      return undefined;
    }
    else {
      return ext;
    }
  }
  else {
    return fields;
  }
}
exports.getFields = getFields;
function buildFields(fields, all) {
  var s = getFields(fields, all);
  if (!s || s.length === 0) {
    return '*';
  }
  else {
    return s.join(',');
  }
}
exports.buildFields = buildFields;
function getMapField(name, mp) {
  if (!mp) {
    return name;
  }
  var x = mp[name];
  if (!x) {
    return name;
  }
  if (typeof x === 'string') {
    return x;
  }
  return name;
}
exports.getMapField = getMapField;
function isEmpty(s) {
  return !(s && s.length > 0);
}
exports.isEmpty = isEmpty;
function version(attrs) {
  var ks = Object.keys(attrs);
  for (var _i = 0, ks_1 = ks; _i < ks_1.length; _i++) {
    var k = ks_1[_i];
    var attr = attrs[k];
    if (attr.version) {
      attr.name = k;
      return attr;
    }
  }
  return undefined;
}
exports.version = version;
var CassandraWriter = (function () {
  function CassandraWriter(client, table, attributes, toDB, buildParam) {
    this.table = table;
    this.attributes = attributes;
    this.write = this.write.bind(this);
    if (typeof client === 'function') {
      this.exec = client;
    }
    else {
      this.client = client;
    }
    this.param = buildParam;
    this.map = toDB;
  }
  CassandraWriter.prototype.write = function (obj) {
    if (!obj) {
      return Promise.resolve(0);
    }
    var obj2 = obj;
    if (this.map) {
      obj2 = this.map(obj);
    }
    var stmt = build_1.buildToSave(obj2, this.table, this.attributes, this.param);
    if (stmt) {
      if (this.exec) {
        return this.exec(stmt.query, stmt.params);
      }
      else {
        return exec(this.client, stmt.query, stmt.params);
      }
    }
    else {
      return Promise.resolve(0);
    }
  };
  return CassandraWriter;
}());
exports.CassandraWriter = CassandraWriter;
var CassandraBatchWriter = (function () {
  function CassandraBatchWriter(client, table, attributes, batchSize, toDB, buildParam) {
    this.table = table;
    this.attributes = attributes;
    this.write = this.write.bind(this);
    if (typeof client === 'function') {
      this.execute = client;
    }
    else {
      this.pool = client;
    }
    this.size = batchSize;
    this.param = buildParam;
    this.map = toDB;
    var x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  CassandraBatchWriter.prototype.write = function (objs) {
    if (!objs || objs.length === 0) {
      return Promise.resolve(0);
    }
    var list = objs;
    if (this.map) {
      list = [];
      for (var _i = 0, objs_2 = objs; _i < objs_2.length; _i++) {
        var obj = objs_2[_i];
        var obj2 = this.map(obj);
        list.push(obj2);
      }
    }
    var stmts = build_1.buildToSaveBatch(list, this.table, this.attributes, this.param);
    if (stmts && stmts.length > 0) {
      if (this.execute) {
        return this.execute(stmts);
      }
      else {
        return execBatch(this.pool, stmts, this.size);
      }
    }
    else {
      return Promise.resolve(0);
    }
  };
  return CassandraBatchWriter;
}());
exports.CassandraBatchWriter = CassandraBatchWriter;
var CassandraChecker = (function () {
  function CassandraChecker(client, service, timeout) {
    this.client = client;
    this.service = service;
    this.timeout = timeout;
    if (!this.timeout) {
      this.timeout = 4200;
    }
    if (!this.service) {
      this.service = 'cassandra';
    }
    this.check = this.check.bind(this);
    this.name = this.name.bind(this);
    this.build = this.build.bind(this);
  }
  CassandraChecker.prototype.check = function () {
    return __awaiter(this, void 0, void 0, function () {
      var obj, promise;
      var _this = this;
      return __generator(this, function (_a) {
        obj = {};
        promise = new Promise(function (resolve, reject) {
          return __awaiter(_this, void 0, void 0, function () {
            var state;
            return __generator(this, function (_a) {
              state = this.client.getState();
              if (state.getConnectedHosts().length > 0) {
                resolve(obj);
              }
              else {
                reject("Client down!");
              }
              return [2];
            });
          });
        });
        if (this.timeout > 0) {
          return [2, promiseTimeOut(this.timeout, promise)];
        }
        else {
          return [2, promise];
        }
        return [2];
      });
    });
  };
  CassandraChecker.prototype.name = function () {
    return this.service;
  };
  CassandraChecker.prototype.build = function (data, err) {
    if (err) {
      if (!data) {
        data = {};
      }
      data['error'] = err;
    }
    return data;
  };
  return CassandraChecker;
}());
exports.CassandraChecker = CassandraChecker;
function promiseTimeOut(timeoutInMilliseconds, promise) {
  return Promise.race([
    promise,
    new Promise(function (resolve, reject) {
      setTimeout(function () {
        reject("Timed out in: " + timeoutInMilliseconds + " milliseconds!");
      }, timeoutInMilliseconds);
    })
  ]);
}
