(function (exports) {
  var pg = require('pg');
  var sql = require('sql');
  var Promise = require('bluebird');
  var idSeparator = require('../lib/datastore.js').Datastore.lowlaIdSeparator;


  // Public API
  exports.DbUtil = DbUtil;
  exports.createResultHandler = createResultHandler;
  exports.enableLongStackSupport = enableLongStackSupport;
  exports.readFile = readFile;
  exports.idToBase64 = idToBase64;
  exports.createLowlaId = createLowlaId;
  exports.NullLogger = NullLogger;
  exports.TestLogger = TestLogger;

  DbUtil.prototype.openDatabase = openDatabase;
  DbUtil.prototype.getCollection = getCollection;
  DbUtil.prototype.removeCollection = removeCollection;
  DbUtil.prototype.findDocs = findDocs;
  DbUtil.prototype.getIds = getIds;
  DbUtil.prototype.insertDocs = insertDocs;
  DbUtil.prototype.getCollectionNames = getCollectionNames;
  DbUtil.prototype.removeAllCollections = removeAllCollections;
  DbUtil.prototype.createTestTable = createTestTable;
  DbUtil.prototype.createDocs = createDocs;
  DbUtil.prototype.findTable = findTable;
  DbUtil.prototype.execQuery = execQuery;

  //return DbUtil;
  ///////////////

  function DbUtil(){
  }

  function createResultHandler(){
    var startCalled = 0;
    var endCalled = 0;
    var writeCalled = 0;
    var results = [];
    return {
      start: function(){
        endCalled.should.be.lessThan(1);
        ++startCalled
      },
      write: function (lowlaId, version, deleted, doc) {
        startCalled.should.be.greaterThan(0);
        results.push({lowlaId: lowlaId, deleted:deleted, doc:doc});
        ++writeCalled;
      },
      end: function(){
        writeCalled.should.be.greaterThan(0);
        startCalled.should.be.greaterThan(0);
        ++endCalled
      },
      getResults: function(){
        return results;
      }
    }
  }

  function createDocs (rootName, num){
    var docs = [];
    for(i=1; i<=num; i++){
      docs.push({pk_one:'pk1', pk_two: i, pk_three:'pk3_'+ i, name: rootName + i, a: i, b: 2*i, _version:1})
    }
    return docs;
  }

  function enableLongStackSupport(){
    if(Promise.hasOwnProperty('enableLongStackTraces')){
      Promise.enableLongStackTraces();
    }
  }

  function readFile(path){
    return new Promise(function(resolve, reject){
      require('fs').readFile(require('path').resolve(__dirname, path), undefined, function(err, data){
        if(err){
          return reject(err);
        }
        resolve( data );
      });
    });
  }

  function idToBase64(id){
    var jsonId = JSON.stringify(id);
    var b64Id = new Buffer(jsonId).toString('base64');
    return b64Id;
  }

  function createLowlaId(dbName, collectionName, id){
    return dbName + '.' + collectionName + idSeparator + this.idToBase64(id);
  }

//loggers for tests

  var nonOp = function(){};
  function NullLogger(){return {verbose:nonOp, debug:nonOp, info:nonOp, warn:nonOp, error:nonOp}};

  function TestLogger(){
    this.logsByLevel = {};
    this.logs = [];
    var logFunc = function(level, binding){
      return function(){
        if(! binding.logsByLevel[level]){
          binding.logsByLevel[level] = [];
        }
        var entry = {level: level, ts: new Date().getTime(), args: Array.prototype.slice.call(arguments)};
        entry.idxInLevel = binding.logsByLevel[level].push(entry);
        entry.idxInAll = binding.logs.push(entry);
        //console.log(entry);
      };
    };
    this.reset=function(){this.logsByLevel={}, this.logs=[];};
    this.print=function(){
      for(l in this.logs){
        console.log(this.logs[l].ts, this.logs[l].level, this.logs[l].args);
      }
    };
    this.inspect=function(){
      for(l in this.logs){
        console.log(util.inspect(this.logs[l], { showHidden: true, depth: null }));
      }
    };
    this.verbose=logFunc('verbose' , this);
    this.debug=logFunc('debug', this);
    this.info=logFunc('info', this);
    this.warn=logFunc('warn', this);
    this.error=logFunc('error', this);
  }


/// postgre ///

  var _client;
  var _ds;
  var _dbName;
  var testTables = {};


   function openDatabase(url){
    return new Promise(function(resolve, reject){
      _client = new pg.Client(url);
      _client.connect(function(err) {
        if(err){
          reject(err);
        }
        resolve(_client);
      });
    });
  }

  function getCollection(db, collName){
    return new Promise(function(resolve, reject) {
      db.collection(collName, function (err, coll) {
        if (err) {
          return reject(err);
        }
        resolve(coll);
      });
    });
  }

  function removeCollection(db, collName){
    return new Promise(function(resolve, reject) {
      var q = 'DROP TABLE ';
        q += '"' + collName.split('.').join('"."') + '"';

      db.query(q, function (err, result) {
        if (err) {
          reject(err)
        } else {
          resolve(result);
        }
      })
    });
  }

  function findDocs (db, collectionName, query) {
    return findTable(db, collectionName).then(function(table){
      var oQuery = table.select(table.star()).from(table);
      if(query){
        if(Object.keys(query).length > 0) {   //and it's not {}
          oQuery.where(query);
        }
      }
      return execQuery(oQuery.toQuery()).then(function(result){
        return result.rows;
      });
    });
  }

  function getIds(db, collectionName){
    return findDocs(db, collectionName).then(function(docs){
      var ids = [];
      for (i in docs){
        var id = {pk_one:docs[i].pk_one, pk_two:docs[i].pk_two, pk_three:docs[i].pk_three}
        ids.push(id);
      }
      return ids;
    })
  }

  function findTable(db, tableName){
    return getCollectionNames(db).then(function(collnames){
      if(!testTables[tableName]){
        throw new Error('Table was not been created through dbUtil (missing definition): ' + tableName);
      }
      var qName = tableName;
      if(tableName.indexOf('.')===-1){
        qName = 'public.' + tableName;
      }
      if(collnames.indexOf(qName)===-1){
        throw new Error('Table has not been created: ' + qName);
      }
      return testTables[tableName].definition;
    });
  }

  function insertDocs(db, collectionName, docs) {
    return findTable(db, collectionName).then(function(table){
      var keys = Object.keys(docs[0]);
      for(key in keys){
        if(!table.hasColumn(keys[key])) {
          table.addColumn(keys[key]);
        }
      }
      var insertQuery = table.insert( docs ).returning('*').toQuery();
      return execQuery(insertQuery);
    });
  }

  function getCollectionNames(db){
    return new Promise(function(resolve, reject){
      var ret = [];

      var table = sql.define({
        name: 'pg_tables',
        columns: ['schemaname']
      });
      var query = table.select('*').where(table.schemaname.equals('public')).toQuery();
      db.query(query.text, query.values, function (err, result) {
        if (err) {
          return reject(err);
        }
        var tables = result.rows
        //logger.verbose(tables);
        for (t in tables) {
          ret.push(tables[t].schemaname + '.' + tables[t].tablename)
        }
        resolve(ret);
      });
    });
  }

  function removeAllCollections(db){
    return getCollectionNames(db).then(function(collnames){
      var promises = [];
      collnames.forEach(function(collname){
        var p = removeCollection(db, collname);
        promises.push(p);
      });
      return Promise.all(promises).then(function(result){
        testTables = {};
        return result;
      });
    });
  }


  function execQuery(query, values){
    var text;
    if (values){
      text = query;
    }else{
      //assume node-sql query object
      text = query.text;
      values = query.values
    }
    return new Promise(function(resolve, reject){
      _client.query(text, values, function(err, result) {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }


  function createTestTable(tablename, arrColumnDefs, arrTablePrimaryKeys){
    return new Promise(function(resolve, reject){
      //if(tablename.indexOf('.')===-1){
      //  tablename = 'public.' + tablename;
      //}
      if (testTables[tablename]){
        reject("tablename already created!")
      }
      var def = {name: tablename}
      def.columns = arrColumnDefs;
      var table = sql.define(def);
      var query = table.create().toQuery();
      var pkAlter;

      if(arrTablePrimaryKeys){
        pkAlter = 'ALTER TABLE "' + tablename + '" ADD PRIMARY KEY (' + '"' + arrTablePrimaryKeys.join('","') + '"' + ')'
      }
      _client.query(query, function(err, result) {
        if (err) {
          return reject(err);
        }
        if(pkAlter){
          _client.query( pkAlter,  function(err, result) {
            if (err) {
              return reject(err);
            }
            testTables[tablename] = {
              definition:table,
              pks: arrTablePrimaryKeys,
              name: tablename
            };
            resolve(testTables[tablename]);
          })
        }

      });
    });
  }


})(module.exports);