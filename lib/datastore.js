(function (exports) {
  var Promise = require('bluebird');
  var _ = require('lodash');
  var pg = require('pg');
  var QueryStream = require('pg-query-stream');
  var sql = require('sql');
  var util = require('util');
  var defaultOptions = {
    db: false,
    postgreUrl: false,
    logger: console
  };

  //Promise.Promise.longStackTraces();

  // Public APIs
  exports.Datastore = Datastore;
  Datastore.prototype.decodeSpecialTypes = decodeSpecialTypes;
  Datastore.prototype.encodeSpecialTypes = encodeSpecialTypes;
  Datastore.prototype.findAll = findAll;
  Datastore.prototype.getAllDocuments = getAllDocuments;
  Datastore.prototype.getDocument = getDocument;
  Datastore.prototype.idFromComponents = idFromComponents;
  Datastore.prototype.namespaceFromId = namespaceFromId;
  Datastore.prototype.removeDocument = removeDocument;
  Datastore.prototype.updateDocumentByOperations = updateDocumentByOperations;

  Datastore.lowlaIdSeparator = '$';

  // Internal APIs
  Datastore.prototype._componentsFromId = _componentsFromId;
  Datastore.prototype._findInTable = _findInTable;
  Datastore.prototype._getTableDefinition = _getTableDefinition;
  Datastore.prototype._collectionNameToTableIdentifier = _collectionNameToTableIdentifier;
  Datastore.prototype._addColumnsToTableDef = _addColumnsToTableDef;
  Datastore.prototype._getTableNames = _getTableNames;
  Datastore.prototype._streamQueryResult = _streamQueryResult;
  Datastore.prototype._getTableKeys = _getTableKeys;
  Datastore.prototype.execQuery = _execQuery;
  Datastore.prototype._updateOpsToDocument = _updateOpToDocument;
  Datastore.prototype._createUpsertCTE = _createUpsertCTE;
  Datastore.prototype._upsert = _upsert;
  Datastore._b64FromId = _b64FromId;
  Datastore._idFromB64 = _idFromB64;



  return Datastore;

  ///////////////


  function Datastore (options) {
    var datastore = this;
    this.ready = new Promise(function(resolve, reject) {
      try {
        var config = datastore.config = _.extend({}, defaultOptions, options);
        datastore.logger = config.logger;

        if (!config.db) {
          if (!config.postgreUrl) {
            return reject(new Error('Must specify either db or postgre url in LowlaAdapter options'));
          }

          var client = new pg.Client(config.postgreUrl);
          client.connect(function (err) {
            if (err) {
              datastore.logger.error('could not connect to postgres', err);
              return reject(err);
            }

            config.db = client;

            client.query('SELECT current_database();', function (err, result) {
              if (err) {
                datastore.logger.error('error running query', err);
                return reject(err);
              }
              config.databaseName = result.rows[0].current_database;
              datastore.logger.info("Postgre datastore is ready. Database: ", config.databaseName);
              resolve(true);
            })
          });
        }
      }catch(err){
        reject(err);
      }
    });
  }

  function namespaceFromId(lowlaId) {
    var idx = lowlaId.indexOf(Datastore.lowlaIdSeparator);
    if (-1 === idx) {
      throw Error('Invalid LowlaID, missing namespace for ID ' + lowlaId);
    }
    return lowlaId.substring(0, idx);
  }

  function idFromComponents(namespace, datastoreKey) {
    var datastore = this;
    var key = datastoreKey
    if(!_.isObject(key)){
      key = {_id: key};
      datastore.logger.info("Converted non-object key to default object key (idx col: _id): ", key)
    }
    return namespace + Datastore.lowlaIdSeparator + Datastore._b64FromId(key);
  }

  function findAll(collection, query) {
    var datastore = this;
    var dot = collection.indexOf('.');
    if (-1 !== dot) {
      collection = collection.substring(dot + 1);
    }
    return datastore._findInTable(collection, query);
  }

  function updateDocumentByOperations (lowlaId, versionPreUpdate, updateOps) {
    var datastore = this;

    //todo for now we are always upserting on new doc from adapter - once we add versionPreUpdate = 0 we insert or update per below.
    if(undefined === versionPreUpdate || null === versionPreUpdate){
      datastore.logger.info("No versionPreUpdate.  Using UPSERT.")
      return datastore._upsert(lowlaId, updateOps);
    }

    var isNew = (versionPreUpdate===0 ? true : false);
    lowlaId = datastore._componentsFromId(lowlaId, true);  //'silent' param here to allow new docs creation when can't be parsed (b64/json)

    return new Promise(function(resolve, reject){
      datastore._getTableDefinition(lowlaId.collectionName).then(
        function (tableDef) {
          try {
            var doc = datastore._updateOpsToDocument(tableDef, updateOps, isNew);
            var sqlBuilder;
            if(isNew){
              sqlBuilder = tableDef.insert(doc).returning(tableDef.star());
            }else{
              if (!lowlaId.id) {
                return reject(new Error('Datastore.updateDocumentByOperations: id must be specified to update existing document.'));
              }
              var query = lowlaId.id;  //the primary keys
              query._version = versionPreUpdate;
              if(!tableDef.hasColumn('_version')){
                tableDef.addColumn('_version') //if we're querying on it.
              }
              sqlBuilder = tableDef.update(doc).where(query).returning(tableDef.star());
            }
            datastore.config.db.query(sqlBuilder.toQuery(), function(err, result) {
              if (err) {
                return reject(err);
              } else if( 1 > result.rows.length ) {
                return reject({isConflict: true})
              }
              var doc = result.rows[0];

              //if the id has changed, prepare to return new id.
              var idNew = {};
              var primaryKeys = tableDef.lowlaPrimaryKeys;
              for(pk in primaryKeys){
                idNew[primaryKeys[pk]] = doc[primaryKeys[pk]];
              }
              if(!_.isEqual(lowlaId.id, idNew)){
                datastore.logger.info("Document id was updated to: ", idNew);
              }
              lowlaId = datastore.idFromComponents(lowlaId.databaseName + '.' +lowlaId.collectionName, idNew);

              resolve(doc);
              //todo replace the previous line with this block to return result {lowlaId, doc} w/ potentially new id
              //resolve({lowlaId:lowlaIdUpdated, document:doc} ;
            });
          }catch(err){
            reject(err);
          }
        },
        function(err){
          reject(err);
        });
    });
  }

  function removeDocument (lowlaId, versionPreDelete) {
    var datastore = this;
    return new Promise(function (resolve, reject) {
      try{
        lowlaId = datastore._componentsFromId(lowlaId);
        datastore._getTableDefinition(lowlaId.collectionName).then(
          function (tableDef) {
            var sqlBuilder = tableDef.delete().where(lowlaId.id).returning(tableDef.star());
            datastore.config.db.query(sqlBuilder.toQuery(), function(err, result) {
              if (err) {
                return reject(err);
              } else if( 1 > result.rows.length ) {
                return reject({isConflict: true})  //assume deleted ∴ conflict
              }
              resolve(true);
            });
          },
          function(err){
            reject(err);
          });
      }catch(err){
        reject(err);
      }
    });
  };

  function getDocument(lowlaId) {
    var datastore = this;
    return new Promise(function (resolve, reject) {
      try{
        lowlaId = datastore._componentsFromId(lowlaId);
        datastore._getTableDefinition(lowlaId.collectionName).then(
          function (tableDef) {
            var sqlBuilder = tableDef.select(tableDef.star()).where(lowlaId.id).toQuery(); //pass id directly to where(), it is primary key/value pairs
            datastore.config.db.query(sqlBuilder.text, sqlBuilder.values, function (err, result) {
              if (err) {
                return reject(err);
              } else if (1 > result.rows.length) {  //assume deleted
                return reject({isDeleted: true});
              }
              var doc = result.rows[0];
              resolve(doc);
            });
          },
          function(err){
            reject(err);
          });
      }catch(err){
        reject(err);
      }
    });
  }

  function getAllDocuments (docHandler) {
    var datastore = this;
    return datastore._getTableNames().then(function (tableNames) {
      var promises = [];
      tableNames.forEach(function (tableName) {
        promises.push(
          datastore._getTableDefinition(tableName).then(
            function (tableDef) {
              var sqlBuilder = tableDef.select(tableDef.star());
              var primaryKeys =[];
              for (col in tableDef.columns){
                primaryKeys.push(tableDef.columns[col].name);
              }
              return datastore._streamQueryResult(sqlBuilder.toQuery(), tableName, primaryKeys, docHandler);
            }
          )
        );
      });
      return Promise.all(promises) ;
    });
  };

  function encodeSpecialTypes (obj) {
    var datastore = this;
    for (var key in obj) {
      if (obj.hasOwnProperty(key)) {
        var val = obj[key];
        if(val){
          if ('object' === typeof val) {
            if(val instanceof Date){
              obj[key] = {_bsonType:'Date', millis: val.getTime()};
            }
            else if( val instanceof Buffer){
                    obj[key] = { _bsonType: 'Binary', type: 0};
                    obj[key].encoded = val.toString('base64');
            }
            else {
              datastore.encodeSpecialTypes(val);
            }
          }
        }
      }
    }
    return obj;
  }

  function decodeSpecialTypes (obj) {
    var datastore = this;
    for (var key in obj) {
      if (obj.hasOwnProperty(key)) {
        var val = obj[key];
        if ('object' === typeof val  && null!== val) {
          if (val.hasOwnProperty('_bsonType')) {
            switch (val._bsonType) {
              case 'Binary':
                obj[key] = new Buffer(val.encoded, 'base64');
                break;

              case 'Date':
                obj[key] = new Date(parseInt(val.millis));
                break;

              default:
                throw Error('Unexpected BSON type: ' + val._bsonType);
            }
          }
          else {
            datastore.decodeSpecialTypes(val);
          }
        }
      }
    }
    return obj;
  }

  function _findInTable (tableName, query) {
    var datastore = this;

    //these ops map directly to the 'sql' module builder functions
    //more could be added if needed, map Mongo operators to ops defined in node_modules/sql/lib/valueExpressions.js.
    var supportedOps = ['equals', 'gt', 'gte', 'lt', 'lte'];

    return datastore._getTableDefinition(tableName).then(
      function (tableDef) {
        if(!_.isObject(query)){
          throw new Error('Query must be an object.');
        }
        var keys = Object.keys(query);
        for(key in keys){
          if(!tableDef.hasColumn(keys[key])) {
            tableDef.addColumn(keys[key]);
          }
        }
        var sqlBuilder = tableDef.select(tableDef.star());
        if(query){
          var queryOps=[];
          for (prop in query){ //convert special ops (e.g. $gte) into 'sql' function calls
            var queryVal = query[prop];
            var op='equals';
            var val = queryVal;
            if(_.isObject(queryVal)){
              for(p in queryVal){
                if (p.indexOf('$')===0){
                  op = p.substring(1)
                  val = queryVal[p];
                }
              }
            }
            if ( -1 === supportedOps.indexOf(op)){
              throw new Error('_findInTable: Operation "' + op + '" not supported');
            }
            queryOps.push( tableDef[prop][op](val) );  //e.g. push( table.somefield.equals(7) ) [or gte(7), lte, etc.]
          }
          sqlBuilder.where(queryOps);
        }
        return datastore.execQuery(sqlBuilder.toQuery()).then(
          function (result) {
            return result.rows;
          }
        )
      }
    );
  }

  function _streamQueryResult (query, namespace, primaryKeys, docHandler){
    var datastore = this;
    return new Promise(function(resolve, reject){
      try{
        var cnt = 0;
        var deleted = false; //should hold so long as the query isn't collection of ids - where we might then know of deletions.
        var qs = new QueryStream(query.text);
        var stream = datastore.config.db.query(qs);
        stream.on('end', function() {
          resolve({namespace: datastore.config.databaseName +"." + namespace, sent: cnt});
        });
        stream.on('data', function(doc) {
          var id = {};
          for(pk in primaryKeys){
            id[primaryKeys[pk]] = doc[primaryKeys[pk]];
          }
          var lowlaId = datastore.idFromComponents(datastore.config.databaseName + '.' + namespace, id);
          docHandler.write(lowlaId, doc._version, deleted, doc);
          ++cnt;
        });
      } catch (err) {
        reject(err);
      }
    });
  }

  function _updateOpToDocument(tableDef, ops, isNew){
    var datastore = this;

    //todo implement $unset.  Set value to NULL?


    var doc = {};  //we could use _.cloneDeep() here but would need a function to copy Buffer
    for(var prop in ops.$set){
      doc[prop] = ops.$set[prop];
    }
    for(var prop in ops.$unset){
      doc[prop] = null;
    }
    datastore._addColumnsToTableDef(tableDef, doc);

    // $INC - from MongoDb Documentation:
    //  1 - The $inc operator accepts positive and negative values.
    //  2 - If the field does not exist, $inc creates the field and sets the field to the specified value.
    //  3 - Use of the $inc operator on a field with a null value will generate an error.
    //  4 - $inc is an atomic operation within a single document.
    //todo: what if the _version ($INC) field is a sequence generator?  can we supply a value or do we need to to check the column def?
    // Note: it is safe to increment the originalVersion (e.g. the pre-edit version returned by client, since it's part of query on update (failure to match = conflict)
    var incrementOps = ops.$inc;
    for(incrementField in incrementOps){
      datastore._addColumnsToTableDef(tableDef, incrementOps);
      if(isNew){
        doc[incrementField] = incrementOps[incrementField];
      }else{
        doc[incrementField] = tableDef[incrementField].plus(incrementOps[incrementField]);
      }
    }
    return doc;
  }

  function _addColumnsToTableDef(tableDef, doc){
    for(key in doc){
      if(!tableDef.hasColumn(key)){
        tableDef.addColumn(key);
      }
    }
  }

  function _upsert(lowlaId, updateOps){

    var datastore = this;
    var lowlaId = datastore._componentsFromId(lowlaId, true); //silent / no err on bad ID, we'll throw here.

    if (!lowlaId.id) {
      throw new Error('Datastore._upsert: id must be specified');
    }
    return new Promise(function(resolve, reject){
      datastore._getTableDefinition(lowlaId.collectionName).then(
        function (tableDef) {
          try {
            if(!updateOps.$set){
              updateOps.$set = {};
            }
            for(key in lowlaId.id){
              if(undefined === updateOps.$set[key]){
                updateOps.$set[key] = lowlaId.id[key];  //ensure the primary key is in the $set for upsert
              }
            }
            var upsertQueryObj = datastore._createUpsertCTE(tableDef.getSchema(), tableDef.getName(), updateOps, tableDef.lowlaPrimaryKeys);
            datastore.config.db.query(upsertQueryObj.query, upsertQueryObj.values, function(err, result) {
              if(err){
                return reject(err);
              }
              if(result.rows.length<1){
                reject(new Error('Upsert did not result in a modification'));
              }
              if(result.rows.length>1){
                reject(new Error('Upsert resulted in more than one modification'));
              }
              resolve(result.rows[0]);
            });

          }catch(err){
            reject(err);
          }
        },
        function(err){
          reject(err);
        });
    });
  }

  function _componentsFromId(lowlaId, silent) {
    var posDot = lowlaId.indexOf('.');
    var posIdSep = lowlaId.indexOf(Datastore.lowlaIdSeparator);
    if(-1===posDot || -1===posIdSep || (-1!==lowlaId.indexOf(Datastore.lowlaIdSeparator, 1+posIdSep))){
      throw new Error('Internal error: Lowla ID must be in the format database.table#id');
    }
    var dbName = lowlaId.substring(0, posDot);
    var id = null;
    var work = lowlaId;
    if (-1 != posIdSep) {
      id = lowlaId.substring(posIdSep + 1);
      work = lowlaId.substring(0, posIdSep);
    }
    var collectionName = work.substring(posDot + 1);
    var idObj;
    try{
      idObj = Datastore._idFromB64(id);
    }catch(err){
      if(!silent){  //optional silent parameter supports ignoring bad ids sent in push from client (new ids sent back to client)
        throw err;
      }
    }
    return {
      dbName: dbName,
      collectionName: collectionName,
      id: idObj
    };
  }

  function _b64FromId (id){
    var jsonId = JSON.stringify(id);
    var b64Id = new Buffer(jsonId).toString('base64');
    return b64Id;
  };

  function _idFromB64 (b64Id){
    var jsonIdNew = new Buffer(b64Id, 'base64').toString('utf-8');
    var newId = JSON.parse(jsonIdNew);
    return newId;
  };

  function _getTableNames () {
    var datastore = this;
    return new Promise(function(resolve, reject){
      var ret = [];
      var tableDef = sql.define({
        name: 'pg_tables',
        columns: ['schemaname']
      });
      var query = tableDef.select('*').where(tableDef.schemaname.equals('public')).toQuery();   //todo support other schemas
      datastore.config.db.query(query.text, query.values, function (err, result) {
        if (err) {
          return reject(err);
        }
        var tables = result.rows
        for (t in tables) {
          ret.push(tables[t].schemaname + '.' + tables[t].tablename)
        }
        resolve(ret);
      });
    });
  };

  function _execQuery(query){
    var datastore = this;
    return new Promise(function(resolve, reject){
      datastore.config.db.query(query.text, query.values, function(err, result) {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  function _getTableKeys(schema, table){
    var datastore = this;
    //based on the postgresql wiki example, query to get table primary keys
    //https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
    var q = 'SELECT pg_attribute.attname AS name, format_type(pg_attribute.atttypid, pg_attribute.atttypmod) AS type '
      + 'FROM pg_index, pg_class, pg_attribute'
      + (schema ? ', pg_namespace ': ' ')
      + 'WHERE '
      + 'pg_class.oid = \'"' + table +  '"\'::regclass AND '
      + 'indrelid = pg_class.oid AND '
      + (schema ? 'nspname = \'' + schema +  '\' AND ' : '')
      + 'pg_attribute.attrelid = pg_class.oid AND '
      + 'pg_attribute.attnum = any(pg_index.indkey) '
      + 'AND indisprimary ';

    return new Promise(function(resolve, reject){
      datastore.config.db.query(q, function(err, result) {
        if (err) {
          return reject(err);
        }
        resolve(result.rows);
      });
    });

  }

  function _collectionNameToTableIdentifier(collectionName){
    var ret = {}
    if(-1 !== collectionName.indexOf('.')){
      var spl = collectionName.split('.');
      if(2 !== spl.length){
        throw new Error('Invalid table identifier: ' + collectionName)
      }
      ret.schema = spl[0];
      ret.table = spl[1];
    }else{
      ret.schema = 'public'
      ret.table = collectionName;
    }
    return ret;
  }

  function _getTableDefinition (collectionName) {
    var datastore = this;
    var tableId = datastore._collectionNameToTableIdentifier(collectionName);
    return datastore._getTableKeys(tableId.schema, tableId.table).then(function(keys){
      var tableDef = sql.define({schema: tableId.schema, name:tableId.table, columns:keys });
      //augment the table def with the primary keys we looked up now so caller can access after adding columns
      tableDef.lowlaPrimaryKeys  =[];
      for (col in tableDef.columns){
        tableDef.lowlaPrimaryKeys.push(tableDef.columns[col].name);
      }
      return tableDef;
    });
  };

  function _createUpsertCTE(schema, table, ops, primaryKeyNames){
    /*
    Generates a Common Table Expression based SQL statement to approximate UPSERT, returns the statement and array of parameter values
    *** NOTE *** that this may fail in high concurrency situations - calling code should verify the result row and retry or log/handle failure as needed.
    *
    * The query is in the form:
    * CTE 1: update_table --- update operation returning * row
    * CTE 2: insert_table --- insert operation returning * row, conditional on update_table's return row not existing
    * SELECT operation returning the UNION ALL of update_table and insert_table -- which should only include 1 row from successful upsert
    *
    * There is a test that verifies the resulting query text that is a lot more readable in terms of understanding the SQL statement.
    *
    * Note that we do not generate a 'values' table in a CTE for use in both queries, as most examples do, because we're using
    * a parameterized query and without knowledge of/supplying the Postgre data types -- which causes datatype comparison errors
    * presumably related to comparing keys in the where statements (e.g. where( update_table.primaryKeyCol = new_value_table.primaryKeyCol) )
    * Parameter values ( $1, $2, ... ) are simply repeated in each query as needed.
     */

    var qtable = '"' + schema + '"."' + table + '"';

    //doc props/values as arrays for mapping of parameters to $# placeholders and are accessed by index where order is important.
    var cols = [], incs = [],  vals = [];
    for(prop in ops.$set){
      cols.push(prop);
      vals.push(ops.$set[prop]);
    }
    for(prop in ops.$inc){
      incs.push(prop);
      vals.push(ops.$inc[prop]);
    }
    for(pk in primaryKeyNames){
      if(-1===cols.indexOf(primaryKeyNames[pk])) {
        throw new Error('Primary keys must be included in update ops $SET to generate Upsert CTE, missing: ' + primaryKeyNames[pk]);
      }
    }

    var query = '';

    //WHERE statement, identical in both CTEs
    var where = 'WHERE ( ';
    for(i=0; i < primaryKeyNames.length; i++){
      where += 't_upd."' + primaryKeyNames[i] + '" = $' + (1+cols.indexOf(primaryKeyNames[i])) + '' + (i < primaryKeyNames.length - 1 ? ' AND ' : ' ) ');
    }

    // start CTEs
    query+= 'WITH ';

    //update table - SET statement   //TODO support upsert on a table with nothing but primary keys?
    query+= 'update_table as ( UPDATE ' + qtable + ' t_upd SET ';
    var setValSeparator = '';
    for(i=0; i < cols.length; i++){
      if(-1===primaryKeyNames.indexOf(cols[i])) {  //update only non-keys
        query += setValSeparator + '"' + cols[i] + '" = $' + (1+i);
        setValSeparator = ', ';
      }
    }
    for(i=0; i < incs.length; i++){
        query += setValSeparator + '"' + incs[i] + '" = "' + incs[i] + '" + ' + vals[i + cols.length]
        setValSeparator = ', ';
    }
    query += ' ';
    query += where;
    query += 'RETURNING t_upd.* ), ';

    //add the increments to the columns for insert
    cols = cols.concat(incs)

    //insert table
    query += 'insert_table as ( INSERT INTO ' + qtable + ' ( "' + cols.join('", "') + '" ) ';
    query += 'SELECT ';  //note SELECT (vs VALUES) since the WHERE NOT EXIST clause throws a syntax err with VALUES.
    for(i=1; i<=vals.length; i++){
      query+= '$' + i + (i<vals.length ? ', ': ' ');
    }

    //insert table - subquery condition to skip insert if update returned a row
    query += 'WHERE NOT EXISTS ( SELECT 1 FROM update_table t_upd ';
    query += where;
    query += ') ';
    query += 'RETURNING * ) ';

    //done with CTEs, SELECT and return the resulting row (only one should exist)
    query += 'SELECT * FROM insert_table UNION ALL SELECT * FROM update_table;';

    //return the values (parameter) array along with the query
    return {query:query, values:vals};
  }


})(module.exports);