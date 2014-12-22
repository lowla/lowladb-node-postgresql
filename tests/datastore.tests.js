var chai = require('chai');
var should = chai.should();
chai.use(require('chai-things'));
var sinon = require('sinon');
var _ = require('lodash');
var Datastore = require('../lib/datastore.js').Datastore;
var testUtil = require('./testUtil');
var DbUtil = testUtil.DbUtil;
var Promise = require('bluebird');
var pg = require('pg');
var sql = require('sql');

testUtil.enableLongStackSupport();

var _db;
var _ds;
var _dbName;
var _dbUtil;
var conString = "postgres://postgres:desmond@localhost/LowlaTest";

describe('pgTestDatastore', function () {

  before(function (done) {
    _ds = new Datastore({postgreUrl:conString, logger:new testUtil.NullLogger()});
    _ds.ready.then(function() {
      _dbUtil = new DbUtil();
      _dbUtil.openDatabase(conString).then(function(db){
        _db = db;
        _dbName = _db.database;
        done();
      });
    });
  });

  after(function (done) {
    if (_db) {
      _db.end();
      done();
    }
  });


  var cols = [
    { name: 'pk_one',
      dataType: 'text'},
    { name: 'pk_two',
      dataType: 'int'},
    { name: 'pk_three',
      dataType: 'text'},
    { name: '_version',
      dataType: 'int'},
    { name: 'name',
      dataType: 'text'},
    { name: 'a',
      dataType: 'int'},
    { name: 'b',
      dataType: 'int'},
    { name: 'date',
      dataType: 'date'},
    { name: 'timestamp',
      dataType: 'timestamp'},
    { name:'binaryval',
      dataType: 'bytea'},
    { name:'jsonval',
      dataType: 'json'}
  ];
  var primeKeys = ['pk_one', 'pk_two', 'pk_three'];
  beforeEach(function (done) {
    _dbUtil.removeAllCollections(_db)
      .then(function () {
        return _dbUtil.createTestTable("TestCollection", cols, primeKeys)
          .then(function(table){
            return _dbUtil.createTestTable("TestCollection2", cols, primeKeys)
              .then(function(table){
                _dbUtil.createTestTable("TestCollection3", cols, primeKeys)
                  .then(function(table){
                    done();
                  })
              })
          })
      })
  });

  afterEach(function (done) {
    _dbUtil.removeAllCollections(_db)
      .then(function () {
        return done();
      })
  });

  describe('Special data types', function () {

    var png_buff, txt_buff;
    before(function(){
      return testUtil.readFile('test.png').then(function(filedata) {
        png_buff = filedata;
      }).then(function(){
        return testUtil.readFile('test.txt').then(function(filedata) {
          txt_buff = filedata;
        });
      });
    });

    it('encodes a date (timestamp)', function () {
      var msDate = 132215400000;
      var doc = {pk_one:'one', pk_two:2, pk_three:'3',  a: 1, _version:1, timestamp: new Date(msDate)};
      return _dbUtil.insertDocs(_db, "TestCollection", [doc])
        .then(function () {
          return _dbUtil.findDocs(_db, 'TestCollection', {});
        }).then(function (docs) {
          var d = docs[0];
          d = _ds.encodeSpecialTypes(d);
          var date = d.timestamp;
          date.should.not.be.instanceOf(Date);
          date.should.have.property('_bsonType');
          date.should.have.property('millis');
          date._bsonType.should.equal('Date');
          date.millis.should.equal(msDate);

        });
    });

    it('encodes a date (no time)', function () {
      var adate = new Date(1973, 1, 31);
      var doc = {pk_one:'one', pk_two:2, pk_three:'3',  a: 1, _version:1, date: adate};
      return _dbUtil.insertDocs(_db, "TestCollection", [doc])
        .then(function () {
          return _dbUtil.findDocs(_db, 'TestCollection', {});
        }).then(function (docs) {
          var d = docs[0];
          d = _ds.encodeSpecialTypes(d);
          var date = d.date;
          date.should.not.be.instanceOf(Date);
          date.should.have.property('_bsonType');
          date.should.have.property('millis');
          date._bsonType.should.equal('Date');
          date.millis.should.equal(adate.getTime());
        });
    });

    it('decodes a date', function () {
      var msDate = 132215400000;
      var doc = { _id: '1234', a: 1, _version:1, date: {_bsonType: 'Date', millis: 132215400000 }};
      var d = _ds.decodeSpecialTypes(doc);
      var date = d.date;
      date.should.be.instanceOf(Date);
      date.getTime().should.equal(msDate);
    });

    // N/A: it('encodes embedded docs containing dates', function () {
    // the only type of embedded doc in PostgreSQL is JSON which will convert date types to string


    it('decodes embedded docs containing dates', function () {
      var msDate = 132215400000;
      var dateField = {_bsonType: 'Date', millis: 132215400000 };
      var doc = { _id: '1234', a: 1, _version: 1,
        date: dateField,
        embed1:{ a: 1, date: dateField,
          embed2:{ a: 1, date: dateField,
            embed3:{a: 1, date: dateField}
          }
        },
        end:true
      };
      var d = _ds.decodeSpecialTypes(doc);
      d.date.should.be.instanceOf(Date);
      d.date.getTime().should.equal(msDate);
      d.embed1.date.should.be.instanceOf(Date);
      d.embed1.date.getTime().should.equal(msDate);
      d.embed1.embed2.date.should.be.instanceOf(Date);
      d.embed1.embed2.date.getTime().should.equal(msDate);
      d.embed1.embed2.embed3.date.should.be.instanceOf(Date);
      d.embed1.embed2.embed3.date.getTime().should.equal(msDate);
    });

    it('reads and writes JSON', function () {
      var src = _dbUtil.createDocs("foo", 1);
      var date = new Date();
      src[0].jsonval = { name:'foo', val:['bar','baz'], date:date, binaryval: png_buff};
      return _dbUtil.insertDocs(_db, "TestCollection", src)
        .then(function() {
          return _dbUtil.findDocs(_db, 'TestCollection', {});
        }).then(function(docs){
          docs.length.should.equal(1);
          var doc = docs[0];
          doc.a.should.equal(1);
          doc.b.should.equal(2);
          doc.jsonval.should.eql(JSON.parse(JSON.stringify(src[0].jsonval)));
          doc.name.should.equal('foo1');  //unmodified
          doc._version.should.equal(1);
          src[0].jsonval.name='foo2';
          src[0].jsonval.val[1]='bang';
          src[0].jsonval.binaryval=txt_buff;
          var ops = {
            $set: {
              a: 99,
              b: 5,
              jsonval:src[0].jsonval,
              _version: 2
            }
          };
          var pk = {pk_one:doc.pk_one, pk_two:doc.pk_two, pk_three:doc.pk_three};
          return _ds.updateDocumentByOperations(testUtil.createLowlaId(_dbName, 'TestCollection', pk), docs[0]._version,  ops);
        }).then(function(newDoc){
          newDoc.a.should.equal(99);
          newDoc.b.should.equal(5);
          newDoc.jsonval.should.eql(JSON.parse(JSON.stringify(src[0].jsonval)));
          newDoc.name.should.equal('foo1');  //unmodified
          newDoc._version.should.equal(2);
          return _dbUtil.findDocs(_db, 'TestCollection', {});
        }).then(function(docs) {
          docs.length.should.equal(1);
          docs[0].a.should.equal(99);
          docs[0].b.should.equal(5);
          docs[0].jsonval.should.eql(JSON.parse(JSON.stringify(src[0].jsonval)));
          docs[0].name.should.equal('foo1');  //unmodified
          docs[0]._version.should.equal(2);
        });
    });

    it('reads and writes stringified JSON', function () {
      //confirms that json inserted stringified into a data-type json column comes back as object/nested document.
      var src = _dbUtil.createDocs("foo", 1);
      var date = new Date();
      var jsonObj = { name:'foo', val:['bar','baz'], date:date, binaryval: png_buff};
      src[0].jsonval = JSON.stringify(jsonObj)
      return _dbUtil.insertDocs(_db, "TestCollection", src)
        .then(function() {
          return _dbUtil.findDocs(_db, 'TestCollection', {});
        }).then(function(docs){
          docs.length.should.equal(1);
          var doc = docs[0];
          doc.a.should.equal(1);
          doc.b.should.equal(2);
          doc.jsonval.should.eql(JSON.parse(JSON.stringify(jsonObj)));
          doc.name.should.equal('foo1');  //unmodified
          doc._version.should.equal(1);
          jsonObj.name='foo2';
          jsonObj.val[1]='bang';
          jsonObj.binaryval=txt_buff;
          src[0].jsonval = JSON.stringify(jsonObj)
          var ops = {
            $set: {
              a: 99,
              b: 5,
              jsonval:src[0].jsonval,
              _version: 2
            }
          };
          var pk = {pk_one:doc.pk_one, pk_two:doc.pk_two, pk_three:doc.pk_three};
          return _ds.updateDocumentByOperations(testUtil.createLowlaId(_dbName, 'TestCollection', pk), docs[0]._version,  ops);
        }).then(function(newDoc){
          newDoc.a.should.equal(99);
          newDoc.b.should.equal(5);
          newDoc.jsonval.should.eql(JSON.parse(JSON.stringify(jsonObj)));
          newDoc.name.should.equal('foo1');  //unmodified
          newDoc._version.should.equal(2);
          return _dbUtil.findDocs(_db, 'TestCollection', {});
        }).then(function(docs) {
          docs.length.should.equal(1);
          docs[0].a.should.equal(99);
          docs[0].b.should.equal(5);
          docs[0].jsonval.should.eql(JSON.parse(JSON.stringify(jsonObj)));
          docs[0].name.should.equal('foo1');  //unmodified
          docs[0]._version.should.equal(2);
        });
    });

    it('encodes a binary (text)', function () {
      var txt = 'Encoded String';
      var bin = new Buffer(txt);
      var doc = { _id: '1234', a: 1, _version:1, val: bin};
      var d = _ds.encodeSpecialTypes(doc);
      d.val.encoded.should.equal('RW5jb2RlZCBTdHJpbmc=');
      d.val.encoded.should.equal(bin.toString('base64'));
    });

    it('decodes a binary (text)', function () {
      var doc = { _id: '1234', a: 1, _version:1, val: { _bsonType: 'Binary', type: 0, encoded: 'RW5jb2RlZCBTdHJpbmc=' }};
      var d = _ds.decodeSpecialTypes(doc);
      var val = d.val.toString('utf-8');
      val.should.equal('Encoded String');
    });

    it('decodes a binary (image)', function () {
      var doc = { _id: '1234', a: 1, _version: 1, val: { _bsonType: 'Binary', type: 0, encoded: png_buff.toString('base64') }};
      var d = _ds.decodeSpecialTypes(doc);
      d.val.should.be.instanceOf(Buffer)
      d.val.should.eql(png_buff)
      d.val.toString('base64').should.equal( png_buff.toString('base64'));
    });

    it('encodes a binary, buffer (image)', function () {
      var doc = {pk_one:'one', pk_two:2, pk_three:'3',  a: 1, _version:1, binaryval: png_buff};
      return _dbUtil.insertDocs(_db, "TestCollection", [doc])
        .then(function () {
          return _dbUtil.findDocs(_db, 'TestCollection', {});
        }).then(function (docs) {
          var d = _ds.encodeSpecialTypes(docs[0]);
          d.binaryval.should.have.property('_bsonType');
          d.binaryval.should.have.property('encoded');
          d.binaryval._bsonType.should.equal('Binary');
          d.binaryval.encoded.should.equal(png_buff.toString('base64'));
          return true;
        });
    });

    it('encodes a binary created as HEX (image)', function () {
      var hex = '\\x' + png_buff.toString('hex');
      var doc = {pk_one:'one', pk_two:2, pk_three:'3',  a: 1, _version:1, binaryval: hex};
      return _dbUtil.insertDocs(_db, "TestCollection", [doc])
        .then(function () {
          return _dbUtil.findDocs(_db, 'TestCollection', {});
        }).then(function (docs) {
          var d = _ds.encodeSpecialTypes(docs[0]);
          d.binaryval.should.have.property('_bsonType');
          d.binaryval.should.have.property('encoded');
          d.binaryval._bsonType.should.equal('Binary');
          d.binaryval.encoded.should.equal(png_buff.toString('base64'));
          return true;
        });
    });

    it('encode/decode roundtrip - buffer (image)', function () {
      var doc = {pk_one:'one', pk_two:2, pk_three:'3',  a: 1, _version:1, binaryval: png_buff};
      return _dbUtil.insertDocs(_db, "TestCollection", [doc])
        .then(function () {
          return _dbUtil.findDocs(_db, 'TestCollection', {});
        }).then(function (docs) {
          var d = _ds.encodeSpecialTypes(docs[0]);
          d.binaryval.should.have.property('_bsonType');
          d.binaryval.should.have.property('encoded');
          d.binaryval._bsonType.should.equal('Binary');
          d.binaryval.encoded.should.equal(png_buff.toString('base64'));
          return d;
        }).then(function(doc){
          var d2 = _ds.decodeSpecialTypes(doc);
          d2.binaryval.should.be.instanceOf(Buffer)
          d2.binaryval.should.eql(png_buff)
          //return testUtil.writeFile('testPngNew.png', d2.binaryval, 'binary');
        });
    });

    it('encode/decode roundtrip - created as HEX (image)', function () {
      var hex = '\\x' + png_buff.toString('hex');
      var doc = {pk_one:'one', pk_two:2, pk_three:'3',  a: 1, _version:1, binaryval: hex};
      return _dbUtil.insertDocs(_db, "TestCollection", [doc])
        .then(function () {
          return _dbUtil.findDocs(_db, 'TestCollection', {});
        }).then(function (docs) {
          var d = _ds.encodeSpecialTypes(docs[0]);
          d.binaryval.should.have.property('_bsonType');
          d.binaryval.should.have.property('encoded');
          d.binaryval._bsonType.should.equal('Binary');
          d.binaryval.encoded.should.equal(png_buff.toString('base64'));
          return d;
        }).then(function(doc){
          var d2 = _ds.decodeSpecialTypes(doc);
          d2.binaryval.should.be.instanceOf(Buffer)
          d2.binaryval.should.eql(png_buff)
          //return testUtil.writeFile('testPngNew.png', d2.binaryval, 'binary');
        });
    });

    it('decodes embedded docs containing binaries (image)', function () {
      var binField = { _bsonType: 'Binary', type: 0, encoded: png_buff.toString('base64') };
      var doc = { _id: '1234', a: 1, _version: 1,
        val: binField,
        embed1:{ a: 1, val: binField,
          embed2:{ a: 1, val: binField,
            embed3:{a: 1, val: binField}
          }
        },
        end:true
      };
      var d = _ds.decodeSpecialTypes(doc);
      d.val.should.be.instanceOf(Buffer);
      d.val.should.eql(png_buff);
      d.embed1.val.should.be.instanceOf(Buffer);
      d.embed1.val.should.eql(png_buff);
      d.embed1.embed2.val.should.be.instanceOf(Buffer);
      d.embed1.embed2.val.should.eql(png_buff);
      d.embed1.embed2.embed3.val.should.be.instanceOf(Buffer);
      d.embed1.embed2.embed3.val.should.eql(png_buff);
      d.embed1.embed2.embed3.val.toString('base64').should.equal(png_buff.toString('base64'));
    });

    // N/A - it('encodes embedded docs containing binaries (image)', function () {
    // the only type of embedded doc in PostgreSQL is JSON which will reinflate Buffer to number[]

    it('upserts a document with special types', function () {
      var msDate = 132215412345;
      var previousVersion = undefined;  //trigger upsert.
      var pk = {pk_one: 'aaa', pk_two: 7, pk_three: 'ccc'};
      var ops = {
        $set: _.clone(pk)
      };
      ops.$set.a = 98;
      ops.$set.b = 7;
      ops.$set.binaryval = { _bsonType: 'Binary', type: 0, encoded: png_buff.toString('base64') };
      ops.$set.timestamp = {_bsonType: 'Date', millis: msDate }
      ops = _ds.decodeSpecialTypes(ops);
      return _ds.updateDocumentByOperations(testUtil.createLowlaId(_dbName, 'TestCollection', pk), previousVersion, ops)
        .then(function (newDoc) {
          newDoc.a.should.equal(98);
          newDoc.b.should.equal(7);
          newDoc.binaryval.toString('base64').should.equal(png_buff.toString('base64'));
          newDoc.timestamp.should.be.instanceOf(Date);
          newDoc.timestamp.getTime().should.equal(msDate);
          return _dbUtil.findDocs(_db, 'TestCollection', {});

        }).then(function (docs) {
          docs.length.should.equal(1);
          docs[0].a.should.equal(98);
          docs[0].b.should.equal(7);
          docs[0].binaryval.toString('base64').should.equal(png_buff.toString('base64'));
          docs[0].timestamp.should.be.instanceOf(Date);
          docs[0].timestamp.getTime().should.equal(msDate);
        }).then(null, function (err) {
          throw err;
        });
    });

    it('creates a document with special types', function () {
      var msDate = 132215400000;
      var previousVersion = 0;
      var pk = {pk_one: 'aaa', pk_two: 7, pk_three: 'ccc'};
      var ops = {
        $set: _.clone(pk)
      };
      ops.$set.a = 98;
      ops.$set.b = 7;
      ops.$set.binaryval = { _bsonType: 'Binary', type: 0, encoded: png_buff.toString('base64') };
      ops.$set.timestamp = {_bsonType: 'Date', millis: msDate }
      ops = _ds.decodeSpecialTypes(ops);
      return _ds.updateDocumentByOperations(testUtil.createLowlaId(_dbName, 'TestCollection', pk), previousVersion, ops)
        .then(function (newDoc) {
          newDoc.a.should.equal(98);
          newDoc.b.should.equal(7);
          newDoc.binaryval.toString('base64').should.equal(png_buff.toString('base64'));
          newDoc.timestamp.should.be.instanceOf(Date);
          newDoc.timestamp.getTime().should.equal(msDate);
          return _dbUtil.findDocs(_db, 'TestCollection', {});

        }).then(function (docs) {
          docs.length.should.equal(1);
          docs[0].a.should.equal(98);
          docs[0].b.should.equal(7);
          docs[0].binaryval.toString('base64').should.equal(png_buff.toString('base64'));
          docs[0].timestamp.should.be.instanceOf(Date);
          docs[0].timestamp.getTime().should.equal(msDate);
        }).then(null, function (err) {
          throw err;
        });
    });

    it("modifies a document but not it's binary", function () {
      var doc = {pk_one:'one', pk_two:2, pk_three:'3',  a: 1, b:2, _version:1, binaryval: png_buff};
      var pk = {pk_one:doc.pk_one, pk_two:doc.pk_two, pk_three:doc.pk_three};
      return _dbUtil.insertDocs(_db, "TestCollection", [doc])
        .then(function () {
          return _dbUtil.findDocs(_db, 'TestCollection', {});
        }).then(function (docs) {
          docs.length.should.equal(1);
          var d = docs[0];
          d.a.should.equal(1);
          d.b.should.equal(2);
          d._version.should.equal(1);
          d.binaryval.toString('base64').should.equal(png_buff.toString('base64'));
          var ops = {
            $set: {
              a: 99,
              b: 5
            }
          };
          return _ds.updateDocumentByOperations(testUtil.createLowlaId(_dbName, 'TestCollection', pk), docs[0]._version, ops);
        }).then(function (newDoc) {
          newDoc.a.should.equal(99);
          newDoc.b.should.equal(5);
          newDoc.binaryval.toString('base64').should.equal(png_buff.toString('base64'));
          return _dbUtil.findDocs(_db, 'TestCollection', {});
        }).then(function (docs) {

          docs.length.should.equal(1);
          var d = docs[0];
          d.a.should.equal(99);
          d.b.should.equal(5);
          d.binaryval.toString('base64').should.equal(png_buff.toString('base64'));
        });
    });


    it("modifies a document and it's binary", function () {
      var doc = {pk_one: 'one', pk_two: 2, pk_three: '3', a: 1, b: 2, _version: 1, binaryval: png_buff};
      var pk = {pk_one: doc.pk_one, pk_two: doc.pk_two, pk_three: doc.pk_three};
      return _dbUtil.insertDocs(_db, "TestCollection", [doc])
        .then(function () {
          return _dbUtil.findDocs(_db, 'TestCollection', {});
        }).then(function (docs) {
          docs.length.should.equal(1);
          var d = docs[0];
          d.a.should.equal(1);
          d.b.should.equal(2);
          d._version.should.equal(1);
          d.binaryval.toString('base64').should.equal(png_buff.toString('base64'));
          var ops = {
            $set: {
              a: 99,
              b: 5,
              binaryval: {_bsonType: 'Binary', type: 0, encoded: txt_buff.toString('base64')}
            }
          };
          ops = _ds.decodeSpecialTypes(ops);
          return _ds.updateDocumentByOperations(testUtil.createLowlaId(_dbName, 'TestCollection', pk), docs[0]._version, ops);
        }).then(function (newDoc) {
          newDoc.a.should.equal(99);
          newDoc.b.should.equal(5);
          newDoc.binaryval.toString('base64').should.equal(txt_buff.toString('base64'));
          return _dbUtil.findDocs(_db, 'TestCollection', {});
        }).then(function (docs) {

          docs.length.should.equal(1);
          var d = docs[0];
          d.a.should.equal(99);
          d.b.should.equal(5);
          d.binaryval.toString('base64').should.equal(txt_buff.toString('base64'));
        });
    });
  });

  describe('Creates and modifies documents', function () {

    it('creates a document', function () {
      var pk = {pk_one:'aaa', pk_two:7, pk_three:'ccc'};
      var ops = {
        $set: _.clone(pk)
      };
      ops.$set.a=98;
      ops.$set.b=7;
      ops.$set.name='foo';
      var versionPreUpdate = 0;  //new doc; adapter sets 0 if no prev version
      return _ds.updateDocumentByOperations(testUtil.createLowlaId(_dbName, 'TestCollection', pk), versionPreUpdate, ops)
        .then(function (newDoc) {
          newDoc.a.should.equal(98);
          newDoc.b.should.equal(7);
          return _dbUtil.findDocs(_db, 'TestCollection', {});

        }).then(function (docs) {
          docs.length.should.equal(1);
          docs[0].a.should.equal(98);
          docs[0].b.should.equal(7);

        }).then(null, function(err){
          throw err;
        });
    });

    it('upsert - creates a document - no versionPreUpdate', function () {
      var pk = {pk_one:'aaa', pk_two:7, pk_three:'ccc'};
      var ops = {
        $set: _.clone(pk)
      };
      ops.$set.a=98;
      ops.$set.b=7;
      ops.$set.name='foo';
      return _ds.updateDocumentByOperations(testUtil.createLowlaId(_dbName, 'TestCollection', pk), undefined, ops)
        .then(function (newDoc) {
          newDoc.a.should.equal(98);
          newDoc.b.should.equal(7);
          return _dbUtil.findDocs(_db, 'TestCollection', {});

        }).then(function (docs) {
          docs.length.should.equal(1);
          docs[0].a.should.equal(98);
          docs[0].b.should.equal(7);

        }).then(null, function(err){
          throw err;
        });
    });

    it('modifies a document', function () {
      return _dbUtil.insertDocs(_db, "TestCollection", _dbUtil.createDocs("foo", 1))
        .then(function() {
          return _dbUtil.findDocs(_db, 'TestCollection', {});
        }).then(function(docs){
          docs.length.should.equal(1);
          var doc = docs[0];
          doc.a.should.equal(1);
          doc.b.should.equal(2);
          doc.name.should.equal('foo1');  //unmodified
          doc._version.should.equal(1);
          var ops = {
            $set: {
              a: 99,
              b: 5,
              _version: 2   //todo this wasn't in the mongo test. but it's the adapters job, not ds...
            },
            $unset: {
              b:""
            }
          };
          var pk = {pk_one:doc.pk_one, pk_two:doc.pk_two, pk_three:doc.pk_three};
          return _ds.updateDocumentByOperations(testUtil.createLowlaId(_dbName, 'TestCollection', pk), docs[0]._version,  ops);
        }).then(function(newDoc){
          newDoc.a.should.equal(99);
          should.equal(newDoc.b, null);
          newDoc.name.should.equal('foo1');  //unmodified
          newDoc._version.should.equal(2);
          return _dbUtil.findDocs(_db, 'TestCollection', {});
        }).then(function(docs) {
          docs.length.should.equal(1);
          docs[0].a.should.equal(99);
          should.equal(docs[0].b, null);
          docs[0].name.should.equal('foo1');  //unmodified
          docs[0]._version.should.equal(2);
        });
    });

    it('fails on conflicts', function() {
      var pk = {pk_one:'one', pk_two:2, pk_three:'3'};
      var key = testUtil.createLowlaId(_dbName, 'TestCollection', pk);
      return _ds.updateDocumentByOperations(key, undefined, { $set: _.extend(pk, { a: 1, b: 2 }), $inc: { _version: 1 }})
        .then(function(doc) {
          doc._version.should.equal(1);
          return _ds.updateDocumentByOperations(key, 1, { $set: _.extend(pk, { a: 2, b: 4 }), $inc: { _version: 1 }})
        })
        .then(function(doc) {
          doc._version.should.equal(2);
          doc.a.should.equal(2);
          doc.b.should.equal(4);
          return _ds.updateDocumentByOperations(key, 1, { $set: _.extend(pk, { a: 22, b: 44 }), $inc: { _version: 1 }});
        })
        .then(function() {
          throw Error('Should not have resolved on conflict doc');
        }, function(err) {
          err.should.deep.equal({isConflict: true});
        });
    });

    it('can force updates on otherwise conflicting ops', function() {
      var pk = {pk_one:'one', pk_two:2, pk_three:'3'};
      var key = testUtil.createLowlaId(_dbName, 'TestCollection', pk);
      return _ds.updateDocumentByOperations(key, undefined, { $set: _.extend(pk, { a: 1, b: 2 }), $inc: { _version: 1 }})
        .then(function(doc) {
          doc._version.should.equal(1);
          return _ds.updateDocumentByOperations(key, 1, { $set: _.extend(pk, { a: 2, b: 4 }), $inc: { _version: 1 }})
        })
        .then(function(doc) {
          doc._version.should.equal(2);
          doc.a.should.equal(2);
          doc.b.should.equal(4);
          return _ds.updateDocumentByOperations(key, 1, { $set: _.extend(pk, { a: 22, b: 44 }), $inc: { _version: 1 }}, true);
        })
        .then(function(doc) {
          doc.a.should.equal(22);
          doc.b.should.equal(44);
          doc._version.should.equal(3);
        }, function(err){
          err.should.not.deep.equal({isConflict: true});
          throw new Error(err);
        });
    });

    it('deletes a document', function () {
      return _dbUtil.insertDocs(_db, "TestCollection", _dbUtil.createDocs("foo", 1))
        .then(function() {
          return _dbUtil.findDocs(_db, 'TestCollection', {});
        }).then(function(docs){
          docs.length.should.equal(1);
          var doc = docs[0]
          doc.name.should.equal('foo1');
          var pk = {pk_one:doc.pk_one, pk_two:doc.pk_two, pk_three:doc.pk_three};
          return _ds.removeDocument(testUtil.createLowlaId(_dbName, 'TestCollection', pk))
        }).then(function(numRemoved){
          //numRemoved.should.equal(1);
          return _dbUtil.findDocs(_db, 'TestCollection', {});
        }).then(function(docs) {
          docs.length.should.equal(0);
        });
    });

    it('conflicts if versions do not match', function() {
      var pk = {pk_one:'one', pk_two:2, pk_three:'3'};
      var key = testUtil.createLowlaId(_dbName, 'TestCollection', pk);
      return _ds.updateDocumentByOperations(key, undefined, { $set: _.extend(pk, { a: 1, _version: 5 }) })
        .then(function() {
          return _ds.removeDocument(key, 3);
        })
        .then(function() {
          throw Error('Should not have removed document!');
        }, function(err) {
          err.isConflict.should.equal(true);
        });
    });

  });


  describe('Retrieves documents', function () {

    beforeEach(function (done) {
      _dbUtil.insertDocs(_db, "TestCollection", _dbUtil.createDocs("public.TestCollection_", 10))
        .then(_dbUtil.insertDocs(_db, "TestCollection2", _dbUtil.createDocs("public.TestCollection2_", 10)))
        .then(_dbUtil.insertDocs(_db, "TestCollection3", _dbUtil.createDocs("public.TestCollection3_", 10)))
        .then(function () {
          done();
        }).done();
    });

    it('gets a doc by id', function () {
      var id;
      return _dbUtil.getIds(_db, 'TestCollection')
        .then(function (ids) {
          id=ids[2];
          return _ds.getDocument(testUtil.createLowlaId(_dbName, 'TestCollection', ids[2]));
        })
        .then(function (doc) {
          doc.name.should.equal('public.TestCollection_3');
          doc.a.should.equal(3);
        });
    });

    it('gets all docs from all collections', function () {
      var h = testUtil.createResultHandler();
      h.start();
      return _ds.getAllDocuments(h).then(function (result) {
        h.end();
        result.length.should.equal(3);
        for (var i in result) {
          result[i].sent.should.equal(10);
          ["LowlaTest.public.TestCollection",
            "LowlaTest.public.TestCollection2",
            "LowlaTest.public.TestCollection3"].should.include(result[i].namespace);
        }
        var results = h.getResults();
        var collections = {};
        for (var i = 0; i < results.length; i++) {
          results.length.should.equal(30);
          var lowlaIdComponents = _ds._componentsFromId(results[i].lowlaId);
          collections[lowlaIdComponents.collectionName]=true;
          results[i].doc.name.should.equal(lowlaIdComponents.collectionName + "_" + results[i].doc.a)
        }
        collections["public.TestCollection"].should.be.true;
        collections["public.TestCollection2"].should.be.true;
        collections["public.TestCollection3"].should.be.true;
      });
    });

  });

  describe('Basics', function(){

    it('gets collection names', function(){
      return _dbUtil.insertDocs(_db, "TestCollection", _dbUtil.createDocs("TestCollection_", 1))
        .then(_dbUtil.insertDocs(_db, "TestCollection2", _dbUtil.createDocs("TestCollection2_", 1)))
        .then(_dbUtil.insertDocs(_db, "TestCollection3", _dbUtil.createDocs("TestCollection3_", 1)))
        .then(function () {
          return _ds._getTableNames().then(function(names){
            should.exist(names);
            names.length.should.equal(3);
            names.should.contain('public.TestCollection');
            names.should.contain('public.TestCollection2');
            names.should.contain('public.TestCollection3');
          });
        });
    });

    it('gets a table definition names', function(){
      return _dbUtil.insertDocs(_db, "TestCollection", _dbUtil.createDocs("TestCollection_", 1))
        .then(_dbUtil.insertDocs(_db, "TestCollection2", _dbUtil.createDocs("TestCollection2_", 1)))
        .then(_dbUtil.insertDocs(_db, "TestCollection3", _dbUtil.createDocs("TestCollection3_", 1)))
        .then(function () {
          return _ds._getTableDefinition("TestCollection2").then(function(tableDef){
            should.exist(tableDef);
            tableDef.getName().should.equal('TestCollection2');
            tableDef.getSchema().should.equal('public');

          });
        });
    });

    it('gets a collection promise', function(){
      return _ds._getTableDefinition("TestCollection").then(function(collection){
        should.exist(collection);
        //.db.databaseName.should.equal(_ds.config.db.databaseName);
        collection._name.should.equal('TestCollection');
        collection.select(collection.star()).toQuery().text.should.equal('SELECT "public"."TestCollection".* FROM "public"."TestCollection"');
      });
    });

    it('finds in collection', function(){
      return _dbUtil.insertDocs(_db, "TestCollection", _dbUtil.createDocs("TestCollection_", 3))
        .then(function(){
          return _ds._findInTable('TestCollection', {a:2})
            .then(function(results){
              results.length.should.equal(1);
              results[0].a.should.equal(2);
              results[0].b.should.equal(4);
            });
        })
    });

    it('finds All - gte', function(){
      var docs = [];
      for(var i=1; i<=3; i++) {
        docs.push({pk_one:'pk1', pk_two: i, pk_three:'pk3_'+ i, name: "important", a: i, b: 2*i, _version:1})
      }
      docs.push({pk_one:'pk1', pk_two: i, pk_three:'pk3_'+ i, name: "notimportant", a: 4, b: 8, _version:1})

        return _dbUtil.insertDocs(_db, "TestCollection", docs)
        .then(function(){
          return _ds.findAll('TestCollection', {name:'important', a:{$gte:3}})
            .then(function(results){
              results.length.should.equal(1);
              results[0].a.should.equal(3);
              results[0].b.should.equal(6);
              results[0].name.should.equal('important');
            });
        })
    });

    it('finds All - gt', function(){
      var docs = [];
      for(var i=1; i<=3; i++) {
        docs.push({pk_one:'pk1', pk_two: i, pk_three:'pk3_'+ i, name: "important", a: i, b: 2*i, _version:1})
      }
      docs.push({pk_one:'pk1', pk_two: i, pk_three:'pk3_'+ 4, name: "notimportant", a: 4, b: 8, _version:1})

      return _dbUtil.insertDocs(_db, "TestCollection", docs)
        .then(function(){
          return _ds.findAll('TestCollection', {name:'important', a:{$gt:2}})
            .then(function(results){
              results.length.should.equal(1);
              results[0].a.should.equal(3);
              results[0].b.should.equal(6);
              results[0].name.should.equal('important');
            });
        })
    });

    it('finds All - lt', function(){
      var docs = [];
      for(var i=1; i<=3; i++) {
        docs.push({pk_one:'pk1', pk_two: i, pk_three:'pk3_'+ i, name: "notimportant", a: i, b: 2*i, _version:1})
      }
      docs.push({pk_one:'pk1', pk_two: i, pk_three:'pk3_'+ 4, name: "important", a: 4, b: 8, _version:1})
      docs.push({pk_one:'pk1', pk_two: i, pk_three:'pk3_'+ 5, name: "important", a: 5, b: 10, _version:1})

      return _dbUtil.insertDocs(_db, "TestCollection", docs)
        .then(function(){
          return _ds.findAll('TestCollection', {name:'important', a:{$lt:5}})
            .then(function(results){
              results.length.should.equal(1);
              results[0].a.should.equal(4);
              results[0].b.should.equal(8);
              results[0].name.should.equal('important');
            });
        })
    });

    it('finds All - lte', function(){
      var docs = [];
      for(var i=1; i<=3; i++) {
        docs.push({pk_one:'pk1', pk_two: i, pk_three:'pk3_'+ i, name: "notimportant", a: i, b: 2*i, _version:1})
      }
      docs.push({pk_one:'pk1', pk_two: i, pk_three:'pk3_'+ 4, name: "important", a: 4, b: 8, _version:1})
      docs.push({pk_one:'pk1', pk_two: i, pk_three:'pk3_'+ 5, name: "important", a: 5, b: 10, _version:1})

      return _dbUtil.insertDocs(_db, "TestCollection", docs)
        .then(function(){
          return _ds.findAll('TestCollection', {name:'important', a:{$lte:4}})
            .then(function(results){
              results.length.should.equal(1);
              results[0].a.should.equal(4);
              results[0].b.should.equal(8);
              results[0].name.should.equal('important');
            });
        })
    });

    it('finds All - Unsupported op', function(){
      var docs = [];
      for(var i=1; i<=3; i++) {
        docs.push({pk_one:'pk1', pk_two: i, pk_three:'pk3_'+ i, name: "notimportant", a: i, b: 2*i, _version:1})
      }
      docs.push({pk_one:'pk1', pk_two: i, pk_three:'pk3_'+ 4, name: "important", a: 4, b: 8, _version:1})
      docs.push({pk_one:'pk1', pk_two: i, pk_three:'pk3_'+ 5, name: "important", a: 5, b: 10, _version:1})

      return _dbUtil.insertDocs(_db, "TestCollection", docs)
        .then(function(){
          return _ds.findAll('TestCollection', {name:'important', a:{$andfoo:4}})
            .then(function(results){
              should.not.exist(results);
            }, function(err){
              err.message.should.contain("not supported");
              err.message.should.contain("andfoo");
              return true;
            });
        })
    });

    it('streams a cursor', function(){
      var h = testUtil.createResultHandler();
      h.start();
      return _dbUtil.insertDocs(_db, "TestCollection", _dbUtil.createDocs("TestCollection_", 3))
        .then(function(){
          return _ds._getTableDefinition('TestCollection').then(
            function (table) {
              var oQuery = table.select(table.star());
              var pks =[];
              for (var col in table.columns){
                pks.push(table.columns[col].name);
              }
              return _ds._streamQueryResult(oQuery.toQuery(), 'TestCollection', pks, h);
            }
          ).then(function(res){
              h.end();
              var results = h.getResults();
              results.length.should.equal(3);
              results.should.all.have.property('lowlaId');
              results.should.all.have.property('deleted');
              results.should.all.have.property('doc');
            });
        })
    });

  });

  describe('Error Handling', function(){

    afterEach(function(){
      sinon.sandbox.restore();
    });

    it('handles cb->err in getCollectionNames', function(){
      sinon.sandbox.stub(_ds.config.db, 'query', function(config, values, callback){
        callback(Error("Error loading collectionNames"), null)
      });

      return _dbUtil.insertDocs(_db, "TestCollection", _dbUtil.createDocs("public.TestCollection_", 1))
        .then(_dbUtil.insertDocs(_db, "TestCollection2", _dbUtil.createDocs("public.TestCollection2_", 1)))
        .then(_dbUtil.insertDocs(_db, "TestCollection3", _dbUtil.createDocs("public.TestCollection3_", 1)))
        .then(function () {
          return _ds._getTableNames().then(function(names) {
            should.not.exist(names);
          }, function(err){
            err.message.should.equal('Error loading collectionNames')
          });
        });
    });

    it('catches throw in getCollectionNames', function(){
      sinon.sandbox.stub(_ds.config.db, 'query').throws(Error('Error loading collectionNames'));
      return _dbUtil.insertDocs(_db, "TestCollection", _dbUtil.createDocs("public.TestCollection_", 1))
        .then(_dbUtil.insertDocs(_db, "TestCollection2", _dbUtil.createDocs("public.TestCollection2_", 1)))
        .then(_dbUtil.insertDocs(_db, "TestCollection3", _dbUtil.createDocs("public.TestCollection3_", 1)))
        .then(function () {
          return _ds._getTableNames().then(function(names) {
            should.not.exist(names);
          }, function(err){
            err.message.should.equal('Error loading collectionNames')
          });
        });
    });

    it('handles reject in getCollection', function(){
      sinon.sandbox.stub(_ds, '_getTableKeys', function(schema, table){
        return new Promise(function(resolve, reject){
          reject(new Error('Error getting table keys'));
        });
      });
      return _ds._getTableDefinition("TestCollection").then(function(collection){
        should.not.exist(collection);
      }, function(err){
        err.message.should.equal('Error getting table keys')
      });
    });

    //TODO verify error handling in _getTableKeys and other uncovered functions new to postgre...

    it('catches throw in getCollection', function(){
      sinon.sandbox.stub(_ds.config.db, 'query').throws(Error('Error loading table'));
      return _ds._getTableDefinition("TestCollection").then(function(collection){
        should.not.exist(collection);
      }, function(err){
        err.message.should.equal('Error loading table')
      });
    });

    it('handles cb->err in updateByOperations', function(){

      sinon.sandbox.stub(_ds.config.db, 'query', function(query, callback){
        callback(Error("findAndModify returns callback error"), null)
      });

      var ops = {
        $set: {
          a: 98,
          b: 7,
          pk_one: 'yo',
          pk_two: '2',
          pk_three: 'bob'
        }
      };
      var versionPreUpdate = 0;  //new doc, adapter sets missing versionPreUpdate to 0
      return _ds.updateDocumentByOperations(testUtil.createLowlaId(_dbName, 'TestCollection', "foo"), versionPreUpdate, ops)
        .then(function (newDoc) {
          should.not.exist(newDoc);
        }, function(err){
          err.message.should.equal('findAndModify returns callback error')
        })
    });

    it('catches throw in updateByOperations', function(){
      sinon.sandbox.stub(_ds.config.db, 'query').throws(new Error('findAndModify throws'));
      var ops = {
        $set: {
          a: 98,
          b: 7
        }
      };
      var versionPreUpdate = 0;  //new doc, adapter sets missing versionPreUpdate to 0
      return _ds.updateDocumentByOperations(testUtil.createLowlaId(_dbName, 'TestCollection', 'foo'), versionPreUpdate, ops)
        .then(function (newDoc) {
          should.not.exist(newDoc);
        }, function(err){
          err.message.should.equal('findAndModify throws');
        })
    });

    it('catches throw in removeDocument', function () {
      sinon.sandbox.stub(_ds.config.db, 'query').throws(new Error('remove throws'));
      return _ds.removeDocument(testUtil.createLowlaId(_dbName, 'TestCollection', '123'))
        .then(function(numRemoved){
          should.not.exist(numRemoved);
        }, function(err){
          err.message.should.equal('remove throws');
        })
    });

    it('handles cb->err in removeDocument', function () {
      sinon.sandbox.stub(_ds.config.db, 'query', function(query, callback){
        callback(Error("remove returns callback error"), null)
      });

      return _ds.removeDocument(testUtil.createLowlaId(_dbName, 'TestCollection', '123'))
        .then(function(numRemoved){
          should.not.exist(numRemoved);
        }, function(err){
          err.message.should.equal('remove returns callback error');
        })
    });

    it('catches throw in findInCollection', function(){
      sinon.sandbox.stub(_ds.config.db, 'query').throws(new Error('find throws'));
      return _dbUtil.insertDocs(_db, "TestCollection", _dbUtil.createDocs("TestCollection_", 3))
        .then(function(){
          return _ds._findInTable('TestCollection', {a:2})
            .then(function(cursor){
              should.not.exist(cursor);
            }, function(err){
              err.message.should.equal('find throws');
            });
        })
    });

    it('handles cb->err in findInCollection', function(){
      sinon.sandbox.stub(_ds.config.db, 'query', function(query, callback){
        callback(Error("find returns callback error"), null)
      });
      return _dbUtil.insertDocs(_db, "TestCollection", _dbUtil.createDocs("TestCollection_", 3))
        .then(function(){
          return _ds._findInTable('TestCollection', {a:2})
            .then(function(cursor){
              should.not.exist(cursor);
            }, function(err){
              err.message.should.equal('find returns callback error');
            });
        })
    });

    it('catches throw in streamCursor', function(){
      var h = testUtil.createResultHandler();
      h.start();
      return _dbUtil.insertDocs(_db, "TestCollection", _dbUtil.createDocs("TestCollection_", 3))
        .then(function(){
          return _ds._getTableDefinition('TestCollection').then(
            function (table) {
              var oQuery = table.select(table.star());
              var pks = [];
              for (var col in table.columns) {
                pks.push(table.columns[col].name);
              }
              sinon.sandbox.stub(_ds.config.db, 'query').throws(new Error('cursor.stream error'));

              return _ds._streamQueryResult(oQuery.toQuery(), 'TestCollection', pks, h)   //query, collectionName, pks, docHandler
                .then(function (result) {
                  should.not.exist(result);
                }, function (err) {
                  err.message.should.equal("cursor.stream error");
                });
            });
        })
    });

    it('catches throw in getAllDocuments', function(){
      var h = testUtil.createResultHandler();
      h.start();
      return _dbUtil.insertDocs(_db, "TestCollection", _dbUtil.createDocs("TestCollection_", 3))
        .then(function(){
          sinon.sandbox.stub(_ds, '_streamQueryResult').throws(new Error('cursor.stream error'));
          return _ds.getAllDocuments(h)
            .then(function(result){
              should.not.exist(result);
            }, function(err){
              err.message.should.equal("cursor.stream error");
            });
        })
    });

    it('handles reject in getAllDocuments', function(){
      var h = testUtil.createResultHandler();
      h.start();
      return _dbUtil.insertDocs(_db, "TestCollection", _dbUtil.createDocs("TestCollection_", 3))
        .then(function(){
          sinon.sandbox.stub(_ds, '_streamQueryResult', function(){return new Promise(function(resolve, reject){reject(new Error('findInCollection error'));})});
          return _ds.getAllDocuments(h)
            .then(function(result){
              should.not.exist(result);
            }, function(err){
              err.message.should.equal("findInCollection error");
            });
        })
    });

  });

  describe('upsert tests', function(){

    it("tests CTE generation for upsert", function(){
      var ops = {$set: {pk_one:'pk1', pk_two:2, pk_three:'pk3', a:7, b:8, name:'test'}};
      var pks = ['pk_one', 'pk_two', 'pk_three'];

      var cte = _ds._createUpsertCTE('public', 'FoOo', ops, pks);
      cte.query.should.equal(
        'WITH '
        + 'update_table as ( '
        +    'UPDATE "public"."FoOo" t_upd '
        +    'SET "a" = $4, "b" = $5, "name" = $6 '
        +    'WHERE ( '
        +      't_upd."pk_one" = $1 AND t_upd."pk_two" = $2 AND t_upd."pk_three" = $3 '
        +    ') '
        +    'RETURNING t_upd.* ), '
        + 'insert_table as ( '
        +    'INSERT INTO "public"."FoOo" ( "pk_one", "pk_two", "pk_three", "a", "b", "name" ) '
        +    'SELECT $1, $2, $3, $4, $5, $6 '
        +    'WHERE NOT EXISTS ( '
        +        'SELECT 1 FROM update_table t_upd WHERE ( t_upd."pk_one" = $1 AND t_upd."pk_two" = $2 AND t_upd."pk_three" = $3 ) '
        +    ') '
        +    'RETURNING * '
        + ') '
        + 'SELECT * FROM insert_table UNION ALL SELECT * FROM update_table;'
      )

    });

    it("single non-pk upsert generation - pk first", function(){
      var ops = {$set: {pk_one:'pk1', name:'test'}};
      var pks = ['pk_one'];

      var cte = _ds._createUpsertCTE('public', 'FoOo', ops, pks);
      cte.query.should.equal(
        'WITH '
        + 'update_table as ( '
        +    'UPDATE "public"."FoOo" t_upd '
        +    'SET "name" = $2 '
        +    'WHERE ( '
        +      't_upd."pk_one" = $1 '
        +    ') '
        +    'RETURNING t_upd.* ), '
        + 'insert_table as ( '
        +    'INSERT INTO "public"."FoOo" ( "pk_one", "name" ) '
        +    'SELECT $1, $2 '
        +    'WHERE NOT EXISTS ( '
        +        'SELECT 1 FROM update_table t_upd WHERE ( t_upd."pk_one" = $1 ) '
        +    ') '
        +    'RETURNING * '
        + ') '
        + 'SELECT * FROM insert_table UNION ALL SELECT * FROM update_table;'
      )
    });

    it("single non-pk upsert generation - non-pk first", function(){
      var ops = {$set: {name:'test', pk_one:'pk1'}};
      var pks = ['pk_one'];

      var cte = _ds._createUpsertCTE('public', 'FoOo', ops, pks);
      cte.query.should.equal(
        'WITH '
        + 'update_table as ( '
        +    'UPDATE "public"."FoOo" t_upd '
        +    'SET "name" = $1 '
        +    'WHERE ( '
        +      't_upd."pk_one" = $2 '
        +    ') '
        +    'RETURNING t_upd.* ), '
        + 'insert_table as ( '
        +    'INSERT INTO "public"."FoOo" ( "name", "pk_one" ) '
        +    'SELECT $1, $2 '
        +    'WHERE NOT EXISTS ( '
        +        'SELECT 1 FROM update_table t_upd WHERE ( t_upd."pk_one" = $2 ) '
        +    ') '
        +    'RETURNING * '
        + ') '
        + 'SELECT * FROM insert_table UNION ALL SELECT * FROM update_table;'
      )
    });

    it('creates CTE and test that query succeeds', function () {
      var pk = {pk_one:'aaa', pk_two:7, pk_three:'ccc'};
      var ops = {
        $set: _.clone(pk),
        $inc: {b: 17}
      };
      var ts = new Date();
      ts.setTime(1009299697);
      ops.$set.a=1;
      ops.$set.name='foo';
      ops.$set.timestamp = ts;

      var lowlaId = _ds.idFromComponents(_dbName + ".TestCollection", pk);
      return _ds._getTableDefinition("public.TestCollection")
        .then(function(table) {

          var query = table.select(table.star()).toQuery();
          return _dbUtil.execQuery(query).then(function(result){
            //verify it's empty
            result.rows.length.should.equal(0);

            var q = _ds._createUpsertCTE('public', 'TestCollection', ops, primeKeys);
            return _dbUtil.execQuery(q.query, q.values).then(function (result) {
              result.rows.length.should.equal(1);
              var retdoc = result.rows[0];
              retdoc.a.should.equal(1);
              retdoc.b.should.equal(17);
              retdoc.name.should.equal('foo')
              retdoc.timestamp.should.eql(ts);

              return _dbUtil.execQuery(query).then(function(result) {
                //verify doc created
                result.rows.length.should.equal(1);
                var querydoc = result.rows[0];
                querydoc.a.should.equal(1);
                querydoc.b.should.equal(17);
                querydoc.name.should.equal('foo');
                querydoc.timestamp.should.eql(ts);

                ops.$set.a = 77;
                ops.$set.name = "an update through upsert"
                var newTs = new Date();
                ops.$set.timestamp = newTs;
                q = _ds._createUpsertCTE('public', 'TestCollection', ops, primeKeys);
                return _dbUtil.execQuery(q.query, q.values).then(function (result) {

                  result.rows.length.should.equal(1);
                  var retdoc2 = result.rows[0];
                  retdoc2.a.should.equal(77);
                  retdoc2.b.should.equal(34);
                  retdoc2.name.should.equal('an update through upsert')
                  retdoc2.timestamp.should.eql(newTs);

                  return _dbUtil.execQuery(query).then(function (result) {
                    //verify only one doc created
                    result.rows.length.should.equal(1);
                    var querydoc2 = result.rows[0];
                    querydoc2.a.should.equal(77);
                    querydoc2.b.should.equal(34);
                    querydoc2.name.should.equal('an update through upsert');
                    querydoc2.timestamp.should.eql(newTs);
                    return true;
                  });
                });
              });
            });
          });

          return true;
        })
    });

    it('creates then modifies using _upsert', function () {
      var pk = {pk_one:'aaa', pk_two:7, pk_three:'ccc'};
      var ops = {
        $set: _.clone(pk)
      };
      ops.$set.a=1;
      ops.$set.b=2;
      ops.$set.name='foo';
      var lowlaId = _ds.idFromComponents(_dbName + ".TestCollection", pk);
      return _ds._upsert(lowlaId, ops)
        .then(function(doc){
          doc.a.should.equal(1);
          doc.b.should.equal(2);
          doc.name.should.equal('foo');  //unmodified
          ops.$set.a=8;
          ops.$set.b=9;
          ops.$set.name='bar';

          return _ds._upsert(lowlaId, ops);
        }).then(function(newDoc){
          newDoc.a.should.equal(8);
          newDoc.b.should.equal(9);
          newDoc.name.should.equal('bar');  //unmodified
          return _dbUtil.findDocs(_db, 'TestCollection', {});
        }).then(function(docs) {
          docs.length.should.equal(1);
          docs[0].a.should.equal(8);
          docs[0].b.should.equal(9);
          docs[0].name.should.equal('bar');  //unmodified
        });
    });


    it('inserts an atom via _upsert', function () {
      var ddl = 'CREATE TABLE "lowlaAtom"'
        + ' ('
        + '  _id text NOT NULL,'
        + '  id text NOT NULL,'
        + '  sequence integer,'
        + '  version integer,'
        + '  deleted boolean,'
        + '  "clientNs" text,'
        + '   CONSTRAINT "pk_lowlaAtoms" PRIMARY KEY (_id)'
        + ' )'
        + ' WITH ('
        + '  OIDS=FALSE'
        + ' );'
        + ' ALTER TABLE "lowlaAtom"'
        + ' OWNER TO postgres;'

      return _dbUtil.execQuery(ddl, {}).then(function(result) {
        /*
         { '$set':
         { clientNs: 'lowlaSample.todos',
         version: 1,
         id: 'lowlaSample.todos$eyJpZCI6ImV5SnBaQ0k2SW1KbU5EQmhNak5tTFdJNVlUVXRORE5qWkMwNFl6RXlMV0ZpT1dZd05qZzROamRpTnlKOSJ9',
         sequence: '1',
         deleted: false,
         _id: 'lowlaSample.todos$eyJpZCI6ImV5SnBaQ0k2SW1KbU5EQmhNak5tTFdJNVlUVXRORE5qWkMwNFl6RXlMV0ZpT1dZd05qZzROamRpTnlKOSJ9' } }
         */
        var pk = {_id: 'lowlaSample.todos$eyJpZCI6ImV5SnBaQ0k2SW1KbU5EQmhNak5tTFdJNVlUVXRORE5qWkMwNFl6RXlMV0ZpT1dZd05qZzROamRpTnlKOSJ9' };
        var ops =      { '$set':
        { clientNs: 'lowlaSample.todos',
          version: 1,
          id: 'lowlaSample.todos$eyJpZCI6ImV5SnBaQ0k2SW1KbU5EQmhNak5tTFdJNVlUVXRORE5qWkMwNFl6RXlMV0ZpT1dZd05qZzROamRpTnlKOSJ9',
          sequence: 1,
          deleted: false,
          _id: 'lowlaSample.todos$eyJpZCI6ImV5SnBaQ0k2SW1KbU5EQmhNak5tTFdJNVlUVXRORE5qWkMwNFl6RXlMV0ZpT1dZd05qZzROamRpTnlKOSJ9' } };

        var lowlaId = _ds.idFromComponents(_dbName + ".lowlaAtom", pk);
        return _ds._upsert(lowlaId, ops)
          .then(function (doc) {
            doc.clientNs.should.equal('lowlaSample.todos');
            doc.version.should.equal(1);
            doc.id.should.equal('lowlaSample.todos$eyJpZCI6ImV5SnBaQ0k2SW1KbU5EQmhNak5tTFdJNVlUVXRORE5qWkMwNFl6RXlMV0ZpT1dZd05qZzROamRpTnlKOSJ9');
            doc.sequence.should.equal(1);
            doc.deleted.should.equal(false);
            doc._id.should.equal('lowlaSample.todos$eyJpZCI6ImV5SnBaQ0k2SW1KbU5EQmhNak5tTFdJNVlUVXRORE5qWkMwNFl6RXlMV0ZpT1dZd05qZzROamRpTnlKOSJ9');
            ops.$set.version = 8;
            ops.$set.sequence = 9;
            ops.$set.deleted = true;

            return _ds._upsert(lowlaId, ops);
          }).then(function (newDoc) {
            newDoc.version.should.equal(8);
            newDoc.sequence.should.equal(9);
            newDoc.deleted.should.be.true();
            return  _dbUtil.execQuery('SELECT * FROM public."lowlaAtom"', {});
          }).then(function (result) {
            result.rows.length.should.equal(1);
            var queryDoc = result.rows[0];
            queryDoc.version.should.equal(8);
            queryDoc.sequence.should.equal(9);
            queryDoc.deleted.should.be.true();
          });
      });
    });


    it('inserts and updates sequence via upsert', function () {
      var ddl = 'CREATE TABLE "lowlaSequence"'
      + ' ('
        + '   _id text NOT NULL,'
        + '   value integer,'
        + '  CONSTRAINT pk_id PRIMARY KEY (_id)'
        + ' )'
        + ' WITH ('
        + '   OIDS=FALSE'
        + ' );'
        + ' ALTER TABLE "lowlaSequence"'
        + ' OWNER TO postgres;'

      return _dbUtil.execQuery(ddl, {}).then(function(result) {

        var pk = {_id: 'current' };
        var ops =      { '$inc':
        { value: 1 } };

        var lowlaId = _ds.idFromComponents(_dbName + ".lowlaSequence", pk);
        return _ds._upsert(lowlaId, ops)
          .then(function (doc) {
            doc.value.should.equal(1);

            return _ds._upsert(lowlaId, ops);
          }).then(function (newDoc) {
            newDoc.value.should.equal(2);
            return _ds._upsert(lowlaId, ops);
          }).then(function (newDoc2) {
            newDoc2.value.should.equal(3);
            return  _dbUtil.execQuery('SELECT * FROM public."lowlaSequence"', {});
          }).then(function (result) {
            result.rows.length.should.equal(1);
            var queryDoc = result.rows[0];
            queryDoc.value.should.equal(3);
          });
      });
    });


  });


});