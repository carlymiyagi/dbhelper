var MongoClient = require('mongodb').MongoClient;

var state = {
  db: null,
}

exports.connect = function(server, port, database, callback) {
    if (state.db) return callback();
    var url = 'mongodb://'+server+":"+port+"/"+(database? database : "local");
    MongoClient.connect(url, function(err, db) {
        if (err) return callback(err);
        
        state.db = db;
        callback();
    });
}

exports.get = function() {
  return state.db
}

exports.close = function(done) {
    if (state.db) {
        state.db.close();
    }
}