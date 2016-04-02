var AWS = require('aws-sdk');


function DynamoDBHelper(){
    AWS.config.update({region:'us-east-1'});
    this.docClient = new AWS.DynamoDB.DocumentClient();
    
    this.putitem = function putItem(object, tablename, callback){
        var params = {
            TableName: tablename,
            Item: object
        };

        this.dbrequest(this.docClient, 'put', params, function putCallback(err, data) {
            callback(err)
        });
    };
    
    this.queryKey = function QueryByKey(key, tablename, callback){
        var params = {
            TableName: tablename,
            //IndexName: 'Index',
            Key: key
        };
        
        this.dbrequest(this.docClient, 'get', params, function getCallback(err, data) {
            callback(err, data)
        });
    }
    
    //TODO: This is very messy especially with the clone of the putItems function in the "then" promise"
    this.replaceitem = function replaceItem(key, item, tablename, callback){
        var params = {
            Key: {},
            TableName: tablename
        };
        
        params.Key[key] = item[key];
        var req = this.dbrequest
        ,dc = this.docClient;
        new Promise(function(resolve, reject){
            req(dc, 'delete', params, function putCallback(err, data) {
                if(err) {
                    callback(err);
                    reject(err);
                }else{
                    resolve();
                }
            });
        }).then(function(err){
            if(!err){
                var params = {
                    TableName: tablename,
                    Item: item
                }

                req(dc, 'put', params, function putCallback(err, data) {
                    callback(err)
                });
            }
        });        
    }

    this.putitems = function putItems(items, tablename, callback){ 
        var error;
        do{
            if (items.length > 25) { console.log('Lots of items... ', items.length); }
            var params = {
                RequestItems: { },
                ReturnConsumedCapacity: 'TOTAL',
                ReturnItemCollectionMetrics: 'SIZE'
            };
            slicedItems = items.slice(0,24);
            items = items.slice(25);
            params.RequestItems[tablename] = slicedItems;
        
            this.dbrequest(this.docClient, 'batchWrite', params, function sliceCallback(err, data) {
                error = err
            });
        }while(!error && items.length > 25)  
        callback(error);
    };
    
    this.scan = function scan(tablename, callback, attributes, scanfilter){
        var params = {
            TableName : tablename,
            AttributesToGet: attributes,
            ScanFilter: scanfilter
        };
        var dbreq = this.dbrequest,
        dc = this.docClient;
        this.dbrequest(this.docClient, 'scan', params, function scanCallback(err, data) {
            if (data.LastEvaluatedKey) {
                params.ExclusiveStartKey = data.LastEvaluatedKey;
                dbreq(dc, 'scan', params, scanCallback, data.Items);
            } else callback(err, data)
        });

    }
    
    this.dbrequest = function(docClient, command, params, cb, items) {
        (function retry(i) {
            if(command == 'batchWrite') { docClient.batchWrite(params, dbCallback); }
            else if(command == 'put') { docClient.put(params, dbCallback); }
            else if(command == 'delete') { docClient.delete(params, dbCallback); }
            else if(command == 'get') { docClient.get(params, dbCallback); }
            else if(command == 'scan') { docClient.scan(params, dbCallback); }
            
            
            function dbCallback(err, data) {
                if (err) {
                    if (i < 10 && (
                    err.statusCode >= 500 ||
                    err.name == "ProvisionedThroughputExceededException" ||
                    err.name == "ThrottlingException"
                    )) {
                        //console.log("Throttling... ", 50 << i, ' ms')
                        setTimeout(retry, 50 << i, i + 1)
                    }
                    else cb(err)
                } else {
                    Array.prototype.push.apply(data.Items, items);
                    if(data.UnprocessedItems && data.UnprocessedItems.length>0) {
                        params.RequestItems = data.UnprocessedItems;
                        this.dbrequest(command, params, cb);
                    } else {
                        //if(i>0) { console.log('Succesful write after throttle of ', 50 << i, ' ms'); }
                        cb(null, data);
                    }
                }
            }
        })(0)
    };
}

function cleanJSON(object) {
    for (var i in object) {
        if (object[i] == null || object[i] == '') {
            delete object[i];
        } else if (typeof object[i] === 'object') {
            cleanJSON(object[i]);
        }
    }
}

function querykey(key, tablename, callback){
    if(!key){ callback('DynamoDBHelper: Item undefined'); return; }
   
    new DynamoDBHelper().queryKey(key, tablename, callback);
}

function put(objects, tablename, callback){
    if(!objects){ callback('DynamoDBHelper: Item undefined'); return; }
    var helper = new DynamoDBHelper();
    
    if(Array.isArray(objects)){
        var items = [];
        objects.forEach(function(item){
            cleanJSON(item);
            items.push( { PutRequest: { Item: item } } );
        })
        helper.putitems(items, tablename, callback); 
    }else{
        cleanJSON(objects);
        helper.putitem(objects, tablename, callback);
    }
}

function replace(keyName, item, tablename, callback){
    if(!item){ callback('DynamoDBHelper: Item undefined'); return; }
    cleanJSON(item);
    new DynamoDBHelper().replaceitem(keyName, item, tablename, callback);
}

function scan(tablename, callback, attributes, scanfilter){
    new DynamoDBHelper().scan(tablename, callback, attributes, scanfilter);
}


exports.DynamoDBHelper = DynamoDBHelper;
exports.put = put;
exports.scan = scan;
exports.queryKey = querykey;
exports.replace = replace;