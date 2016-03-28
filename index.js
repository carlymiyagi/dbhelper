var AWS = require('aws-sdk');


function DynamoDBHelper(){
    AWS.config.update({region:'us-east-1'});
    this.docClient = new AWS.DynamoDB.DocumentClient();
    
    this.putitem = function putItem(object, tablename, callback, update){
        cleanJSON(object);
        var params = {
            TableName: tablename,
            Item: object
        }

        if(!update){
            this.dbrequest(this.docClient, 'put', params, function sliceCallback(err, data) {
                callback(err)
            });
        }else{
            this.docClient.update(params, callback);
        }
    };

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
    
    this.dbrequest = function(docClient, command, params, cb) {
        (function retry(i) {
            if(command == 'batchWrite') { docClient.batchWrite(params, dbCallback); }
            else if(command == 'put') { docClient.put(params, dbCallback); } 
            
            
            function dbCallback(err, data) {
                if (err) {
                    if (i < 10 && (
                    err.statusCode >= 500 ||
                    err.name == "ProvisionedThroughputExceededException" ||
                    err.name == "ThrottlingException"
                    )) {
                        console.log("Throttling... ", 50 << i, ' ms')
                        setTimeout(retry, 50 << i, i + 1)
                    }

                    else cb(err)
                } else if(data.UnprocessedItems && data.UnprocessedItems.length>0) {
                    params.RequestItems = data.UnprocessedItems;
                    this.dbrequest(command, params, cb);
                } else {
                    if(i>0) { console.log('Succesful write after throttle of ', 50 << i, ' ms'); }
                    cb(null, data);
                }
            }
        })(0)
    };
}

function putCallback(err, data) {
    if (err) {
        //console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
    } else {
        //console.log("Added item:", JSON.stringify(data, null, 2));
    }
};

function cleanJSON(object) {
    for (var i in object) {
        if (object[i] === null || object[i] == '') {
            delete object[i];
        } else if (typeof object[i] === 'object') {
            cleanJSON(object[i]);
        }
    }
}

function put(object, tablename, callback, update){
    var helper = new DynamoDBHelper();
    helper.putitem(object, tablename, callback, update);    
}

function putAll(objects, tablename, callback){
    if(!objects || !Array.isArray(objects)){}
    var helper = new DynamoDBHelper();
    var items = [];
    objects.forEach(function(item){
        cleanJSON(item);
        items.push( { PutRequest: { Item: item } } );
    })
    helper.putitems(items, tablename, callback); 
}

exports.DynamoDBHelper = DynamoDBHelper;
exports.put = put;
exports.putAll = putAll;