var AWS = require('aws-sdk');

function DynamoDBHelper(){
    AWS.config.update({region:'us-east-1'});
    this.docClient = new AWS.DynamoDB.DocumentClient();
    
    this.putitem = function putItem(object, tablename, update){
        cleanJSON(object);
        var params = {
            TableName: tablename,
            Item: object
        }

        if(!update){
            this.docClient.put(params, putCallback);
        }else{
            this.docClient.update(params, putCallback);
        }
    };
}

function putCallback(err, data) {
    if (err) {
        //console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
    } else {
        //console.log("Added item:", JSON.stringify(data, null, 2));
    }
};

function cleanJSON(test) {
    for (var i in test) {
        if (test[i] === null || test[i] == '') {
            delete test[i];
        } else if (typeof test[i] === 'object') {
            cleanJSON(test[i]);
        }
    }
}

function put(object, tablename, update){
    var helper = new DynamoDBHelper();
    this.putitem(object, tablename, update);    
}

function putAll(objects, tablename, update){
    if(!objects || !Array.isArray(objects)){}
    var helper = new DynamoDBHelper();
    objects.forEach(function(object){
        helper.putitem(object, tablename, update);
    })  
}

exports.DynamoDBHelper = DynamoDBHelper;
exports.put = put;
exports.putAll = putAll;