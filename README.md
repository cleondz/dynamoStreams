# @venzee/dynamoStreams

NodeJs Stream wrappers around DynamoDB requests.

## Installation

`npm i -S @venzee/dynamo_streams`

## Usage

### QueryStream

You can inject DocumentClient instances or DynamoDB instances. The stream uses calls the _query_ method of the injected client with the provided params.
The stream handles pagination and will emit all items found that match the provided query.

See the documentation for [DynamoDB](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#query-property) and [DocumentClient](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#query-property) for more info;

```js
const client = new AWS.DynamoDB.DocumentClient();
const queryStream = require("@venzee/dynamo_streams/query")(client, params);

queryStream.on("data", doStuffWithData).on("error", handleError);
```
