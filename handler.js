'use strict';

const { Client, Connection } = require("@opensearch-project/opensearch");
const { defaultProvider } = require("@aws-sdk/credential-provider-node");
const aws4 = require("aws4");

module.exports.start = async (event) => {

  const result = await deleteWithRange(process.env.SEARCHINDEX, process.env.DELETEAFTER );
  return {
    statusCode: 200,
    body: JSON.stringify(
      result
    ),
  };

  // Use this code if you don't use the http event with the LAMBDA-PROXY integration
  // return { message: 'Go Serverless v1.0! Your function executed successfully!', event };
};

const createAwsConnector = (credentials, region) => {
  class AmazonConnection extends Connection {
      buildRequestObject(params) {
          const request = super.buildRequestObject(params);
          request.service = 'es';
          request.region = region;
          request.headers = request.headers || {};
          request.headers['host'] = request.hostname;

          return aws4.sign(request, credentials);
      }
  }
  return {
      Connection: AmazonConnection
  };
};

const getClient = async () => {
  try{
    const credentials = await defaultProvider()();
    return new Client({
        ...createAwsConnector(credentials),
        node: process.env.HOST,
    });
  }catch( exception ){
    console.log( exception )
  }
  
}

async function deleteWithRange( searchIndex, deleteAfter ){
  var client = await getClient();
  try{
    const result = await client.deleteByQuery({
        index: searchIndex, 
        body: {
          query: {
            range: {
              updated_at: {
                lte: deleteAfter 
              }
              
            }
          }
        }
    });

    console.log( result )

  }catch( error ){
    console.log( error );
  }

}
