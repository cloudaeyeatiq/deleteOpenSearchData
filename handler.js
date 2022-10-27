'use strict';

const { Client, Connection } = require("@opensearch-project/opensearch");
const { defaultProvider } = require("@aws-sdk/credential-provider-node");
const aws4 = require("aws4");
const AWS = require('aws-sdk');

module.exports.start = async (event) => {

  //const items = await getQueryItems( process.env.SEARCHINDEX, process.env.DELETEAFTER );

  //await saveToS3( items );

  const result = await deleteWithRange(process.env.SEARCHINDEX, process.env.DELETEAFTER );
  return {
    statusCode: 200,
    body: JSON.stringify(
      result
    ),
  }; 
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

//get the items before delete 

async function getQueryItems(searchIndex, deleteAfter){
  var client = await getClient(); 
  try{
    const items = await client.search({
      index: searchIndex, 
      body:{
        query: {
          range:{
            time:{
              lte: deleteAfter
            }
          }
        }
      }
    });

    return items.body.hits ? items.body.hits.hits : []; 

  }catch( error ){
    console.log( error );
    return [] ; 
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
              time: {
                lte: deleteAfter 
              }
              
            }
          }
        }
    });

    console.dir( result );

  }catch( error ){
    console.log( error );
  }

}

async function saveToS3(data) {
  const s3 = new AWS.S3({});
  const today = new Date();

  const filename = 'deletedat-' + today.getDate() + '-' + (today.getMonth() + 1 ) + '-' + today.getFullYear() ; 

  // console.log( filename );
  const bucketName = process.env.AWS_BUCKET_NAME ; 

  const params = {
      Bucket: bucketName,
      Key: filename +'.json',
      Body: JSON.stringify(data),
      ContentType: 'application/json',
  };

  const uploadedDocument = await s3.upload( params ).promise(); 

  console.log( uploadedDocument.Location );

  return uploadedDocument.Location ; 
}
