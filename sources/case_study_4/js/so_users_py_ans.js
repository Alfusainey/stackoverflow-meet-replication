const BigQuery = require('@google-cloud/bigquery');
var fs = require('fs');

const projectId = "githubsolidityquery";
const sqlQuery = 'SELECT * FROM `LOL.SO_PY_ANS_USERS`' ; 

var out_fil = 'SO_PY_ANS_USERS.csv'

const bigquery = new BigQuery({
  projectId: projectId,
  keyFilename: 'PUT_YOUR_KEY', 
  location: 'US'
});

const options = {
  query: sqlQuery,
  useLegacySql: false, 
};

let job;
var fullData = '' ;


bigquery
  .createQueryJob(options)
  .then(results => {
    job = results[0];
    console.log(`Job ${job.id} started.`);
    return job.promise();
  })
  .then(() => {
    return job.getMetadata();
  })
  .then(metadata => {
    const errors = metadata[0].status.errors;
    if (errors && errors.length > 0) {
      throw errors;
    }
  })
  .then(() => {
    console.log(`Job ${job.id} completed.`);
    return job.getQueryResults();
  })
  .then(results => {
    const rows = results[0];

    rows.forEach(function(row_as_json){
      AccountID   = row_as_json['AccountId'];
      CreateDate  = row_as_json['CreateDate'];
      LastAccDate = row_as_json['LastDate'];
      Reputation  = row_as_json['Reputation'];

      Location_   = row_as_json['Location'];
      Location_   = Location_.replace(/,/g, '=')            
      Ups         = row_as_json['UpVotes'];   
      Downs       = row_as_json['DownVotes'];
      Views_      = row_as_json['Views']      
      

      data   = AccountID.toString() + ',' + CreateDate + ',' + LastAccDate  + ',' + Reputation.toString() + ',' + Location_ + ',' + Ups.toString() + ',' + Downs.toString() + ',' + Views_.toString() + '\n' ;
      fullData = fullData + data ; 
    });

    fs.writeFile(out_fil, fullData, function(err) {
    if(err) {
        return console.log(err);
    }
        console.log("SO+USER+ANSWERS data dumped succesfully ... ");
    }); 

  })
  .catch(err => {
    console.error('ERROR:', err);
  });