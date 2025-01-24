const BigQuery = require('@google-cloud/bigquery');
var fs = require('fs');

const projectId = "githubsolidityquery";
// const sqlQuery = 'SELECT * FROM `LOL.SO_USERS`' ; 
const sqlQuery = 'SELECT * FROM `LOL.SO_GENDER_USERS`' ; 

// var out_fil = 'NON_EMPTY_LOCATION_SO_USERS.csv'
var out_fil = 'NON_EMPTY_LOCATION_SO_GENDER_USERS.csv'

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
      ID          = row_as_json['Id'];
      CreateDate  = row_as_json['CreateDate'];
      LastAccDate = row_as_json['LastAccessDate'];
      Reputation  = row_as_json['Reputation'];

      Location_   = row_as_json['Location'];
      Location_   = Location_.replace(/,/g, '=')            
      Ups         = row_as_json['UpVotes'];   
      Downs       = row_as_json['DownVotes'];
      Name_       = row_as_json['DisplayName']
      

      data   = ID.toString() + ',' + Name_ + ',' + CreateDate+ ',' + LastAccDate  + ',' + Reputation.toString() + ',' + Location_ + ',' + Ups.toString() + ',' + Downs.toString() + '\n' ;
      fullData = fullData + data ; 
    });

    fs.writeFile(out_fil, fullData, function(err) {
    if(err) {
        return console.log(err);
    }
        console.log("SO+USER data dumped succesfully ... ");
    }); 

  })
  .catch(err => {
    console.error('ERROR:', err);
  });