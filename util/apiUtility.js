import axios from "axios";
import dateformat from "dateformat";



export default class apiUtility {

    constructor(env,app) {

        switch (env) {
            case "qa":
                switch (app) {
                        case "ExportTrigger":
                        this.baseURL = process.env.triggerExport;
                        this.timeout = 10000;
                        break;

                        case "ExportReader":
                        this.baseURL = process.env.exportHandlerUrl;
                        this.timeout = 10000;
                        break;

                        case "HistoryService":
                            this.baseURL = process.env.historyService;
                            this.timeout = 10000;
                            break;
                
                    default:
                        break;
                }
                
                break;
            case "anotherEnv":
                this.baseURL = process.env.exportHandlerUrl;
                this.timeout = 5000;
                break;
            default:
                this.baseURL = process.env.exportHandlerUrl;
                this.timeout = 5000;
        }
        try {
            this.api =  axios.create({
                baseURL: this.baseURL,
                timeout: this.timeout})
                            // this.s3.connect()
        } catch (err) {
            console.log("Error : " + err)
            throw new Error("Error related to api connection while connecting : " + err.message.toString())
        }
    }

    async getExportHandlerData(billingAccount,zipCode) {
        try {
            const response = await this.api.post('/focus/exportgethandler',{"billingAccount":billingAccount,"zipCode":zipCode,"source":"cms"},{headers:{
                    'Content-Type': 'application/json'
                  }});
                console.log(response.data)
                  return response;
         
        } catch (err) {
            throw new Error(`Error on connectiing  from the api: ${err}, stacktrace: ${err.stack}`)
        }
    }

    async triggerExport(CMS_EXPORT,TOKEN) {
        try {
            const response = await this.api.post('/sia/v3/scheduling/trigger-export',[CMS_EXPORT],{
                headers:{
                'Authorization': TOKEN,    
                'Content-Type': 'application/json'
                  }});
                  console.log(response.status+" , "+response.data.body);
                  return response;
         
        } catch (error) {
            console.log(error)
            return error;
        }
    }

    async checkHistoryToGetEventId() {
        // dateformat(now, 'mmmm%20dd%2Cyyyy')
        try {
          const response = await this.api.get('/history/sia/v3/history/date/July%2014%2C2022', {
            headers:{
              'Content-Type': 'application/json'
            }
          });
        //   console.log(response.data.body);
          return response;
        } catch (ex) {
          console.log( 'Error : could not get data' + ex);
          return ex
        }
      };

    async checkHistoryToGetS3FileName(EventId) {
        try {
          const response = await this.api.get('/history/sia/v3/history/id/'+EventId, {
            headers:{
              'Content-Type': 'application/json'
            }
          });
        //   console.log(response.data.body);
          return response;
        } catch (ex) {
          console.log( 'Error : could not get data' + ex);
          return ex
        }}
        
    async selectS3BucketData(bucketName, keyName, sql, opFileName) {


        let params = {
            Bucket: bucketName,
            Key: keyName,
            ExpressionType: 'SQL',
            Expression: sql,
            InputSerialization: {
                CompressionType: 'GZIP',
                CSV: {
                    // FileHeaderInfo: 'USE',
                    RecordDelimiter: '\n',
                    FieldDelimiter: ','
                }
            },
            OutputSerialization: {
                CSV: {}
            }
        }

        try {
               this.s3.selectObjectContent(params, (err, data) => {
                if (err) {
                    console.log(err)
                    return;
                }
                const eventStream =  data.Payload;


                if (fs.existsSync(opFileName)) {
                    fs.unlinkSync(opFileName)
                }
                console.log("===============Start=======================");

                console.log("S3 Bucket Data");

                  eventStream.on('data', ({ Records, Stats, Progress, Cont, End }) => {
                    if ( Records) {
                        // event.Records.Payload is a buffer containing
                        // a single record, partial records, or multiple records
                        process.stdout.write( Records.Payload.toString());
                        fs.appendFileSync(opFileName,  Records.Payload.toString())


                    } else if (Stats) {
                        console.log(`Processed ${Stats.Details.BytesProcessed} bytes`);

                    } else if (End) {
                        console.log('SelectObjectContent completed');
                        console.log("===============End=======================");
                        return true ;
                    }
                });

                // Handle errors encountered during the API call
                  eventStream.on('error', (err) => {
                    switch (err.name) {
                        // Check against specific error codes that need custom handling
                    }
                });

            })

        } catch (err) {
            console.log("Error : " + err)
            throw new Error("Error related to S3 select  query : " + err.message.toString())
        }
    }




}
