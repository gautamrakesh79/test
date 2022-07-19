import aws from "aws-sdk";
import fs from "fs";
import { ReadStream } from "fs";
import { readFile } from "fs";


export default class awsUtility {

    constructor(testEnv) {

        switch (testEnv) {
            case "qa":

                console.log("started with qa")
                this.region = process.env.awsRegion;
                this.accessKeyId = process.env.accessKeyId;
                this.secretAccessKey = process.env.secretAccessKey;
                this.sessionToken = process.env.sessionToken;
                break;
            case "anotherEnv":
                this.region = process.env.awsRegion;
                this.accessKeyId = process.env.accessKeyId;
                this.secretAccessKey = process.env.secretAccessKey;
                this.sessionToken = process.env.sessionToken;
                break;
            default:
                this.region = process.env.awsRegion;
                this.accessKeyId = process.env.accessKeyId;
                this.secretAccessKey = process.env.secretAccessKey;
                this.sessionToken = process.env.sessionToken;
        }


        //console.log("Connection string : "+ this.connectionStr)
        try {
            this.s3 = new aws.S3({
                region: this.region,
                accessKeyId: this.accessKeyId,
                secretAccessKey: this.secretAccessKey,
                sessionToken: this.sessionToken
            });
            // this.s3.connect()
        } catch (err) {
            console.log("Error : " + err)
            throw new Error("Error related to S3 connection while connecting : " + err.message.toString())
        }
    }

    async finish() {
        try {
            await this.s3.end()
        } catch (err) {
            throw new Error(`Error on disconnecting from the S3: ${err}, stacktrace: ${err.stack}`)
        }
    }

    async selectS3BucketData(bucketName, keyName, sql, opFileName) {

console.log(keyName);

        let params = {
            Bucket: bucketName,
            Key: keyName,
            ExpressionType: 'SQL',
            Expression: sql,
            InputSerialization: {
                CompressionType: 'GZIP',
                CSV: {
                    FileHeaderInfo: 'IGNORE',
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

console.log(opFileName)
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
