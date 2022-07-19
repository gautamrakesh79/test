import emr from "aws-sdk";
import fs from "fs";
import { ReadStream } from "fs";
import { readFile } from "fs";


export default class emrUtility {

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
        try {
            this.emr = new emr.EMR({
                region: this.region,
                accessKeyId: this.accessKeyId,
                secretAccessKey: this.secretAccessKey,
                sessionToken: this.sessionToken
            });
            // this.s3.connect()
        } catch (err) {
            console.log("Error : " + err)
            throw new Error("Error related to emr connection while connecting : " + err.message.toString())
        }
    }

    async getEmrStatusById(exportId) {
        const params={
            ClusterId:exportId
        }

        try {
            var state = await (await this.emr.describeCluster(params).promise()).$response.data.Cluster.Status.State;
            console.log("EMR ("+exportId+") Status is "+state)
        } catch (error) {
            console.log(error)
        }
        return state;
    };

    }

        








