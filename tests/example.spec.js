import { expect, test } from "@playwright/test";
import awsUtility from "../util/awsUtility.js"
import apiUtility from "../util/apiUtility.js"
import emrUtility from "../util/emrUtility.js"

import yaml from "js-yaml"
import fs from "fs";
import path from "path";





test.beforeAll(async ({ }) => {
  const env = process.env.ENV || 'qa';
  const assetFile = `assets-${env}.yml`;
  const configData = yaml.load(fs.readFileSync(path.join('assets/', assetFile), 'utf8'));
  const data = {};
  Object.keys(configData).map(k =>
    Object.keys(configData[k]).map(sk => {
      process.env[`${sk}`] = configData[k][sk];
      data[`${sk}`] = configData[k][sk];
    })
  );
});





test.describe.serial('New Todo', () => {
  test.setTimeout(0);
  const accountVsDivisionMap = new Map();
  const accountVsZipMap = new Map();
  const accountVsProfilesMap = new Map();
  const accountSet = new Set();
  let ExportFileName;
  let Result, ExportId, EventId, EMRStatus;

  test('Trigger Export using API', async ({ }) => {
    let api = new apiUtility(process.env.env, "ExportTrigger");
    Result = await api.triggerExport(process.env.cmsExport, process.env.token);
    var test = Result.data.body.match(/ID=(.*)/);
    ExportId = test[1];
    console.log(ExportId);
  })

  test('Monitor EMR status', async ({ }) => {
    let aws = new emrUtility(process.env.env);
    let counter = 30 * 60000;
    do {
      console.log(counter)
      EMRStatus = await aws.getEmrStatusById(ExportId)
      if (EMRStatus == "STARTING") {
        await new Promise(r => setTimeout(r, 60000));
        counter = counter - 60000;
      }
    } while (EMRStatus == "STARTING" && counter > 0);
    EMRStatus = await aws.getEmrStatusById(ExportId)
  })

  test('Get Event ID using History service ', async ({ }) => {
    let api = new apiUtility(process.env.env, "HistoryService");
    let flag = false;
    do {
      try {
        Result = await api.checkHistoryToGetEventId();
        EventId = Result.data.body.list.filter(element => (element.eventType == 'COMPLETE'))[0].eventID
        flag = true;
        console.log("Event ID =" + EventId);

      } catch (error) {
        console.log("Waiting for Event ID")

      }
    } while (!flag);
  })

  test('Get S3 file name History service ', async ({ }) => {

    let api = new apiUtility(process.env.env, "HistoryService");
    let flag = false;
    do {
      Result = await api.checkHistoryToGetS3FileName(EventId);

      try {
        console.log()
        var test = Result.data.body.notes.match(/CMS_(.*).csv.gz/);
        console.log(test[0]);
        ExportFileName = test[0];
        flag = true;
        console.log("Event Id =" + EventId + " Export File Name - " + test[0])


      } catch (error) {
        console.log("Event Id =" + EventId + " Waiting for export file name ")
        await new Promise(r => setTimeout(r, 10000));
      }
    } while (!flag);

  })



  test('Get S3 Data', async ({ }) => {
    console.log("Executing - Get S3 Data and store in CSV file")
    let aws = new awsUtility(process.env.env, "ExportReader");
    let flag = false;
    do {
      try {
        await aws.selectS3BucketData(process.env.exportBucketName, "cms/" + ExportFileName, process.env.sql, ExportFileName.replace('cms/', '').replace('.gz', ''));
        await new Promise(r => setTimeout(r, 10000));
        if (fs.existsSync(ExportFileName.replace('cms/', '').replace('.gz', ''))) {
          flag = true;
        }
      } catch (error) {
        console.log("S3 File not avl")
      }

    } while (!flag);
  });

  test('Get collection details', async ({ }) => {
    console.log("Executing - Fille data collection using CSV file")
    await new Promise(r => setTimeout(r, 5000));

    fs.readFile(ExportFileName.replace('cms/', '').replace('.gz', ''), "utf8", (error, textContent) => {
      try {
        for (let row of textContent.split("\n")) {
          const rowItems = row.split("|");
          accountSet.add(rowItems[0].toString());
          accountVsDivisionMap.set(rowItems[0].toString(), rowItems[1].toString())
          accountVsZipMap.set(rowItems[0].toString(), rowItems[2].toString())
          accountVsProfilesMap.set(rowItems[0].toString(), rowItems[3].toString())
        }
        console.log(accountSet)
        console.log(accountVsDivisionMap)
        console.log(accountVsZipMap)
        console.log(accountVsProfilesMap)
      } catch (error) {

      }
    })
    await new Promise(r => setTimeout(r, 10000));
  });


  test('Validate result', async ({ }) => {
    await new Promise(r => setTimeout(r, 10000));

    let api = new apiUtility(process.env.env, "ExportReader");

    for (const element of accountSet) {
      try {
        console.log("===============Start=======================")
        let response = await api.getExportHandlerData(element, accountVsZipMap.get(element));
        expect(response.data.division_id).toStrictEqual(accountVsDivisionMap.get(element));
        expect(response.data.zip_code).toStrictEqual(accountVsZipMap.get(element));
        expect(response.data.associated_profiles).toStrictEqual(accountVsProfilesMap.get(element));
        console.log("Validation passed for - " + element)
        console.log("===============End=======================")
      } catch (error) { console.log(error) }


    }
  });
});

