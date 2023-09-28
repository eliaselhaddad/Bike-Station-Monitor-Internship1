import axios from "axios";
import { DynamoDB } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocument } from "@aws-sdk/lib-dynamodb";


const region = "eu-north-1";
const dynamo = DynamoDBDocument.from(new DynamoDB({ region }));

export const handler = async (event, context) => {
  const app_id = process.env.APP_ID;
  let url =
    "https://data.goteborg.se/SelfServiceBicycleService/v2.0/Stations/{APPID}?getclosingperiods={CLOSINGPERIODS}&latitude={LATITUDE}&longitude={LONGITUDE}&radius={RADIUS}&format={FORMAT}";

  url = url
    .replace("{APPID}", app_id)
    .replace("{CLOSINGPERIODS}", "true")
    .replace("{LATITUDE}", "57.7089")
    .replace("{LONGITUDE}", "11.9746")
    .replace("{RADIUS}", "30000")
    .replace("{FORMAT}", "json");

  try {
    const response = await axios.get(url);
    const stations = response.data;

    const currentTimestamp = new Date().toISOString();

    for (const station of stations) {
      const StationId = station.Name || station.StationId;
      const params = {
        TableName: process.env.DYNAMODB_TABLE_NAME || "StyrOchStallStations",
        Item: {
          stationId: StationId,
          timestamp: currentTimestamp,
          ...station,
        },
      };
      console.log(`Added ${stations.length} stations to DynamoDB.`);
      await dynamo.put(params);
    }
    return `Added ${stations.length} stations to DynamoDB.`;
  } catch (error) {
    console.log("Error:", error);
    throw error;
  }
};
