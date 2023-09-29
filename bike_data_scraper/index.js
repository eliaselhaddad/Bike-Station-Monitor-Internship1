const axios = require("axios");
const AWS = require("aws-sdk");

AWS.config.update({ region: "eu-north-1" });
const ddb = new AWS.DynamoDB.DocumentClient();

exports.handler = async (event, context) => {
  const app_id = "d722487b-3b24-451c-87fe-db73219c9568";
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
        TableName: "Cyrille-dscrap-bike-data-table",
        Item: {
          stationId: StationId,
          timestamp: currentTimestamp,
          ...station,
        },
      };

      await ddb.put(params).promise();
    }
    return `Added ${stations.length} stations to DynamoDB.`;
  } catch (error) {
    throw error;
  }
};
