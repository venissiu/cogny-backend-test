const {
  DATABASE_SCHEMA,
  DATABASE_URL,
  SHOW_PG_MONITOR,
  START_DATE,
  FINAL_DATE,
} = require("./config");
const massive = require("massive");
const monitor = require("pg-monitor");
const axios = require("axios");

// Call start
(async () => {
  console.log("main.js: before start");

  const db = await massive(
    {
      connectionString: DATABASE_URL,
      ssl: { rejectUnauthorized: false },
    },
    {
      // Massive Configuration
      scripts: process.cwd() + "/migration",
      allowedSchemas: [DATABASE_SCHEMA],
      whitelist: [`${DATABASE_SCHEMA}.%`],
      excludeFunctions: true,
    },
    {
      // Driver Configuration
      noWarnings: true,
      error: function (err, client) {
        console.log(err);
        //process.emit('uncaughtException', err);
        //throw err;
      },
    }
  );

  if (!monitor.isAttached() && SHOW_PG_MONITOR === "true") {
    monitor.attach(db.driverConfig);
  }

  const execFileSql = async (schema, type) => {
    return new Promise(async (resolve) => {
      const objects = db["user"][type];

      if (objects) {
        for (const [key, func] of Object.entries(objects)) {
          console.log(`executing ${schema} ${type} ${key}...`);
          await func({
            schema: DATABASE_SCHEMA,
          });
        }
      }

      resolve();
    });
  };

  //public
  const migrationUp = async () => {
    return new Promise(async (resolve) => {
      await execFileSql(DATABASE_SCHEMA, "schema");

      //cria as estruturas necessarias no db (schema)
      await execFileSql(DATABASE_SCHEMA, "table");
      await execFileSql(DATABASE_SCHEMA, "view");

      console.log(`reload schemas ...`);
      await db.reload();

      resolve();
    });
  };

  const fetchDataFromApi = async function () {
    const apiLink =
      "https://datausa.io/api/data?drilldowns=Nation&measures=Population";
    try {
      const responseFromApi = await axios.get(apiLink);
      return responseFromApi.data.data;
    } catch (error) {
      console.error(error.message);
    }
  };

  const sumPopulationLocally = async function () {
    const dataFromApi = await fetchDataFromApi();
    const populationSum = dataFromApi
      .filter(
        (element) => element.Year >= START_DATE && element.Year <= FINAL_DATE
      )
      .reduce(
        (previous, currentValue) => previous + currentValue.Population,
        0
      );
    return populationSum;
  };

  try {
    await migrationUp();
    const queryToSumPopulationFromDB = `
        SELECT SUM((doc_record->>'Population')::int) as population_sum
        FROM ${DATABASE_SCHEMA}.api_data
        WHERE 
        (doc_record ->> 'Year')::int BETWEEN ${START_DATE} AND ${FINAL_DATE}`;

    const fetchedDataFromApi = await fetchDataFromApi();

    const processedDataToInsertIntoDB = fetchedDataFromApi.map(
      (arrayElement) => {
        return {
          api_name: "datausa",
          doc_name: arrayElement["Nation"],
          doc_id: arrayElement["ID Nation"],
          doc_record: arrayElement,
        };
      }
    );
    await db[DATABASE_SCHEMA].api_data.insert(processedDataToInsertIntoDB);
    const result2 = await sumPopulationLocally();


    console.log(
      "Population sum made locally:",
      result2.toLocaleString("pt-BR")
    );


    const resultFromQuery = await db.query(queryToSumPopulationFromDB);

    const resultInCorrectShape = parseInt(resultFromQuery[0].population_sum);


    console.log(
      "Population sum made by query to database:",
      resultInCorrectShape.toLocaleString("pt-BR"),
    );
  } catch (e) {
    console.log(e.message);
  } finally {
    console.log("finally");
  }
  console.log("main.js: after start");
})();
