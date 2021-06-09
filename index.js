#!/usr/bin/env node
/**
 * Important information
 * https://docs.aws.amazon.com/redshift/latest/mgmt/copy-unload-iam-role.html
 **/ 


// Constant defined for database connection
const athena = new (require('aws-sdk/clients/athena'))();
const pg = require('pg')
const logr = require('@everymundo/simple-logr')
const { AWS_LAMBDA_FUNCTION_NAME } = process.env



// Constants defined for batch processing; to define the period of data to be extracted
const _1hour = 60 * 60 * 1000
const _24hours = 24 * _1hour


// Loads all the partition in table's location on S3 
const reMapPartitions = async (table = 'aggregated_normalized_farenet_001') => {
  const ClientRequestToken = `${table}-${Math.random()}`

  const params = {
    QueryString: `MSCK REPAIR TABLE ${table};`,
    ClientRequestToken,
    QueryExecutionContext: {
      Database: 'farenet_s3'
    },
    ResultConfiguration: {
      OutputLocation: `s3://em-data-analytics-bucket/athena-temp-outputs/${AWS_LAMBDA_FUNCTION_NAME}/add-partition/${table}/results/`
    },
    WorkGroup: 'primary'
  };

// Log to debug; can be monitored in Lambda and CloudWatch
  console.log('Stating query with these params', params)

  const { QueryExecutionId } = await athena.startQueryExecution(params).promise();
  
  console.log({ QueryExecutionId })
  
  const sleep = (n) => new Promise((resolve) => setTimeout(resolve, n))

  for (var i = 5; i--;) {
    const { QueryExecution } = await athena.getQueryExecution({QueryExecutionId}).promise()
    console.log(JSON.stringify({QueryExecution}))

    if (QueryExecution.Status.State !== 'RUNNING') {
      console.log(`QueryExecution.Status.State = ${QueryExecution.Status.State}`)
      console.log(QueryExecution.ResultConfiguration.OutputLocation)

      if (QueryExecution.Status.State !== 'SUCCEEDED') {
        console.error(`QueryExecution.Status.State [${QueryExecution.Status.State}] !== 'SUCCEEDED' for QueryExecutionId=[${QueryExecutionId}]`)
      }
      break
    }

    await sleep(1000)
  }
}

// This calculates the timestamps required to fetch data from Redshift
const getSqlQuery = (NOW = new Date()) => {
  logr.debug({ func: 'getSqlQuery', NOW })
  const now = new Date(NOW)
  const RIGHTNOW = now.toJSON()
  const TO_DATE = RIGHTNOW.substr(0, 10) + ' 00:00:00.000Z'                                         // Previous day search information from midnight, 00:00
  const FROM_DATE = new Date(now.getTime() - _24hours).toJSON().substr(0, 10) + ' 00:00:00.000Z'    // Previous day search information until last minute of the day

  
  // This query aggregates search information across 15 columns stated below
  const sqlQuery = `
      UNLOAD ('
        SELECT
          UPPER(airlineiatacode) AS airlineiatacode,                                   
          farenettimestamp::DATE AS farenetdate,
          UPPER(departureairportiatacode) as departureairportiatacode,
          UPPER(arrivalairportiatacode) AS arrivalairportiatacode,
          departuredate,
          returndate,
          UPPER(flighttype) AS flighttype,
          UPPER(journeytype) AS journeytype,
          UPPER(devicecategory) AS devicecategory,
          UPPER(siteedition) AS siteedition,
          UPPER(outboundfareclass) AS outboundfareclass,
          UPPER(inboundfareclass) AS inboundfareclass,
          
          substring(farenettimestamp, 1, 4)::smallint as year,                       //Custom column created from timestamp for partition
          substring(farenettimestamp, 6, 2)::smallint as month,                      //Custom column created from timestamp for partition
          substring(farenettimestamp, 9, 2)::smallint as day,                        //Custom C created from timestamp for partition
          
          count(distinct farenetid) AS searches,                                     //Column farenetid transformed as Metric - searches
          AVG(CASE
                WHEN totalpriceusd > ''0''
                  AND totalpriceusd < ''100000.00''
                  THEN totalpriceusd
                 ELSE NULL
             END)                    AS "averagepriceusd",                          //Column totalpriceusd transformed as Metric - averagepriceusd
          SUM(passengercount)::INT      AS passengercount,                          //Column passengercount summed as Metric - passengercount
          min(farenettimestamp)::TIMESTAMP     AS "minfarenettimestamp",            //Column farenettimestamp transformed as Metric - minfarenettimestamp 
          max(farenettimestamp)::TIMESTAMP     AS "maxfarenettimestamp"             //Column farenettimestamp transformed as Metric - maxfarenettimestamp
        FROM public.normalized_farenet_001
        WHERE __createdat     >= ''${FROM_DATE}''            AND      __createdat < ''${RIGHTNOW}''        //previously calculated timestamps used to fetch data
        AND farenettimestamp >= ''${FROM_DATE}''::TIMESTAMP AND farenettimestamp < ''${TO_DATE}''::TIMESTAMP
        AND airlineiatacode not in (''AB'',''AY'',''CA'',''LH'',''LO'',''LX'',''OS'',''OU'',''OZ'',''SA'',   //Errored tenants and STAR members excluded
        ''SK'',''SN'',''TK'',''11'',''HAV'',''ey_test'',''jet_test'','''','''')
        AND isusersearch is TRUE                                                                             // Retrieves only user search information
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
        ') 
      TO 's3://em-data-analytics-bucket/athena-temp-outputs/aggregated_normalized_farenet_001/v1/'           //Data Storage Location 
      iam_role 'arn:aws:iam::002584813484:role/DA-reportAggregatedNormalized_farenet-001_unload_role'
      PARQUET 
      PARTITION BY (airlineiatacode,year,month,day) 
      ALLOWOVERWRITE;`

  console.log(sqlQuery)

  return sqlQuery
}

// This part of the code establish the redshift connection and fetchs the required data from Redshift to S3

const queryAggregated = async (NOW = new Date()) => {
  logr.debug({ func: 'queryAggregated', NOW })
  const pgClient = new pg.Client()
  await pgClient.connect()
  // const res = await pgClient.query('SELECT $1::text as message', ['Hello world!'])
  try {
    const sqlQuery = getSqlQuery(NOW)
    // throw new Error(sqlQuery)

    const res = await pgClient.query(sqlQuery)

    logr.info({ func: 'queryAggregated', rows0: res.rows[0] })

    return res.rows
  } catch (e) {
    console.error(e)
  } finally {
    await pgClient.end()
  }
}

// This part of the code calls the function to query the data and remap the partition 

const handler = async (event) => {
  const now = event && event.now ? new Date(event.now) : new Date()
  await queryAggregated(now)
  await reMapPartitions()
}

module.exports = {
  handler
}
