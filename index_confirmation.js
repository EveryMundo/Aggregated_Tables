#!/usr/bin/env node
/**
 * Important information
 * https://docs.aws.amazon.com/redshift/latest/mgmt/copy-unload-iam-role.html
 **/ 

const athena = new (require('aws-sdk/clients/athena'))();
const pg = require('pg')
const logr = require('@everymundo/simple-logr')

const _1hour = 60 * 60 * 1000
const _24hours = 24 * _1hour
const { AWS_LAMBDA_FUNCTION_NAME } = process.env

const reMapPartitions = async (table = 'aggregated_normalized_farenet_confirmation_001') => {
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

const getSqlQuery = (NOW = new Date()) => {
  logr.debug({ func: 'getSqlQuery', NOW })
  const now = new Date(NOW)
  const RIGHTNOW = now.toJSON()
  const TO_DATE = RIGHTNOW.substr(0, 10) + ' 00:00:00.000Z'
  // const FROM_DATE = new Date(new Date(TO_DATE) - _1hour*24).toJSON().substr(0, 10) + ' 00:00:00.000Z'
  const FROM_DATE = new Date(now.getTime() - _24hours).toJSON().substr(0, 10) + ' 00:00:00.000Z'

  const sqlQuery = `
      UNLOAD ('
        SELECT UPPER(airlineiatacode)                                AS airlineiatacode,
             farenetconfirmationtimestamp::DATE                      AS farenetconfirmationdate,
             UPPER(departureairportiatacode)                         AS departureairportiatacode,
             UPPER(arrivalairportiatacode)                           AS arrivalairportiatacode,
             departuredate,
             returndate,
             UPPER(flighttype)                                       AS flighttype,
             UPPER(journeytype)                                      AS journeytype,
             UPPER(devicecategory)                                   AS devicecategory,
             UPPER(siteedition)                                      AS siteedition,
             UPPER(outboundfareclass)                                AS outboundfareclass,
             UPPER(inboundfareclass)                                 AS inboundfareclass,
             substring(farenetconfirmationtimestamp, 1, 4)::smallint as year,
             substring(farenetconfirmationtimestamp, 6, 2)::smallint as month,
             substring(farenetconfirmationtimestamp, 9, 2)::smallint as day,
             count(distinct farenetconfirmationid)                   AS bookings,
             SUM(passengercount)::INT                                AS passengercount,
             sum(totalpriceusd)::DECIMAL(24,2)                                      AS "revenue",
             min(farenetconfirmationtimestamp)::TIMESTAMP            AS "minfarenetconfirmationtimestamp",
             max(farenetconfirmationtimestamp)::TIMESTAMP            AS "maxfarenetconfirmationtimestamp"
      FROM public.normalized_farenet_confirmation_001
       WHERE __createdat     >= ''${FROM_DATE}''            AND      __createdat < ''${RIGHTNOW}''
        AND farenetconfirmationtimestamp >= ''${FROM_DATE}''::TIMESTAMP AND farenetconfirmationtimestamp < ''${TO_DATE}''::TIMESTAMP
        AND airlineiatacode not in (''AB'',''AY'',''CA'',''LH'',''LO'',''LX'',''OS'',''OU'',''OZ'',''SA'',''SK'',''SN'',''PC'',''TK'',''11'',''HAV'',''ey_test'',''jet_test'','''','''')
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
        ') 
      TO 's3://em-data-analytics-bucket/reports/aggregated_normalized_farenet_confirmation_001/v1/' 
      iam_role 'arn:aws:iam::002584813484:role/DA-reportAggregatedNormalized_farenet-001_unload_role'
      PARQUET 
      PARTITION BY (airlineiatacode,year,month,day)
      ALLOWOVERWRITE;`

  console.log(sqlQuery)

  return sqlQuery
}


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


const handler = async (event) => {
  const now = event && event.now ? new Date(event.now) : new Date()
  await queryAggregated(now)
  await reMapPartitions()
}

module.exports = {
  handler
}
