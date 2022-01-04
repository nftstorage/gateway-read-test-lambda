'use strict'

const aws = require('aws-sdk')
const fetch = require('node-fetch')
const { AbortController } = require('node-abort-controller')

const { getIpfsGatewayUrlForCid } = require('./config')
const { logger, elapsed, serializeError } = require('./logging')
const { carStat, inspectCarBlocks } = require('./car')

// TODO: Refine MAX_SIZE_TO_ATTEMPT - currently 100MB
const MAX_SIZE_TO_ATTEMPT = 100 * 1024 * 1024

/**
 * Lambda triggers on S3 write object event.
 *
 * With the know object key, we get the S3 object and inspect the CAR content to get its:
 * - rootCid
 * - structure
 * - size
 *
 * We try to read the CAR from the gateway if:
 * - S3 Object Metadata has a "Complete" structure 
 * - S3 Object has a DagPB encoded root with a known size "acceptable" and the S3 directory
 * for that root CID already has all the expected chunks
 *
 * TODO: Once we have the new CAR chunking in place where last sent CAR has the root, we will be able
 * to simplify the lambda logic to only try to fetch from the gateway when that CAR is received.
 */
async function main(event) {
  const s3 = new aws.S3({ apiVersion: '2006-03-01' })
  const start = process.hrtime.bigint()

  // Get the object from the event and show its content type
  const bucket = event.Records[0].s3.bucket.name
  const key = decodeURIComponent(
    event.Records[0].s3.object.key.replace(/\+/g, ' ')
  )

  const logOpts = { start, bucket, key }

  // TODO: Do we want to filter out namespaces? e.g complete CARs namespace
  const { body, metadata } = await getS3Object(s3, bucket, key, logOpts)
  // @ts-ignore body has different type from AWS SDK
  const { rootCid, structure, size } = await inspectCar(body, metadata, logOpts)

  /**
   * If inserted CAR from the trigger is not complete, let's verify if:
   * - We know the dag size of the CAR (aka we only support read for dagPB for now)
   * - The know size is bigger than the MAX_SIZE_TO_ATTEMPT
   * - We have already received everything in S3
   */
  if (structure !== 'Complete') {
    if (!size) {
      logger.info(
        { elapsed: elapsed(start), path: key },
        `Car with root ${rootCid} does not have a DagPB root and we cannot find the size`
      )
      return { rootCid, structure }
    }

    if (size > MAX_SIZE_TO_ATTEMPT) {
      logger.info(
        { elapsed: elapsed(start), path: key },
        `Car with root ${rootCid} is not complete for object ${key} from bucket ${bucket} and its known size is larger than ${MAX_SIZE_TO_ATTEMPT}`
      )
      return { rootCid, structure }
    }

    const { accumSize } = await getDirectoryStat(s3, bucket, key, logOpts)

    // TODO: accumSize will be bigger than expected DagSize in the end, so we will end up trying
    // to fetch CARs that are still uploading. Can we improve this?
    if (size > accumSize) {
      logger.info(
        { elapsed: elapsed(start), path: key },
        `Car with root ${rootCid} is still not entirely uploaded to bucket ${bucket}`
      )
      return { rootCid, structure }
    }
  }

  // Fetch from gateway
  const gatewayUrl = getIpfsGatewayUrlForCid(rootCid)
  const controller = new AbortController()
  const signal = controller.signal
  const timer = setTimeout(() => controller.abort(), 25000)

  let response
  try {
    response = await fetch(gatewayUrl, { signal })
  } finally {
    clearTimeout(timer)
  }

  if (!response.ok) {
    const err = new Error('Failed to fetch file from gateway')
    logger.error(
      { elapsed: elapsed(start), path: key },
      `Error fetching CAR with root ${rootCid} from gateway: ${serializeError(
        err
      )}`
    )

    throw err
  }

  logger.info(
    { elapsed: elapsed(start), path: key },
    `Car was successfully requested`
  )

  return { rootCid, structure, response }
}

/**
 * @typedef LogOpts
 * @property {bigint} start
 * @property {string} bucket
 * @property {string} key
 */

/**
 * @param {Uint8Array} body
 * @param {Object} metadata
 * @param {LogOpts} logOpts
 */
async function inspectCar (body, metadata, { start, key, bucket }) {
  let rootCid, structure, size
  try {
    const stat = await carStat(body)
    rootCid = stat.rootCid

    logger.debug(
      { elapsed: elapsed(start), path: key },
      `Obtained root cid ${rootCid} for object ${key} from bucket ${bucket}`
    )

    const inspection = await inspectCarBlocks(rootCid, stat.blocksIterator)
    structure = inspection.structure || metadata.structure
    size = inspection.size

  } catch (err) {
    logger.error(
      `Error parsing CAR with ${key} from bucket ${bucket}: ${serializeError(
        err
      )}`
    )
    throw err
  }

  logger.debug(
    { elapsed: elapsed(start), path: key },
    `Obtained structure ${structure} for object ${key} from bucket ${bucket}`
  )

  return {
    rootCid,
    structure,
    size
  }
}

/**
 * @param {aws.S3} s3
 * @param {string} bucket
 * @param {string} key
 * @param {LogOpts} logOpts
 */
async function getS3Object (s3, bucket, key, { start }) {
  let s3Object

  try {
    logger.debug(
      { elapsed: elapsed(start), path: key },
      `Getting object ${key} from bucket ${bucket}`
    )

    s3Object = await s3
      .getObject({
        Bucket: bucket,
        Key: key,
      })
      .promise()
  } catch (err) {
    logger.error(
      `Error getting object ${key} from bucket ${bucket}: ${serializeError(
        err
      )}`
    )
    throw err
  }

  return {
    body: s3Object.Body,
    metadata: s3Object.Metadata
  }
}

/**
 * @param {aws.S3} s3
 * @param {string} bucket
 * @param {string} key
 * @param {LogOpts} logOpts
 */
async function getDirectoryStat (s3, bucket, key, { start }) {
  const prefix = key.replace(/[^\/]*$/, '').slice(0, -1)

  let accumSize
  try {
    logger.debug(
      { elapsed: elapsed(start), path: key },
      `Getting list of objects of prefix ${prefix} from bucket ${bucket}`
    )
    const s3ListObjects = await s3.listObjectsV2({
      Bucket: bucket,
      Prefix: prefix
    }).promise()

    accumSize = s3ListObjects.Contents.reduce((acc, obj) => acc + obj.Size, 0)  
  } catch (err) {
    logger.error(
      `Error listing objects of prefix ${prefix} from bucket ${bucket}: ${serializeError(
        err
      )}`
    )
    throw err
  }

  return {
    accumSize
  }
}

exports.handler = main
