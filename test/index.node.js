const { serial: test } = require('ava')

const aws = require('aws-sdk-mock')
const { TreewalkCarSplitter } = require('carbites')
const { Blob } = require('@web-std/blob')
const { CarReader } = require('@ipld/car')
const { pack } = require('ipfs-car/pack')
const { packToBlob } = require('ipfs-car/pack/blob')
const { handler } = require('../src/index') // lambda function

const { largerContent } = require('./fixtures/larger-content')

const s3Event = {
  Records: [{
    s3: {
      bucket: {
        name: 'test-bucket'
      },
      object: {
        key: 'raw/bafybeif3hyrtm2skjmldufjid3myfp37sxyqbtz7xe3f2fqyd7ugi33b2a/102702852462097292/ciqg66cgf5pib7qhdczhhzlpb3z2dk5xeo4l6zlel7snh3j4v3g7l5i.car'
      }
    }
  }]
}

test.afterEach(t => {
  aws.restore('S3')
})

test('raw file without metadata', async t => {
  const { root, car } = await packToBlob({
    input: [{
      path: 'file.txt',
      content: new Uint8Array([21, 31]),
    }],
    wrapWithDirectory: false
  })

  const body = new Uint8Array(await car.arrayBuffer())
  aws.mock('S3', 'getObject', { Body: body, Metadata: {} })

  const { rootCid, structure, response } = await handler(s3Event)
  t.deepEqual(root.toString(), rootCid.toString())
  t.deepEqual(structure, 'Complete')
  t.is(response.ok, true, 'Obtained gateway response')
})

test('raw file with complete structure metadata', async t => {
  const { root, car } = await packToBlob({
    input: [{
      path: 'file.txt',
      content: new Uint8Array([21, 31]),
    }],
    wrapWithDirectory: false
  })

  const body = new Uint8Array(await car.arrayBuffer())
  aws.mock('S3', 'getObject', { Body: body, Metadata: {
    structure: 'Complete'
  }})

  const { rootCid, structure, response } = await handler(s3Event)
  t.deepEqual(root.toString(), rootCid.toString())
  t.deepEqual(structure, 'Complete')
  t.is(response.ok, true, 'Obtained gateway response')
})

test('dag pb complete CAR file', async t => {
  const { root, car } = await packToBlob({
    input: [{
      path: 'file.txt',
      content: new Uint8Array([21, 31]),
    }],
    wrapWithDirectory: true
  })

  const body = new Uint8Array(await car.arrayBuffer())
  aws.mock('S3', 'getObject', { Body: body, Metadata: {} })

  const { rootCid, structure, response } = await handler(s3Event)
  t.deepEqual(root.toString(), rootCid.toString())
  t.deepEqual(structure, 'Complete')
  t.is(response.ok, true, 'Obtained gateway response')
})

test('dag pb incomplete CAR file with not all CAR files already stored does not trigger gateway request', async t => {
  const { root, out } = await pack({
    input: [{
      path: 'file.txt',
      content: largerContent,
    }],
    wrapWithDirectory: true
  })
  const car = await CarReader.fromIterable(out)
  const targetSize = 90 // chunk

  const splitter = new TreewalkCarSplitter(car, targetSize)
  const carParts = []
  for await (const car of splitter.cars()) {
    const part = []
    for await (const block of car) {
      part.push(block)
    }
    carParts.push(part)
  }

  // Body with only first part
  const carFile = new Blob(carParts[0], { type: 'application/car' })
  const body = new Uint8Array(await carFile.arrayBuffer())
  aws.mock('S3', 'getObject', { Body: body, Metadata: {} })
  aws.mock('S3', 'listObjectsV2', {
    Contents: []
  })

  const { rootCid, structure, response } = await handler(s3Event)
  t.deepEqual(root.toString(), rootCid.toString())
  t.deepEqual(structure, 'Partial')
  t.is(response, undefined, 'No obtained response')
})

test('dag pb incomplete CAR file with all CAR files already stored triggers gateway request', async t => {
  const { root, out } = await pack({
    input: [{
      path: 'file.txt',
      content: largerContent,
    }],
    wrapWithDirectory: true
  })
  const car = await CarReader.fromIterable(out)
  const targetSize = 90 // chunk

  const splitter = new TreewalkCarSplitter(car, targetSize)
  const carParts = []
  for await (const car of splitter.cars()) {
    const part = []
    for await (const block of car) {
      part.push(block)
    }
    carParts.push(part)
  }

  // Body with only first part
  const carFile = new Blob(carParts[0], { type: 'application/car' })
  const body = new Uint8Array(await carFile.arrayBuffer())
  aws.mock('S3', 'getObject', { Body: body, Metadata: {} })
  aws.mock('S3', 'listObjectsV2', {
    Contents: [
      {
        Key: `raw/${root.toString()}/304288589213073932/ciqace6wkhzula3pb3vh6uesdnmf5ul42ljzjmtqrvjrgh2zlefg4wq.car`,
        LastModified: new Date().toISOString(),
        ETag: '60faf8595327ae9c8a7f7ab099a5c9b0',
        Size: 49,
        StorageClass: 'STANDARD'
      },
      {
        Key: `raw/${root.toString()}/304288589213073932/ciqace6wkhzula3pb3vh6uesdnmf5ul42ljzjmtqrvjrgh2zlefg4wq.car`,
        LastModified: new Date().toISOString(),
        ETag: '60faf8595327ae9c8a7f7ab099a5c9b0',
        Size: 49,
        StorageClass: 'STANDARD'
      }
    ]
  })

  const { rootCid, structure, response } = await handler(s3Event)
  t.deepEqual(root.toString(), rootCid.toString())
  t.deepEqual(structure, 'Partial')
  t.is(response.ok, true, 'Obtained gateway response')
})
