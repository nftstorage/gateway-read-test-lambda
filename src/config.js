'use strict'

require('dotenv').config()

const { normalizeCid } = require('./utils')

const { GATEWAY: gateway, USE_IPFS_PATH: useIpfsPath } = process.env

const getIpfsGatewayUrlForCid = (cid) => {
  const nCid = normalizeCid(cid.toString())
  if (useIpfsPath) {
    const ipfsUrl = new URL('ipfs', gateway)
    return `${ipfsUrl.toString()}/${nCid}`
  }

  return `https://${nCid}.${gateway}`
}

module.exports = {
  gateway,
  getIpfsGatewayUrlForCid,
}
