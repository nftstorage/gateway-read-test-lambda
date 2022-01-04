'use strict'

const { CID } = require('multiformats/cid')

/**
 * Parse CID and return normalized b32 v1
 *
 * @param {string} cid
 */
function normalizeCid(cid) {
  const c = CID.parse(cid)
  return c.toV1().toString()
}

module.exports = {
  normalizeCid,
}
