# gateway read test

> Lambda trigger on every nft.storage write that reads data from multiple gateways.

## Motivation

The `gateway-read-test-lambda` aims to track the IPFS gateway performance over time once writes happen in nft.storage, as well as alerting when availability issues happen.

It uses the [nft.storage gateway](https://github.com/nftstorage/nft.storage/tree/main/packages/gateway), which keeps a record of the performance of each backend gateway it uses, as well as which gateways had successfully retrieved a given [CID](https://docs.ipfs.io/concepts/content-addressing/#identifier-formats).
