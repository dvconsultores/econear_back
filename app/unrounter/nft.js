const express = require('express')
const router = express.Router()
const { BuscarCollection, ListMarketplace, ListCollections, ListMarketplaceCollection, ListNft, 
        CollectionDetails, BuyOnMarketplace, SearchNft, ListNftOwner, BulkList, BulkListDetails, Collections
} = require('../controllers/nft')

router.post('/buscarcollection', BuscarCollection)
router.post('/listmarketplace', ListMarketplace)
router.post('/listcollections', ListCollections)
router.post('/listmarketplacecollection', ListMarketplaceCollection)
router.post('/listnft', ListNft)
router.post('/collectiondetails', CollectionDetails)
router.post('/buyonmarketplace', BuyOnMarketplace)
router.post('/searchnft', SearchNft)
router.post('/listnftowner', ListNftOwner)
router.post('/bulklist', BulkList)
router.post('/bulklistdetails', BulkListDetails)
router.post('/collections', Collections)

module.exports = router