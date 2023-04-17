const express = require('express')
const router = express.Router()
const { ProjectsDetailsHeader, StastMarketCapVolumenCollection, StastPriceCollection, StastBuyersTradersCollection, StastSalesLiquidCollection, StastTopSalesCollection, 
    StastSearchNftCollection, StastNftCollection, StastTopBuyersCollection, StastTopSellersCollection, StastActivityCollection,
    StastRarityDistributionCollection } = require('../controllers/projectsDetails')



router.post('/projectsdetailsheader', ProjectsDetailsHeader)
router.post('/stastmarketcapvolumencollection', StastMarketCapVolumenCollection)
router.post('/stastpricecollection', StastPriceCollection)
router.post('/stastbuyerstraderscollection', StastBuyersTradersCollection)
router.post('/StastSalesLiquidCollection', StastSalesLiquidCollection)
router.post('/stasttopsalescollection', StastTopSalesCollection)
router.post('/stastsearchnftcollection', StastSearchNftCollection)
router.post('/stastnftcollection', StastNftCollection)
router.post('/stasttopbuyerscollection', StastTopBuyersCollection)
router.post('/stasttopsellerscollection', StastTopSellersCollection)
router.post('/stastactivitycollection', StastActivityCollection)
router.post('/stastraritydistributioncollection', StastRarityDistributionCollection)


module.exports = router