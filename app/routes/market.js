const express = require('express')
const router = express.Router()
const { MarketplaceVolumen } = require('../controllers/market')



router.post('/marketplacevolumen', MarketplaceVolumen)


module.exports = router