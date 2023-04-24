const express = require('express')
const router = express.Router()
const { StastPriceCollection, AlertVolumen } = require('../controllers/statisticsCollection')



router.post('/stastpricecollection', StastPriceCollection)
router.post('/alertvolumen', AlertVolumen)


module.exports = router