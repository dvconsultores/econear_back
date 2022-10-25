const express = require('express')
const router = express.Router()
const { AlertPrice, AlertVolumen } = require('../controllers/alert')



router.post('/alertprice', AlertPrice)
router.post('/alertvolumen', AlertVolumen)


module.exports = router