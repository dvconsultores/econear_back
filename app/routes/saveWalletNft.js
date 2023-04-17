const express = require('express')
const router = express.Router()
const { saveWalletNft } = require('../controllers/saveWalletNft')

router.post('/save-wallet-nft/', saveWalletNft)

module.exports = router