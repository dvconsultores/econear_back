const express = require('express')
const router = express.Router()
const { saveWalletNft, getWalletsNfts } = require('../controllers/saveWalletNft')

router.post('/save-wallet-nft/', saveWalletNft)
router.get('/get-wallets-nfts', getWalletsNfts)

module.exports = router