const express = require('express')
const router = express.Router()
const { listaServicios, RefreshForm, RefreshVotes, RefreshVotesUpcoming, RefrescarNft } = require('../controllers/servicios')

router.post('/listaservicios', listaServicios)
router.post('/refreshform', RefreshForm)
router.post('/refreshvotes', RefreshVotes)
router.post('/refreshvotesupcoming', RefreshVotesUpcoming)
router.post('/refrescarnft', RefrescarNft)


module.exports = router