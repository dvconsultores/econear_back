const express = require('express')
const router = express.Router()
const { SalesOfTheDay, HighestVOLGainers, Volumen24h, Volumen7d, BuscarCollection, Ranking, UpcomingListed, NewProjectsListed, ActiveWallets, ActiveWalletsMarket } = require('../controllers/monkeon')

router.post('/salesoftheday', SalesOfTheDay)
router.post('/highestvolgainers', HighestVOLGainers)
router.post('/volumen24h', Volumen24h)
router.post('/volumen7d', Volumen7d)
router.post('/buscarcollection', BuscarCollection)
router.post('/ranking', Ranking)
router.post('/upcominglisted', UpcomingListed)
router.post('/newprojectslisted', NewProjectsListed)
router.post('/ActiveWallets', ActiveWallets)
router.post('/activewalletsmarket', ActiveWalletsMarket)

module.exports = router