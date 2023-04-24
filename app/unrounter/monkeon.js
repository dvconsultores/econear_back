const express = require('express')
const router = express.Router()
const { SalesOfTheDay, RecentSales, HighestVOLGainers, TopFloorMovers, Volumen24h, Volumen7d, Ranking, UpcomingListed,
    NewProjectsListed, ActiveWalleHeader, ActiveWallets, ActiveWalletsMarket, StastMarket, CompareProjects } = require('../controllers/monkeon')

router.post('/salesoftheday', SalesOfTheDay)
router.post('/recentsales', RecentSales)
router.post('/highestvolgainers', HighestVOLGainers)
router.post('/topfloormovers', TopFloorMovers)
router.post('/volumen24h', Volumen24h)
router.post('/volumen7d', Volumen7d)
router.post('/ranking', Ranking)
router.post('/upcominglisted', UpcomingListed)
router.post('/newprojectslisted', NewProjectsListed)
router.post('/activewalleheader', ActiveWalleHeader)
router.post('/ActiveWallets', ActiveWallets)
router.post('/activewalletsmarket', ActiveWalletsMarket)
router.post('/stastmarket', StastMarket)
router.post('/compareprojects', CompareProjects)

module.exports = router