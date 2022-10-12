const express = require('express')
const router = express.Router()
const { RecentlyListed, RecentlyAdded, ThemostVoted } = require('../controllers/snipetools')

router.post('/recentlylisted', RecentlyListed)
router.post('/recentlyadded', RecentlyAdded)
router.post('/themostvoted', ThemostVoted)

module.exports = router