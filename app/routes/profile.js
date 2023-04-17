const express = require('express')
const router = express.Router()
const { YourBalance, YourProjectsList, ToSubscribe, SavePerfil, YourPerfil, ToSettings, YourSettings, IsHolderMonkeonnear, RarezasToken } = require('../controllers/profile')

const upload = require('../controllers/storage');
const pinataSDK = require('@pinata/sdk');
const pinata = pinataSDK('1a2e44bc58cc1099d76e', '551e04783315476f5fda96bae0935053ea2164b268a9d2e698522d4fdb19ceb6');

router.post('/yourbalance', YourBalance)
router.post('/yourprojectslist', YourProjectsList)
router.post('/tosubscribe', ToSubscribe)
router.post('/SavePerfil', upload.single('uploaded_file'), SavePerfil)
router.post('/yourperfil', YourPerfil)
router.post('/tosettings', ToSettings)
router.post('/yoursettings', YourSettings)
router.post('/isholdermonkeonnear', IsHolderMonkeonnear)
router.post('/rarezastoken', RarezasToken)

module.exports = router