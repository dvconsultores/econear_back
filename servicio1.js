const moment = require('moment');
/*const { CONFIG } = require('./app/helpers/utils')

const nearAPI = require("near-api-js");
const axios = require('axios');



const { utils, Contract, keyStores, KeyPair, Near, Account } = nearAPI


const SIGNER_ID = process.env.SIGNER_ID;
const SIGNER_PRIVATEKEY = process.env.SIGNER_PRIVATEKEY;
const NETWORK = process.env.NETWORK;

const keyStore = new keyStores.InMemoryKeyStore()
const keyPair = KeyPair.fromString(SIGNER_PRIVATEKEY)
keyStore.setKey(NETWORK, SIGNER_ID, keyPair)
const near = new Near(CONFIG(keyStore))
const account = new Account(near.connection, SIGNER_ID)*/


//const { Databuy, DataSell, UpdateDataMarket, DataMarket, HistCollection, updateProjects, UpdateVotes, UpdateVotesUpcoming, update_masivo_collections } = require('./app/controllers/funciones')
//const { Databuy } = require('./app/controllers/funciones')
//const { ListarNft, CargarRutaIfsImgNft, UpdateNft, CargarJsonAtributosNft } = require('./app/controllers/funcionesNft')





async function ejecutar() {
    const epoch_h = moment().subtract(5, 'm').valueOf()*1000000;
    console.log('ejecuto')
    //await Databuy()
    //await DataSell()
    //await UpdateDataMarket(epoch_h)
    //await updateProjects(epoch_h);
    //await UpdateNft(epoch_h)
    //await DataMarket(epoch_h)
    //await UpdateVotes(epoch_h)
    //await UpdateVotesUpcoming(epoch_h)
}

ejecutar()