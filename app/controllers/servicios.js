const { CONFIG } = require('../helpers/utils')
const { dbConnect, dbConnect2, dbConnect3 } = require('../../config/postgres')
const nearAPI = require("near-api-js");
const axios = require('axios');
const moment = require('moment');
/*
const secp = require('tiny-secp256k1');
const ecfacory = require('ecpair');
const path = require('path');
const { ParaSwap } = require('paraswap');
const { response } = require('express');
*/
const { Databuy, DataSell, UpdateDataMarket, DataMarket, HistCollection, updateProjects, UpdateVotes, UpdateVotesUpcoming, update_masivo_collections } = require('./funciones')
const { ListarNft, CargarRutaIfsImgNft, UpdateNft, CargarJsonAtributosNft } = require('./funcionesNft')


const { utils, Contract, keyStores, KeyPair, Near, Account } = nearAPI


const SIGNER_ID = process.env.SIGNER_ID;
const SIGNER_PRIVATEKEY = process.env.SIGNER_PRIVATEKEY;
const NETWORK = process.env.NETWORK;

const keyStore = new keyStores.InMemoryKeyStore()
const keyPair = KeyPair.fromString(SIGNER_PRIVATEKEY)
keyStore.setKey(NETWORK, SIGNER_ID, keyPair)
const near = new Near(CONFIG(keyStore))
const account = new Account(near.connection, SIGNER_ID)

async function listar() {
    await update_masivo_collections()
    await ListarNft()
    await CargarRutaIfsImgNft()
}



async function listar2() {
    const epoch_h = moment().subtract(3, 'd').valueOf()*1000000;
    await updateTransactions(epoch_h);
    /*await updateProjects(epoch_h);
    await UpdateNft(epoch_h)
    await UpdateVotes(epoch_h)
    await UpdateVotesUpcoming(epoch_h)*/
}

//DataMarket()

//listar()
//listar2()
//const epoch_h = moment().subtract(4, 'd').valueOf()*1000000;
//updateTransactions(epoch_h)
//UpdateNft(epoch_h)

//HistCollection()
//ListarNft()  
//UpdateVotesUpcoming()
//const epoch_h = moment().subtract(24, 'h').valueOf()*1000000;
//UpdateDataMarket(epoch_h)
//CargarJsonAtributosNft()
//UpdateDataMarket(epoch_h)
//CargarJsonAtributosNft()
//HistCollection()


const servico_updateTransaction = setInterval(async function () { 
    console.log('--------------------------------------------------------------------')
    console.log('ejecutando servico_updateTransaction')
    try {
        const epoch_h = moment().subtract(30, 'm').valueOf()*1000000;
        await Databuy()
        await DataSell()
        await UpdateDataMarket(epoch_h)
        await updateProjects(epoch_h);
        await UpdateNft(epoch_h)
        await UpdateVotes(epoch_h)
        await UpdateVotesUpcoming(epoch_h)
    } catch (error) {
        console.log(error)
    }
    console.log('culminado servico_updateTransaction')
    console.log('--------------------------------------------------------------------')
}, 60000 * 1);


const servico_carga_historico_collection = setInterval(async function () { 
    console.log('--------------------------------------------------------------------')
    console.log('ejecutando servico_carga_historico_collection')
    try {
        await HistCollection()
    } catch (error) {
        console.log(error)
    }
    console.log('culminado servico_carga_historico_collection')
    console.log('--------------------------------------------------------------------')
}, 60000 * 10);


const servico_updateMasivos = setInterval(async function () { 
    console.log('--------------------------------------------------------------------')
    console.log('ejecutando servico update masivo collections')
    try {
        await update_masivo_collections()
        await ListarNft()
        await CargarRutaIfsImgNft()
    } catch (error) {
        console.log(error)
    }
    console.log('culminado update masivo collections')
    console.log('--------------------------------------------------------------------')
}, 3600000 * 1);


const listaServicios = async (req, res) => {
    const lista = [
        {
            servicio: "cargar ruta ipfs nft"
        }
    ]
    //clearInterval(servico_updateTransaction)
    res.json(lista)
}



const RefreshForm = async (req, res) => {
    const epoch_h = moment().subtract(10, 'm').valueOf()*1000000;
    setTimeout(async() => {
        //await updateTransactions(epoch_h);
        const resp = await updateProjects(epoch_h)
        res.json(resp)
    }, 1000 * 30)
}

const RefreshVotes = async (req, res) => {
    setTimeout(async() => {
        const fecha = moment().subtract(10, 'm').valueOf()*1000000;
        //await updateTransactions(fecha);
        const resp = await UpdateVotes(fecha)
        res.json(resp)
    }, 1000 * 30)
}

const RefreshVotesUpcoming = async (req, res) => {
    setTimeout(async() => {
        const fecha = moment().subtract(10, 'm').valueOf()*1000000;
        //await updateTransactions(fecha);
        const resp = await UpdateVotesUpcoming(fecha)
        res.json(resp)
    }, 1000 * 30)
}

const RefrescarNft = async (req, res) => {
    setTimeout(async() => {
        const epoch_h = moment().subtract(20, 'm').valueOf()*1000000;
        //await updateTransactions(epoch_h)
        await UpdateNft(epoch_h)
        await CargarRutaIfsImgNft()
        res.json({respuesta: 'Listo'})
    }, 1000 * 30)
} 

module.exports = { listaServicios, RefreshForm, RefreshVotes, RefreshVotesUpcoming, RefrescarNft }





















