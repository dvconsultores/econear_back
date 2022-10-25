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


const { utils, Contract, keyStores, KeyPair, Near, Account } = nearAPI


const SIGNER_ID = process.env.SIGNER_ID;
const SIGNER_PRIVATEKEY = process.env.SIGNER_PRIVATEKEY;
const NETWORK = process.env.NETWORK;

const keyStore = new keyStores.InMemoryKeyStore()
const keyPair = KeyPair.fromString(SIGNER_PRIVATEKEY)
keyStore.setKey(NETWORK, SIGNER_ID, keyPair)
const near = new Near(CONFIG(keyStore))
const account = new Account(near.connection, SIGNER_ID)



const YourBalance = async (req, res) => {
    try {
        const { user_id } = req.body;

        const conexion = await dbConnect2();

        let contract_usdc = "usdc.fakes.testnet"
        let saldo_usdc = 0

        let contract_usdt = "dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near"
        let saldo_usdt = 0

        let contract_dai = "6b175474e89094c44da98b954eedeac495271d0f.factory.bridge.near"
        let saldo_dai = 0

        try {
            const contract = new Contract(account, contract_usdc, {
                viewMethods: ['ft_balance_of'],
                sender: account
            })
        
            const response = await contract.ft_balance_of({account_id: user_id})
            console.log("paso aqui", response)
            saldo_usdc = response
        } catch (error) {
            console.log('error 2: ', error)
        }

        try {
            const contract = new Contract(account, contract_usdt, {
                viewMethods: ['ft_balance_of'],
                sender: account
            })
        
            const response = await contract.ft_balance_of({account_id: user_id})
            saldo_usdt = response
        } catch (error) {
            console.log('error 2: ', error)
        }

        try {
            const contract = new Contract(account, contract_dai, {
                viewMethods: ['ft_balance_of'],
                sender: account
            })
        
            const response = await contract.ft_balance_of({account_id: user_id})
            saldo_dai = response
        } catch (error) {
            console.log('error 2: ', error)
        }
        
        res.json({saldo_usdc: saldo_usdc, saldo_usdt: saldo_usdt, saldo_dai: saldo_dai})
        
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}

const YourProjectsList = async (req, res) => {
    try {
        const { user_id } = req.body;

        const conexion = await dbConnect2();
        
        let query = "   select \
                            project_name, email, discord_id, website, twiter, discord_server, \
                            upcoming, already, fecha_lanzamiento, id_contract_project \
                        from projects where user_creation = $1 ";
        
        const resultados = await conexion.query(query, [user_id]);
        const data = resultados.rows;
        
        res.json(data)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}


const ToSubscribe = async (req, res) => {
    try {
        const { email } = req.body;

        const conexion = await dbConnect2();
        
        const duplicado = await conexion.query(" select email from subscriptions where email = $1 ", [email]);

        if(duplicado.rows.length == 0) {
            const resultados = await conexion.query(" insert into subscriptions (email) values ($1)  ", [email]);
            const data = resultados.rows;
            
            res.json({result: "ok", value: "subcrito con exito!"})
        } else {
            res.json({result: "error", value: "ese correo ya esta subcrito"})
        }
        
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}

const pinataSDK = require('@pinata/sdk');
const pinata = pinataSDK('1a2e44bc58cc1099d76e', '551e04783315476f5fda96bae0935053ea2164b268a9d2e698522d4fdb19ceb6');
const fs = require('fs')


async function SavePerfil (req, res) {
    const { wallet, username, email, discord_id, discord_server, web_site, twitter_account, bio } = req.body
    try {
        const conexion = await dbConnect2();
        const duplicado = await conexion.query(" select wallet from profiles where wallet = $1 ", [wallet]);
        
        if(duplicado.rows.length == 0){
            await conexion.query(" insert into profiles (wallet, username, email, discord_id, discord_server, web_site, twitter_account, bio) \
                                    values ($1, $2, $3, $4, $5, $6, $7, $8)", [wallet, username, email, discord_id, discord_server, web_site, twitter_account, bio]);
        
        } else {
            await conexion.query(" update profiles set email = $2, discord_id = $3, discord_server = $4, web_site = $5, twitter_account = $6, bio = $7 \
                                    where wallet = $1 ", [wallet, email, discord_id, discord_server, web_site, twitter_account, bio]);
        
        }

        if (req.file) {
        
            const readableStreamForFile = fs.createReadStream(__dirname + '/storage/img/' + req.file.filename);
            const options = {
                pinataMetadata: {
                    name: wallet,
                    keyvalues: {
                        customKey: 'customValue',
                        customKey2: 'customValue2'
                    }
                },
                pinataOptions: {
                    cidVersion: 0
                }
            };
        
            pinata.pinFileToIPFS(readableStreamForFile, options).then(async (result) => {
                //handle results here
                const path = __dirname + '/storage/img/' + '/' + req.file.filename;
                if (fs.existsSync(path)) {
                fs.unlinkSync(path);
                }
                const media = 'https://gateway.pinata.cloud/ipfs/' + result.IpfsHash
                console.log(media)
                console.log(result)
                try {
                    await conexion.query(" update profiles set img = $2 where wallet = $1 ", [wallet, media]);
                } catch(error) {
                    console.log(err);    
                }

                res.json({result: 'ok'});
            }).catch((err) => {
                //handle error here
                res.json({error: err});
                console.log(err);
            });
        } else {
            res.json({result: 'ok'});
        }
    } catch(error) {
        res.json({error: error});
        console.log(error);
    }
}

const YourPerfil = async (req, res) => {
    try {
        const { wallet } = req.body;

        const conexion = await dbConnect2();
        
        let query = "   select \
                            wallet, username, email, discord_id, discord_server, web_site, \
                            twitter_account, img, bio \
                        from profiles where wallet = $1 ";
        
        const resultados = await conexion.query(query, [wallet]);
        const data = resultados.rows;
        
        res.json(data)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}
//CargarRutaIfsImgNft()




module.exports = { YourBalance, YourProjectsList, ToSubscribe, SavePerfil, YourPerfil }





















