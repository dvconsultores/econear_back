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

const { UpdateNft } = require('./funcionesNft')


const YourBalance = async (req, res) => {
    try {
        const { user_id } = req.body;

        const conexion = await dbConnect2();

        let contract_usdc = "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near"
        let saldo_usdc = 0

        let contract_usdt = "dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near"
        let saldo_usdt = 0

        let contract_dai = "6b175474e89094c44da98b954eedeac495271d0f.factory.bridge.near"
        let saldo_dai = 0

        let contract_nexp = "nexp.near"
        let saldo_nexp = 0

        let contract_uto = "utopia.secretskelliessociety.near"
        let saldo_uto = 0

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
        
        try {
            const contract = new Contract(account, contract_nexp, {
                viewMethods: ['ft_balance_of'],
                sender: account
            })
        
            const response = await contract.ft_balance_of({account_id: user_id})
            saldo_nexp = response
        } catch (error) {
            console.log('error 2: ', error)
        }

        try {
            const contract = new Contract(account, contract_uto, {
                viewMethods: ['ft_balance_of'],
                sender: account
            })
        
            const response = await contract.ft_balance_of({account_id: user_id})
            saldo_uto = response
        } catch (error) {
            console.log('error 2: ', error)
        }

        res.json({saldo_usdc: saldo_usdc, saldo_usdt: saldo_usdt, saldo_dai: saldo_dai, saldo_nexp: saldo_nexp, saldo_uto: saldo_uto})
        conexion.end()
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
        conexion.end()
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
        conexion.end()
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}

const pinataSDK = require('@pinata/sdk');
const pinata = pinataSDK(process.env.PINATA_API_KEY_CALENDARIO, process.env.PINATA_API_SECRET_CALENDARIO);
const fs = require('fs');
const { Query } = require('pg');


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
                const media = 'https://bronze-far-catfish-347.mypinata.cloud/ipfs/' + result.IpfsHash
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
        conexion.end()
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
        conexion.end()
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}
//CargarRutaIfsImgNft()

const ToSettings = async (req, res) => {
    try {
        const { user_id, email, nft_drop, noti_transaction } = req.body;

        const conexion = await dbConnect2();
        
        const duplicado = await conexion.query(" select user_id, email from settings where user_id = $1 ", [user_id]);

        if(duplicado.rows.length == 0) {
            const resultados = await conexion.query(" insert into settings(user_id, email, nft_drop, noti_transaction) values ($1, $2, $3, $4)  ", 
            [user_id, email, nft_drop, noti_transaction]);
            const data = resultados.rows;
            
            res.json({result: "ok", value: "Los datos guardados con exito!"})
        } else if(duplicado.rows.length > 0) {
            let queryUpdate = " update settings \
                                set \
                                    email = $1, \
                                    nft_drop = $2, \
                                    noti_transaction = $3 \
                                where \
                                    user_id = $4 \
            ";
            if(duplicado.rows[0].email == email) {
                await conexion.query(queryUpdate, [email, nft_drop, noti_transaction, user_id]);
            } else {
                const duplicadoEmail = await conexion.query(" select user_id, email from settings where email = $1 ", [email]);
                if(duplicadoEmail.rows.length > 0) {
                    if(duplicadoEmail.rows[0].user_id == user_id) {
                        await conexion.query(queryUpdate, [email, nft_drop, noti_transaction, user_id]);    
                    } else {
                        res.json({result: "email", value: "Ya existe un email igual!"})
                    }
                } else if(duplicadoEmail.rows.length == 0) {
                    await conexion.query(queryUpdate, [email, nft_drop, noti_transaction, user_id]);
                } else {
                    res.json({result: "error", value: "Error inesperado!"})        
                }
            }
            res.json({result: "ok", value: "Los datos se actualizaron con exito!"})
        } else {
            res.json({result: "error", value: "Error inesperado!"})
        }
        conexion.end()
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}

const YourSettings = async (req, res) => {
    try {
        const { user_id } = req.body;

        const conexion = await dbConnect2();
        
        let query = "   select \
                            user_id, email, nft_drop, noti_transaction \
                        from settings where user_id = $1 ";
        
        const resultados = await conexion.query(query, [user_id]);
        const data = resultados.rows;
        
        res.json(data)
        conexion.end()
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}


const IsHolderMonkeonnear = async (req, res) => {
    try {
        const { user_id } = req.body;

        const conexion = await dbConnect2();

        const epoch_h = moment().subtract(10, 'm').valueOf()*1000000;
        await UpdateNft(epoch_h)
        
        let query = "   select token_new_owner_account_id as owner \
                        from nft_collections nc  \
                        where collection = 'monkeonear.neartopia.near'  \
                        and token_new_owner_account_id = $1  \
                        limit 1  \
        ";
        
        const resultados = await conexion.query(query, [user_id]);
        const data = resultados.rows.length > 0 ? true : false;

        
        res.json({respuesta: data})
        conexion.end()
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}

const RarezasToken = async (req, res) => {
    try {
        const { collection, token_id } = req.body;

        const conexion = await dbConnect2();
        
        let query = "   select \
                            sub.collection, sub.token_id, coalesce(rarity_score, 0) as rarity_score,  \
                            coalesce(case   \
                                when rarity_score > 0 and rarity_score < 35.9 then 'common'  \
                                when rarity_score > 35 and rarity_score < 37.9 then 'uncommon' \
                                when rarity_score > 37 and rarity_score < 39.9 then 'rare'  \
                                when rarity_score > 39 and rarity_score < 40.9 then 'epic'  \
                                when rarity_score > 40 then 'legendary'  \
                            end, 'common') as rareza, \
                            coalesce((select nm.precio_near from nft_marketplace nm where nm.collection = sub.collection and precio_near > 0 order by nm.precio_near limit 1), 0) as floor_price \
                        from ( \
                            select $1 as collection, $2 as token_id \
                        ) sub \
                        left join (  \
                            select   \
                                    a2.collection, a2.token_id,   \
                                    avg(round(atr.porcentaje)) as rarity_score,  \
                                    ROW_NUMBER () OVER (ORDER BY avg(round(atr.porcentaje)) desc) as ranking  \
                                from atributos a2   \
                                inner join (  \
                                    select   \
                                        collection, trait_type, value,   \
                                        case   \
                                            when coalesce(count(value)::numeric, 0) > 0 and coalesce(max(c.total_supply)::numeric, 0) > 0  \
                                            then (count(value)::numeric / max(c.total_supply)::numeric) * 100  \
                                            else 0  \
                                        end as porcentaje  \
                                    from atributos a  \
                                    inner join collections c on c.nft_contract = a.collection  \
                                        where collection = $1 \
                                    group by   \
                                        collection, trait_type, value  \
                                ) atr on atr.collection = a2.collection and atr. trait_type = a2.trait_type and atr.value = a2.value  \
                                where a2.collection = $1 \
                                    and a2.token_id = $2 \
                                group by  \
                                    a2.collection, a2.token_id  \
                                order by   \
                                    avg(round(atr.porcentaje)) desc	  \
                        ) rarity on rarity.collection = sub.collection and rarity.token_id = sub.token_id \
        ";

        const resultados = await conexion.query(query, [collection, token_id]);
        const data = resultados.rows

        
        res.json(data)
        conexion.end()
    } catch (error) {
        console.log('error 1: ', error)
        res.json({error: error})
    }
}

module.exports = { YourBalance, YourProjectsList, ToSubscribe, SavePerfil, YourPerfil, ToSettings, YourSettings, IsHolderMonkeonnear, RarezasToken }





















