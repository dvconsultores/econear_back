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

const { utils, Contract, keyStores, KeyPair , Near, Account} = nearAPI;



const SIGNER_ID = process.env.SIGNER_ID;
const SIGNER_PRIVATEKEY = process.env.SIGNER_PRIVATEKEY;
const NETWORK = process.env.NETWORK;

const keyStore = new keyStores.InMemoryKeyStore()
const keyPair = KeyPair.fromString(SIGNER_PRIVATEKEY)
keyStore.setKey(NETWORK, SIGNER_ID, keyPair)
const near = new Near(CONFIG(keyStore))
const account = new Account(near.connection, SIGNER_ID)



const RecentlyListed = async (req, res) => {
    try {
        //const fecha = moment().subtract(1, 'M').valueOf()*1000000;
        const { limit, index } = req.body
        const conexion2 = await dbConnect2()
        
        let query = "   select \
                            c.nft_contract, c.total_supply, case when p.fecha is null then c.fecha_creacion else p.fecha end as fecha_listado, \
                            nm.marketplace, m.name as market_name, m.icon as market_icon, m.web as market_web, \
                            c.name, c.symbol, c.icon, c.base_uri, c.reference, c.fecha_creacion, \
                            min(cast(nm.precio as numeric) / 1000000000000000000000000) as price \
                        from collections c \
                        left join projects p on c.nft_contract = p.id_contract_project and p.already = true \
                        inner join nft_marketplace nm on nm.collection = c.nft_contract  \
                        inner join marketplaces m on m.marketplace = nm.marketplace   \
                        group by c.nft_contract, c.total_supply, p.fecha, nm.marketplace, \
                            m.name, m.icon, m.web, \
                            c.name, c.symbol, c.icon, c.base_uri, c.reference, c.fecha_creacion \
                    order by case when p.fecha is null then c.fecha_creacion else p.fecha end desc \
                    limit $1 offset $2 ";
        
        const respuesta = await conexion2.query(query, [limit, index])

        res.json(respuesta.rows)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: false, error: error})
    }
}


const RecentlyAdded = async (req, res) => {
    try {
        const { top, order, owner } = req.body;

        //const fecha = moment().subtract(2, 'M').valueOf()*1000000;

        const conexion2 = await dbConnect2()

        /*let query = "   select c.nft_contract, c.total_supply, p.fecha as fecha_listado, c.name, \
                        c.symbol, c.icon, c.base_uri, c.reference, c.fecha_creacion , 104.4 as price \
                    from projects p \
                    inner join collections c on c.nft_contract = p.id_contract_project \
                    where p.fecha > $1 \
                    and p.already = true \
                    order by p.fecha desc  \
                    limit $2 ";*/
        
        let query = "   select \
                            c.nft_contract, c.total_supply, p.fecha as fecha_listado, c.name, \
                            nm.marketplace, m.name as market_name, m.icon as market_icon, m.web as market_web, \
                            c.symbol, c.icon, c.base_uri, c.reference, c.fecha_creacion, nm.precio as price, \
                            sum(coalesce(case when v.voto = 'true' then 1 else 0 end, 0)) as votos_positivos, \
                            sum(coalesce(case when v.voto = 'false' then 1 else 0 end, 0)) as votos_negativos, \
                            coalesce((select true from nft_collections nc where token_new_owner_account_id = $2 and collection = c.nft_contract group by true), false) as permission_voto_nega \
                        from projects p \
                        inner join collections c on c.nft_contract = p.id_contract_project \
                        inner join ( \
                            select \
                                nmm.marketplace, nmm.collection, \
                                min(cast(nmm.precio as numeric) / 1000000000000000000000000) as precio \
                            from nft_marketplace nmm \
                            group by \
                                nmm.marketplace, nmm.collection \
                        ) nm on nm.collection = p.id_contract_project \
                        inner join marketplaces m on m.marketplace = nm.marketplace \
                        left join votos v on v.collection = c.nft_contract \
                        where p.already = true \
                        group by c.nft_contract, c.total_supply, p.fecha, nm.marketplace, c.name, m.name, m.icon, m.web, \
                                c.symbol, c.icon, c.base_uri, c.reference, c.fecha_creacion, nm.precio "
        
        switch (order) {
            case "voto":
                query += " order by sum(coalesce(case when v.voto = 'true' then 1 else 0 end, 0)) desc "
                break;
        
            default:
                query += " order by c.fecha_creacion desc "
                break;
        }

        query += " limit $1 "

        const respuesta = await conexion2.query(query, [top, owner])
        res.json(respuesta.rows)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: false, error: error})
    }
}


const ThemostVoted = async (req, res) => {
    try {
        const { top, owner } = req.body;
        //const fecha = moment().subtract(1, 'M').valueOf()*1000000;

        const conexion2 = await dbConnect2()
        
        let query = "   select \
                            c.nft_contract as collection, c.name, c.icon, c.reference, c.total_supply, \
                            sum(coalesce(case when v.voto = 'true' then 1 else 0 end, 0)) as votos_positivos, \
                            sum(coalesce(case when v.voto = 'false' then 1 else 0 end, 0)) as votos_negativos, \
                            coalesce((select true from nft_collections nc where token_new_owner_account_id = $2 and collection = c.nft_contract group by true), false) as permission_voto_nega \
                        from collections c \
                        inner join votos v on v.collection = c.nft_contract  \
                        group by c.nft_contract, c.name, c.icon, c.reference, c.total_supply \
                        order by sum(coalesce(case when v.voto = 'true' then 1 else 0 end, 0)) desc  \
                        limit $1 ";
        
        const respuesta = await conexion2.query(query, [top, owner])

        res.json(respuesta.rows)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: false, error: error})
    }
}

//update_collections()
//update_masivo_collections()


module.exports = { RecentlyListed, RecentlyAdded, ThemostVoted }