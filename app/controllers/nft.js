const { CONFIG } = require('../helpers/utils')
const { dbConnect, dbConnect2, dbConnect3 } = require('../../config/postgres')
const nearAPI = require("near-api-js");
const axios = require('axios');
const moment = require('moment');
const { updateTransactions } = require('./funciones')
const { CargarRutaIfsImgNft, UpdateNft } = require('./funcionesNft')
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


const BuscarCollection = async (req, res) => {
    try {
        const { search, top } = req.body;
        const conexion = await dbConnect2()
        const resultados = await conexion.query("   select \
                                                        c.nft_contract, c.owner_id, c.name, c.symbol, c.icon, c.base_uri , c.reference, c.fecha_creacion \
                                                    from collections c \
                                                    where upper(c.name) like '%'||upper($1)||'%' \
                                                    limit $2 \
                                                ", [search, top])
                        
        res.json(resultados.rows)
    } catch (error) {
        res.error
    }
}


const ListMarketplace = async (req, res) => {
    try {
        const conexion = await dbConnect2();
        
        let query = "select m.marketplace, m.name as market_name, m.icon as market_icon, m.web as market_web from marketplaces m";
        
        const resultados = await conexion.query(query);
        const data = resultados.rows;
        
        res.json(data)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}


const SearchCollections = async (req, res) => {
    try {
        const { input, limit, index } = req.body;

        const conexion = await dbConnect2();
        
        let entrada = "%"+input.toUpperCase()+"%"

        let query = "select nft_contract, name, symbol, icon, base_uri, fecha_creacion  from collections where upper(name) like $1 limit $2 offset $3 ";
   
        const resultados = await conexion.query(query, [entrada, limit, index]);
        const data = resultados.rows;
        
        res.json(data)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}

const ListCollections = async (req, res) => {
    try {
        const { limit, index } = req.body;

        const conexion = await dbConnect2();
        
        let query = "select nft_contract, name, symbol, icon, base_uri, fecha_creacion  from collections limit $1 offset $2 ";
        
        const resultados = await conexion.query(query, [limit, index]);
        const data = resultados.rows;
        
        res.json(data)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}

const ListMarketplaceCollection = async (req, res) => {
    try {
        const { collection } = req.body;

        const conexion = await dbConnect2();

        let query = "   select cm.marketplace, m.name as market_name, m.icon as market_icon, m.web as market_web \
                        from collections_marketplace cm  \
                        inner join marketplaces m on m.marketplace = cm.marketplace \
                        where cm.collection = $1 ";
        
        const resultados = await conexion.query(query, [collection]);
        const data = resultados.rows;
        
        res.json(data)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}

const ListNft = async (req, res) => {
    try {
        const { collection, tokenid, marketplace, limit, index, sales, order, type_order } = req.body;

        const conexion = await dbConnect2();
        
        let query = "select collection, token_id, owner_id, precio, precio_near, base_uri, titulo, descripcion, media, extra, reference, \
                        market_name, marketplace, market_icon, market_web \
                    from ( \
                    select \
                        nc.collection, nc.token_id, nc.token_new_owner_account_id as owner_id, nm.precio, cast(nm.precio as numeric) / 1000000000000000000000000 as precio_near, \
                        c.base_uri, nc.titulo, nc.descripcion, nc.media, nc.extra, nc.reference, \
                        nm.marketplace, m.name as market_name, m.icon as market_icon, m.web as market_web \
                    from nft_collections nc \
                    inner join collections c on c.nft_contract = nc.collection \
                    left join nft_marketplace nm on nm.collection = nc.collection and nm.token_id = nc.token_id \
                    left join marketplaces m on m.marketplace = nm.marketplace \
                    where nc.collection = $1 and ('%' = $2 or '' = $2 or nm.marketplace = $2) \
                    and ('%' = $3 or nc.token_id = $3 ) "
        
        //if(marketplace == "" || marketplace == "%") {
        switch (sales) {
            case 'true':
                query += " and nm.precio is not null ";
                break;
            case 'false':
                query += " and nm.precio is null ";
                break;
            default:
                query += "";
                break;
        }
        //}
        
        query += " ) sub "

        let typeOrder
        switch (type_order) {
            case 'asc':
                typeOrder = " asc ";
                break;
            default:
                typeOrder = " desc ";
                break;
        }

        switch (order) {
            case 'precio':
                query += " order by sub.precio_near " + typeOrder;
                break;
            case 'token_id':
                query += " order by sub.token_id " + typeOrder;
                break;
            default:
                query += "";
                break;
        }

        query += " limit $4 offset $5 ";
        
        const resultados = await conexion.query(query, [collection, marketplace, tokenid, limit, index]);
        const data = resultados.rows;
        
        res.json(data)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}

const CollectionDetails = async (req, res) => {
    try {
        const { collection } = req.body;

        const conexion = await dbConnect2();

        let query = " select  \
                        (select c.name  from collections c where nft_contract = $1) as name, \
                        (select c.icon  from collections c where nft_contract = $1) as icon, \
                        (select c.total_supply from collections c where nft_contract = $1) as supply, \
                        (select count(owner_id) \
                        from (select distinct token_new_owner_account_id as owner_id  \
                        from nft_collections nc where collection = $1) sub) as owner_for_tokens, \
                        (select sum(cast(cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->>'balance' as numeric) / 1000000000000000000000000) \
                        from transaction t \
                        where receipt_predecessor_account_id in (select marketplace from marketplaces) \
                        and receipt_receiver_account_id = $1 \
                        and method_name = 'nft_transfer_payout' ) as total_volumen ";
        
        const resultados = await conexion.query(query, [collection]);
        const data = resultados.rows;
        
        res.json(data)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}

const BuyOnMarketplace = async (req, res) => {
    try {
        const { collection } = req.body;

        const conexion = await dbConnect2();

        let query = "   select cm.marketplace, m.name as market_name, m.icon as market_icon, m.web as market_web \
                        from collections_marketplace cm  \
                        inner join marketplaces m on m.marketplace = cm.marketplace \
                        where cm.collection = $1 \
                        and exists (select 1 from nft_marketplace nm where nm.collection = cm.collection and nm.marketplace = cm.marketplace  group by 1) ";
        
        const resultados = await conexion.query(query, [collection]);
        const data = resultados.rows;
        
        res.json(data)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}

const SearchNft = async (req, res) => {
    try {
        const { search, collection, sales, top } = req.body;
        const conexion = await dbConnect2()

        let query = "   select titulo, token_id, media \
                        from ( \
                            select nc.titulo, nc.token_id, nc.media \
                            from nft_collections nc \
                            left join (select sum(coalesce(cast(ma.precio as numeric), 0)) as precio, ma.token_id, ma.collection \
                                        from nft_marketplace ma group by ma.token_id, ma.collection) nm \
                                        on nm.collection = nc.collection and nm.token_id = nc.token_id \
                            where nc.collection = $1 ";

        switch (sales) {
            case 'true':
                query += " and nm.precio is not null ";
                break;
            case 'false':
                query += " and nm.precio is null ";
                break;
            default:
                query += "";
                break;
        }

        query += " ) sub where upper(titulo||' '||token_id) like '%'||upper($2)||'%' "

        query += " limit $3 "

        const resultados = await conexion.query(query, [collection, search, top])
                        
        res.json(resultados.rows)
    } catch (error) {
        res.error
    }
}

const ListNftOwner = async (req, res) => {
    try {
        const { owner, limit, index } = req.body;

        const conexion = await dbConnect2();

        let query = "select \
                        nc.collection, nc.token_id, nc.token_new_owner_account_id as owner_id, nm.precio, \
                        c.base_uri, nc.media, nc.titulo, nm.marketplace, m.name as market_name, m.icon as market_icon, m.web as market_web \
                    from nft_collections nc \
                    inner join collections c on c.nft_contract = nc.collection \
                    left join nft_marketplace nm on nm.collection = nc.collection and nm.token_id = nc.token_id \
                    left join marketplaces m on m.marketplace = nm.marketplace \
                    where nc.token_new_owner_account_id = $1 limit $2 offset $3 ";

        const resultados = await conexion.query(query, [owner, limit, index]);
        const data = resultados.rows;
        
        res.json(data)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}


const BulkList = async (req, res) => {
    try {
        const { owner, limit, index, listed } = req.body;

        const conexion = await dbConnect2();

        let query = "   select  \
                            nc.collection, \
                            count(nc.token_id) as nft, \
                            sum(COALESCE(cast(nm.precio as numeric), 0) / 1000000000000000000000000) as total_price, \
                            min(COALESCE(cast(nm.precio as numeric), 0) / 1000000000000000000000000) as floor_price, \
                            c.icon, c.base_uri, c.name \
                        from nft_collections nc \
                        inner join collections c on c.nft_contract = nc.collection \
                        left join nft_marketplace nm on nm.collection = nc.collection and nm.token_id = nc.token_id \
                        where nc.token_new_owner_account_id = $1 "
        ;

        switch (listed) {
            case true:
                query += " and nm.precio is not null ";
                break;
            default:
                query += " and nm.precio is null ";
                break;
        }

        query += "      group by \
                            nc.collection, c.icon, c.base_uri, nc.token_new_owner_account_id, c.name \
                        limit $2 offset $3 ";
        
        
        const resultados = await conexion.query(query, [owner, limit, index]);
        const data = resultados.rows;

        
        res.json(data)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}

const BulkListDetails = async (req, res) => {
    try {
        const { owner, collection, limit, index, listed } = req.body;

        const conexion = await dbConnect2();

        let query = "   select  \
                            nc.collection, nc.titulo, nc.descripcion, nc.media, nc.token_id, \
                            COALESCE(cast(nm.precio as numeric), 0) / 1000000000000000000000000 as price, \
                            nm.marketplace, m.name as market_name, m.icon as market_icon, m.web as market_web \
                        from nft_collections nc  \
                        left join nft_marketplace nm on nm.collection = nc.collection and nm.token_id = nc.token_id \
                        left join marketplaces m on m.marketplace = nm.marketplace \
                        where nc.token_new_owner_account_id = $1  \
                        and nc.collection = $2 "
                        
        switch (listed) {
            case true:
                query += " and cast(nm.precio as numeric) > 0 "
                break;
            default:
                query += " and nm.precio is null "
                break;
        }
        
        let plimit = limit;
        let pindex = index;

        if(index == '%' && limit == '%') {
            plimit = "10000000000";
            pindex = "0";
        }
        
        query +=         " limit $3 offset $4 ";
        
        const resultados = await conexion.query(query, [owner, collection, plimit, pindex]);
        const data = resultados.rows;
        
        res.json(data)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}


async function pruebas() {
    try {
        const conexion = await dbConnect2();

        let query = "select \
                        nc.collection, nc.token_id, nc.token_new_owner_account_id as owner_id, nm.precio, \
                        nm.marketplace, c.base_uri \
                    from nft_collections nc \
                    inner join collections c on c.nft_contract = nc.collection \
                    left join nft_marketplace nm on nm.collection = nc.collection and nm.token_id = nc.token_id \
                    where nc.token_new_owner_account_id = $1 limit $2 offset $3 ";
        
        const resultados = await conexion.query(query, [owner, limit, index]);
        const data = resultados.rows;
        
        res.json(data)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}




//RefrescarNft()

//update_collections()
//update_masivo_collections()


module.exports = { BuscarCollection, ListMarketplace, ListMarketplaceCollection, SearchCollections,
                ListCollections, ListNft, CollectionDetails, BuyOnMarketplace, SearchNft, ListNftOwner,
                BulkList, BulkListDetails }





















