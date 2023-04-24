const { CONFIG } = require('../helpers/utils')
const { dbConnect, dbConnect2, dbConnect3 } = require('../../config/postgres')
const nearAPI = require("near-api-js");
const axios = require('axios');
const moment = require('moment');
const { UpdateDataMarket } = require('./funciones')
const { CargarRutaIfsImgNft, UpdateNft } = require('./funcionesNft')
const { colsutaGraph } = require('../graphql/dataThegraph')
const gql = require('graphql-tag');
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
                                                        c.nft_contract, c.owner_id, c.name, c.symbol, \
                                                        case when c.flag_pinata = true then c.icon_pinata else c.icon end as icon, \
                                                        c.base_uri , c.reference, c.fecha_creacion \
                                                    from collections c \
                                                    where upper(c.name) like '%'||upper($1)||'%' \
                                                    limit $2 \
                                                ", [search, top])
                        
        res.json(resultados.rows)
        conexion.end()
    } catch (error) {
        res.json({error: error})
    }
}


const ListMarketplace = async (req, res) => {
    try {
        const conexion = await dbConnect2();
        
        let query = "select m.marketplace, m.name as market_name, m.icon as market_icon, m.web as market_web from marketplaces m";
        
        const resultados = await conexion.query(query);
        const data = resultados.rows;
        
        res.json(data)
        conexion.end()
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}


/*const SearchCollections = async (req, res) => {
    try {
        const { input, limit, index } = req.body;

        const conexion = await dbConnect2();
        
        let entrada = "%"+input.toUpperCase()+"%"

        let query = "select nft_contract, name, symbol, case when flag_pinata = true then icon_pinata else icon end as icon \
                    , base_uri, fecha_creacion  \
                    from collections where upper(name) like $1 limit $2 offset $3 ";
   
        const resultados = await conexion.query(query, [entrada, limit, index]);
        const data = resultados.rows;
        
        res.json(data)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}*/

const ListCollections = async (req, res) => {
    try {
        const { limit, index } = req.body;

        const conexion = await dbConnect2();
        
        let query = "select nft_contract, name, symbol, icon, base_uri, fecha_creacion  from collections limit $1 offset $2 ";
        
        const resultados = await conexion.query(query, [limit, index]);
        const data = resultados.rows;
        
        res.json(data)
        conexion.end()
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
        conexion.end()
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}

const ListNft = async (req, res) => {
    try {
        const { collection, tokenid, marketplace, limit, index, sales, order, type_order } = req.body;
        
        //setTimeout(async () => {
            if(parseInt(index) == 0) {
                const epoch_h = moment().subtract(30, 'm').valueOf()*1000000;
                await UpdateDataMarket(epoch_h)
            }

            const conexion = await dbConnect2();
            
            

            let query = "select \
                            collection, token_id, owner_id, precio, precio_near, base_uri, titulo, descripcion, media, extra, reference, \
                            market_name, marketplace, market_icon, market_web, media_pinata \
                        from ( \
                        select \
                            case when nc.flag_pinata = true then nc.media_pinata else nc.media end as media, \
                            coalesce(nc.media_pinata, '') as media_pinata, \
                            nc.collection, nc.token_id, nc.token_new_owner_account_id as owner_id, \
                            split_part((nm.precio_near * 1000000000000000000000000)::varchar, '.', 1) as precio, \
                            nm.precio_near, c.base_uri, nc.titulo, nc.descripcion, nc.extra, nc.reference, \
                            nm.marketplace, m.name as market_name, m.icon as market_icon, m.web as market_web \
                        from nft_collections nc \
                        inner join collections c on c.nft_contract = nc.collection \
                        left join nft_marketplace nm on nm.collection = nc.collection and nm.token_id = nc.token_id \
                        left join marketplaces m on m.marketplace = nm.marketplace \
                        where nc.collection = $1 and ('%' = $2 or '' = $2 or nm.marketplace = $2) \
                        and ('%' = $3 or nc.token_id = $3 ) "
            
            //if(marketplace == "" || marketplace == "%") {
            switch (sales) {
                case true:
                    query += " and nm.precio_near  > 0 ";
                    break;
                case false:
                    query += " and nm.precio_near <= 0 ";
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
            
            res.json({excess: 0, data: data})
            conexion.end()
        //}, "5000")
        
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
                        (select case when c.flag_pinata = true then c.icon_pinata else c.icon end as icon  from collections c where nft_contract = $1) as icon, \
                        (select c.total_supply from collections c where nft_contract = $1) as supply, \
                        (select count(owner_id) as owner_id from ( \
                        select token_new_owner_account_id as owner_id from nft_collections nc \
                        where collection = $1 group by token_new_owner_account_id) sub ) as owner_for_tokens, \
                        (select sum(pricenear) \
                        from nft_buy t \
                        where collection = $1 ) as total_volumen ";
        
        const resultados = await conexion.query(query, [collection]);
        const data = resultados.rows;
        
        res.json(data)
        conexion.end()
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
        conexion.end()
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
        conexion.end()
    } catch (error) {
        res.error
    }
}

const ListNftOwner = async (req, res) => {
    try {
        const { owner, limit, index } = req.body;

        //setTimeout(async () => {
          
            if(parseInt(index) == 0) {
                const epoch_h = moment().subtract(30, 'm').valueOf()*1000000;
                await UpdateDataMarket(epoch_h)
            }

            const conexion = await dbConnect2();

            let query = "select \
                            nc.collection, nc.token_id, nc.token_new_owner_account_id as owner_id, (nm.precio_near*1000000000000000000000000)::text as precio, \
                            c.base_uri, nc.media, nc.titulo, nm.marketplace, m.name as market_name, m.icon as market_icon, m.web as market_web \
                        from nft_collections nc \
                        inner join collections c on c.nft_contract = nc.collection \
                        left join nft_marketplace nm on nm.collection = nc.collection and nm.token_id = nc.token_id \
                        left join marketplaces m on m.marketplace = nm.marketplace \
                        where nc.token_new_owner_account_id = $1 limit $2 offset $3 ";

            const resultados = await conexion.query(query, [owner, limit, index]);
            const data = resultados.rows;
            
            res.json(data)
            conexion.end()
        //}, "5000")
        
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}


const BulkList = async (req, res) => {
    try {
        const { owner, limit, index, listed } = req.body;

        //setTimeout(async () => {
            const conexion = await dbConnect2();

            if(parseInt(index) == 0) {
                const epoch_h = moment().subtract(30, 'm').valueOf()*1000000;
                await UpdateDataMarket(epoch_h)
            }

            let query = "   select  \
                                nc.collection, \
                                count(nc.token_id) as nft, \
                                sum(COALESCE(nm.precio_near, 0)) as total_price, \
                                min(COALESCE(nm.precio_near, 0)) as floor_price, \
                                case when c.flag_pinata = true then c.icon_pinata else c.icon end as icon, c.base_uri, c.name \
                            from nft_collections nc \
                            inner join collections c on c.nft_contract = nc.collection \
                            left join nft_marketplace nm on nm.collection = nc.collection and nm.token_id = nc.token_id and nm.precio_near > 0 \
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
                                nc.collection, c.icon, c.base_uri, nc.token_new_owner_account_id, c.name, c.flag_pinata, c.icon_pinata \
                            limit $2 offset $3 ";
            
            
            const resultados = await conexion.query(query, [owner, limit, index]);
            const data = resultados.rows;

            
            res.json(data)
            conexion.end()
        //}, "5000")
        
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}

const BulkListDetails = async (req, res) => {
    try {
        const { owner, collection, limit, index, listed } = req.body;

        //setTimeout(async () => {
            const conexion = await dbConnect2();

            if(parseInt(index) == 0) {
                const epoch_h = moment().subtract(30, 'm').valueOf()*1000000;
                await UpdateDataMarket(epoch_h)
            }

            let query = "   select  \
                                nc.collection, nc.titulo, nc.descripcion, nc.media, nc.token_id, \
                                COALESCE(nm.precio_near, 0) as price, \
                                nm.marketplace, m.name as market_name, m.icon as market_icon, m.web as market_web \
                            from nft_collections nc  \
                            left join nft_marketplace nm on nm.collection = nc.collection and nm.token_id = nc.token_id and nm.precio_near > 0 \
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
            conexion.end()
       // }, "5000")
        
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
        conexion.end()
        
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}

        
const ListNft3 = async (req, res) => {
    try {
        const { collection, tokenid, marketplace, limit, index, sales, order, type_order } = req.body;
        let excess = 0
        let array = []
        let queryGql 
        let variables
        let limite = parseInt(limit)
        let indice = parseInt(index)

        let var_sales = tokenid ? tokenid == "%" || tokenid == "" ? sales : "tokenid" : sales

        const conexion = await dbConnect2()
        //------------------------------delete market ---------------------------
        async function DeleteMarket() {
            const dm = await conexion.query("   select max(fecha) as fecha, token_id  \
            from nft_collections_transaction nct \
            where collection = $1 \
            group by token_id ", [collection]) //and token_id in ("+tokenDeletemarket+") \
            return dm.rows

        }
        //------------------------------------------------------------------------
        


        //--------------------ajustes data nft marketplace -----------------------
        async function DetailsCollectioMarket(nftmarketfinal, detail) {
            //---------------------------------data nft tokens---------------------------------
            let nftcolecionDetails = []
            let marketDetails = []

            if(detail == 'collection' || detail == 'todo') {
                const tokens = nftmarketfinal.length > 0 ? nftmarketfinal.map(item => {return item.tokenid}) : []
                let token = ''
                for(let i = 0; i < tokens.length; i++) {
                    token += "'"+tokens[i]+"'"
                    if(i != tokens.length -1) {
                        token += ","
                    }
                }
                const dt = nftmarketfinal.length == 0 ? {rows: []} : await conexion.query("   select nc.token_id as tokenid, nc.token_new_owner_account_id  as owner, titulo, descripcion, \
                                                                case when nc.flag_pinata = true then coalesce(nc.media_pinata, '') else coalesce(nc.media, '') end as media, \
                                                                coalesce(nc.extra, '') as extra, coalesce(nc.reference, '') as reference, coalesce(nc.media_pinata, '') as media_pinata, \
                                                                nc.collection, c.name as name_collection, coalesce(c.icon, '') as icon_collection, c.base_uri \
                                                            from nft_collections nc \
                                                            inner join collections c on c.nft_contract = nc.collection \
                                                            where collection = $1 \
                                                            and token_id in ("+token+") ", [collection])
                nftcolecionDetails = dt.rows
            }
            //-------------------------------------------------------------------------------------------------------
            
            //--------------------------------- data marketplace-----------------------------------------------------
            if(detail == 'market' || detail == 'todo') {
                const dmp = nftmarketfinal.length == 0 && detail != 'market' ? {rows: []} : await conexion.query(" select marketplace as market, name, icon, web from marketplaces ")
                marketDetails = dmp.rows
            }
            //--------------------------------------------------------------------------------------------------------

            return {nftcolecionDetails: nftcolecionDetails, marketDetails: marketDetails}
        }

        //------------------------------------------------------------------------

        //-----------------------data nft collection bd -------------------------
        async function nftCollectionBd(limt, offset) { 
            try {
                console.log("funcion bd")
                let var_tokenid = var_sales == "tokenid" ? tokenid : "%"
                console.log("algodon", var_tokenid)
                let query = "select collection, token_id, token_id as tokenid, owner_id, '' as precio, 0 as precio_near, base_uri, titulo, descripcion, \
                                media, media_pinata, \
                                extra, reference, '' as market_name, '' as marketplace, '' as market_icon, '' as market_web \
                    from ( \
                    select \
                        nc.collection, nc.token_id, nc.token_new_owner_account_id as owner_id, \
                        c.base_uri, nc.titulo, nc.descripcion, \
                        case when nc.flag_pinata = true then nc.media_pinata else nc.media end as media, \
                        nc.extra, nc.reference, \
                        coalesce(nc.media_pinata, '') as media_pinata \
                    from nft_collections nc \
                    inner join collections c on c.nft_contract = nc.collection \
                    where nc.collection = $1 and ('%' = $2 or nc.token_id = $2) "
                
                query += " ) sub "

                let typeOrder = type_order ? type_order : "asc"

                switch (order) {
                    case 'token_id':
                        query += " order by sub.token_id " + typeOrder;
                        break;
                    default:
                        query += "";
                        break;
                }

                query += " limit $3 offset $4 ";

                const resultados = await conexion.query(query, [collection, var_tokenid, limt, offset]);
                
                return resultados.rows
            } catch(err) {
                console.log(err)
                return []
            }
        }

        switch (var_sales) {
            case true: {
                const deleteMarket = await DeleteMarket()
                //---------------------------------------------------------------------------
                let limitOrigen = parseInt(limit)
                let nftmarketfinal = []
                let orderD = type_order ? type_order : "asc"

                while (100) {
                    if(limite > 999) { break }
                    if(marketplace == '%' || marketplace.trim() == '') {
                        queryGql = gql`
                            query MyQuery($collection: String!, $index: Int!, $limit: Int!, $orderD: String!) {
                                nftmarkets(skip: $index, first: $limit, where: { collection: $collection, pricenear_not: 0 }, orderBy: pricenear, orderDirection: $orderD) {
                                    id
                                    collection
                                    tokenid
                                    market
                                    price
                                    pricenear
                                    fecha 
                                }
                            }
                        `;
                        variables = { collection: collection, index: indice, limit: limite, orderD: orderD }    
                    } else {
                        queryGql = gql`
                            query MyQuery($collection: String!, $index: Int!, $limit: Int!, $market: String!, $orderD: String!) {
                                nftmarkets(skip: $index, first: $limit, where: { collection: $collection, pricenear_not: 0, market: $market }, orderBy: pricenear, orderDirection: $orderD) {
                                    id
                                    collection
                                    tokenid
                                    market
                                    price
                                    pricenear
                                    fecha 
                                }
                            }
                        `;
                        variables = { collection: collection, index: indice, limit: limite, market: marketplace, orderD: orderD }
                    }

                    const nftmarket = await colsutaGraph(queryGql, variables)
                    if(nftmarket.length == 0) { break }
                        
                    nftmarketfinal = []
                    nftmarket.forEach(item => {
                            const delte_market = deleteMarket.find(item2 => item2.token_id == item.tokenid && item2.fecha > item.fecha) 
                            if(delte_market == undefined ) {
                                nftmarketfinal.push(item)
                            }
                    })
                    if(nftmarketfinal.length == limitOrigen) {
                        break;
                    } else {
                        limite = (limite - nftmarketfinal.length) + limitOrigen
                        excess = limite - limitOrigen
                    }
                    //console.log(nftmarketfinal.length, limite)        
                }

                const details = await DetailsCollectioMarket(nftmarketfinal, 'todo')
                
                for(let i = 0; i < nftmarketfinal.length; i++){
                    const cd = details.nftcolecionDetails.find(item => item.tokenid == nftmarketfinal[i].tokenid)
                    const md = details.marketDetails.find(item => item.market == nftmarketfinal[i].market)
                    array.push({
                        collection: nftmarketfinal[i].collection,
                        token_id: nftmarketfinal[i].tokenid,
                        owner_id: cd.owner ? cd.owner : '',
                        precio: nftmarketfinal[i].price,
                        precio_near: nftmarketfinal[i].pricenear,
                        base_uri: cd.base_uri ? cd.base_uri : '',
                        titulo: cd.titulo ? cd.titulo : '',
                        descripcion: cd.descripcion ? cd.descripcion : '',
                        media: cd.media ? cd.media : '',
                        media_pinata: cd.media_pinata ? cd.media_pinata : '',
                        extra: cd.extra ? cd.extra : '',
                        reference: cd.reference ? cd.reference : '',
                        market_name: md.name ? md.name : '',
                        marketplace: nftmarketfinal[i].market,
                        market_icon: md.icon ? md.icon : '',
                        market_web: md.web ? md.web : ''
                    })
                }

            }
                break;
            case false: {
                let limitOrigen = parseInt(limit)
                let limite = parseInt(limit)
                let indice = parseInt(index)
                let nftCollectionBdFinal = []
                let DeleteNftCollection = []

                let count_consulta = 0
                let nftCollection_length = 0

                const deleteMarket = await DeleteMarket()

                while (10000) {
                    console.log('paso')
                    const nftCollection = await nftCollectionBd(limite, indice)
                    const tokens = nftCollection.length > 0 ? nftCollection.map(item => {return item.tokenid}) : []

                    queryGql = gql`
                        query MyQuery($collection: String!, $tokenid: [String]!) {
                            nftmarkets(where: { collection: $collection, pricenear_not: 0, tokenid_in: $tokenid }, 
                            orderBy: pricenear, orderDirection: asc) {
                                id
                                collection
                                tokenid
                                market
                                price
                                pricenear
                                fecha 
                            }
                        }
                    `;

                    if(nftCollection.length == 0) { break }
                    
                    if(count_consulta > 5 && nftCollection_length == nftCollection.length) { break }
                    nftCollection_length = nftCollection.length

                    variables = { collection: collection, tokenid: tokens } 
                    const nftmarket = await colsutaGraph(queryGql, variables)
                    
                    DeleteNftCollection = []
                    nftmarket.forEach(item => {
                            const delte_market = deleteMarket.find(item2 => item2.token_id == item.tokenid && item2.fecha > item.fecha) 
                            if(delte_market == undefined ) {
                                DeleteNftCollection.push(item)
                            }
                    })

                    nftCollectionBdFinal = []
                    nftCollection.forEach(item => {
                            const nftmarketDelete = DeleteNftCollection.find(item2 => item2.tokenid == item.tokenid) 
                            if(nftmarketDelete == undefined ) {
                                nftCollectionBdFinal.push(item)
                            }
                    })
                    if(nftCollectionBdFinal.length == limitOrigen) {
                        count_consulta = 0
                        break;
                    } else {
                        count_consulta += 1
                        limite = (limite - nftCollectionBdFinal.length) + limitOrigen
                        excess = limite - limitOrigen
                    }
                }
                array = nftCollectionBdFinal;
            }
            break;

            default: {
                const deleteMarket = await DeleteMarket()

                const nftCollection = await nftCollectionBd(1, 0)
                const tokens = nftCollection.length > 0 ? nftCollection.map(item => {return item.tokenid}) : []
                
                queryGql = gql`
                    query MyQuery($collection: String!, $tokenid: [String]!) {
                        nftmarkets(where: { collection: $collection, pricenear_not: 0, tokenid_in: $tokenid }, 
                        orderBy: pricenear, orderDirection: asc) {
                            id
                            collection
                            tokenid
                            market
                            price
                            pricenear
                            fecha 
                        }
                    }
                `;
                
                if(nftCollection.length == 0) { break }
                
                variables = { collection: collection, tokenid: tokens } 
                const nftmarket = await colsutaGraph(queryGql, variables)
                
                nft_market = []
                nftmarket.forEach(item => {
                        const delte_market = deleteMarket.find(item2 => item2.token_id == item.tokenid && item2.fecha > item.fecha) 
                        if(delte_market == undefined ) {
                            nft_market.push(item)
                        }
                })

                const details = await DetailsCollectioMarket([], "market")
                
                nftCollection.forEach(item => {
                    const findMarket = nft_market.filter(item2 => item2.tokenid == item.tokenid)
                    let market_name = ""
                    let marketplace = ""
                    let market_icon = ""
                    let market_web = ""
                    let precio = ""
                    let precio_near = 0
                    
                    function insert() {
                        array.push({
                            collection: item.collection,
                            token_id: item.tokenid,
                            owner_id: item.owner,
                            base_uri: item.base_uri,
                            titulo: item.titulo,
                            descripcion: item.descripcion,
                            media: item.media,
                            media_pinata: item.media_pinata,
                            extra: item.extra,
                            reference: item.reference,
                            market_name: market_name,
                            marketplace: marketplace,
                            market_icon: market_icon,
                            market_web: market_web,
                            precio: precio,
                            precio_near: precio_near
                        })
                    }
                    
                    if(findMarket.length > 0) {
                        console.log("si pasa")
                        findMarket.forEach(item3 => {
                            const md = details.marketDetails.find(item4 => item4.market == item3.market)
                            market_name = md.name ? md.name : ''
                            marketplace = item3.market
                            market_icon = md.icon ? md.icon : ''
                            market_web = md.web ? md.web : ''
                            precio = item3.price
                            precio_near = item3.pricenear
                            insert()    
                        })
                    } else {
                        insert()
                    }
                })
            }
                break;
        }
        
        //console.log(array)
        res.json({excess: excess, data: array})
        conexion.end()
    } catch (error) {
        console.log(error)
        res.json([])
    } 
}

const Collections = async (req, res) => {
    res.json([])
}

//RefrescarNft()

//update_collections()
//update_masivo_collections()


module.exports = { BuscarCollection, ListMarketplace, ListMarketplaceCollection,
                ListCollections, ListNft, CollectionDetails, BuyOnMarketplace, SearchNft, ListNftOwner,
                BulkList, BulkListDetails, Collections }





















