const { CONFIG } = require('../helpers/utils')
const { dbConnect, dbConnect2, dbConnect3 } = require('../../config/postgres')
const nearAPI = require("near-api-js");
const Web3 = require('web3');
const axios = require('axios');
const moment = require('moment');
const { colsutaGraph } = require('../graphql/dataThegraph')
const gql = require('graphql-tag');
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




const SalesOfTheDay = async (req, res) => {
    console.log(moment().format());
    console.log(moment().subtract(48, 'h').valueOf()*1000000)
    console.log(moment().subtract(24, 'h').valueOf()*1000000)
    console.log(moment().valueOf()*1000000)
    console.log('dia en curso ', moment(moment().format('DD/MM/YYYY'), 'DD/MM/YYYY').valueOf()*1000000)
    console.log('1 dia atras ', moment(moment().subtract(1, 'd').format('DD/MM/YYYY'), 'DD/MM/YYYY').valueOf()*1000000)
    console.log('2 dias atras ', moment(moment().subtract(2, 'd').format('DD/MM/YYYY'), 'DD/MM/YYYY').valueOf()*1000000)
    console.log('1 hora atras ', moment().subtract(1, 'h').valueOf()*1000000)
    console.log('24 horas atras ', moment(moment().subtract(24, 'h').format('DD/MM/YYYY'), 'DD/MM/YYYY').valueOf()*1000000)
    console.log('48 horas atras ', moment(moment().subtract(48, 'h').format('DD/MM/YYYY'), 'DD/MM/YYYY').valueOf()*1000000)
    console.log('24 horas atras ', moment().subtract(24, 'h').valueOf()*1000000)
    console.log('24 horas atras ', moment().subtract(48, 'h').valueOf()*1000000)
    console.log('7 dias atras ', moment().subtract(7, 'd').valueOf()*1000000)
    console.log('14 dias atras ', moment().subtract(14, 'd').valueOf()*1000000)
    try {
        const { top } = req.body
        //const top = 10
        const tiempoTranscurrido = moment(moment().subtract(24, 'h').format('DD/MM/YYYY'), 'DD/MM/YYYY').valueOf();
        const fecha2 = (new Date(new Date(tiempoTranscurrido).toLocaleDateString()).getTime()/1000.0)*1000000000;

        //const fecha = moment(moment().format('DD/MM/YYYY'), 'DD/MM/YYYY').valueOf()*1000000;
        const fecha = moment().subtract(24, 'h').valueOf()*1000000;
        
        console.log(fecha)
        console.log(fecha2)
        const conexion = await dbConnect2()
        
        const resultados = await conexion.query("   select \
                                                        to_timestamp(nsb.fecha::numeric/1000000000) as fecha, \
                                                        nsb.collection as nft_contract_id, \
                                                        nsb.tokenid as token_id, \
                                                        nsb.pricenear as price, \
                                                        c.name, \
                                                        c.symbol, \
                                                        c.icon, \
                                                        c.base_uri \
                                                    from nft_sellbuy nsb \
                                                    inner join collections c on c.nft_contract = nsb.collection \
                                                    where nsb.fecha > $1 \
                                                    order by nsb.pricenear desc \
                                                    limit $2 \
                                                ", [fecha, top])
        /*const arreglo = [];
        try {
            const datas = resultados.rows;
            for(var i = 0; i < datas.length; i++) {
                const contract = new Contract(account, datas[i].nft_contract_id, {
                    viewMethods: ['nft_metadata'],
                    sender: account
                })
        
                const response = await contract.nft_metadata()
                arreglo.push({
                    fecha: datas[i].fecha,
                    name: response.name,
                    token_id: datas[i].token_id,
                    symbol: response.symbol,
                    icon: response.icon,
                    base_uri: response.base_uri,
                    nft_contract_id: datas[i].nft_contract_id,
                    price: datas[i].price
                });

            }
        } catch (error) {
            console.log('error 2: ', error)
            res.error
        }*/
        
        res.json(resultados.rows)
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: 'error', error: error})
    }
}

const HighestVOLGainers = async (req, res) => {
    try {
        const { top } = req.body;
        const fecha24h = moment().subtract(24, 'h').valueOf()*1000000;
        const fecha48h = moment().subtract(48, 'h').valueOf()*1000000;
        console.log('24 horas atras ', fecha24h);
        console.log('48 horas atras ', fecha48h);
        const conexion = await dbConnect2();

        /*const resultados = await conexion.query("select \
                                                    fecha, \
                                                    nft_contract_id, \
                                                    volumen24h, \
                                                    volumen48h, \
                                                    porcentaje, \
                                                    c.name, \
                                                    c.symbol, \
                                                    c.icon, \
                                                    c.base_uri \
                                                from ( \
                                                        SELECT \
                                                            max(to_timestamp(x.receipt_included_in_block_timestamp::numeric/1000000000)) as fecha, \
                                                            x.receipt_receiver_account_id as nft_contract_id, \
                                                            sum(cast(cast(convert_from(decode(x.args->>'args_base64', 'base64'), 'UTF8') as json)->> 'balance' as numeric) / 1000000000000000000000000) as volumen24h, \
                                                            max(sub.volumen48h) as volumen48h, \
                                                            ((sum(cast(cast(convert_from(decode(x.args->>'args_base64', 'base64'), 'UTF8') as json)->> 'balance' as numeric) / 1000000000000000000000000) / max(sub.volumen48h)) * 100) - 100 as porcentaje \
                                                        FROM transaction x \
                                                        inner join ( \
                                                            SELECT \
                                                                x.receipt_receiver_account_id as nft_contract_id, \
                                                                sum(cast(cast(convert_from(decode(x.args->>'args_base64', 'base64'), 'UTF8') as json)->> 'balance' as numeric) / 1000000000000000000000000) as volumen48h \
                                                            FROM transaction x \
                                                            where \
                                                                x.receipt_included_in_block_timestamp > $1 \
                                                                and x.receipt_included_in_block_timestamp < $2 \
                                                                and method_name = 'nft_transfer_payout' \
                                                                and receipt_predecessor_account_id in (select marketplace from marketplaces) \
                                                            group by \
                                                                x.receipt_receiver_account_id \
                                                        ) sub on x.receipt_receiver_account_id = sub.nft_contract_id \
                                                        where \
                                                            x.receipt_included_in_block_timestamp > $3 \
                                                            and method_name = 'nft_transfer_payout' \
                                                            and receipt_predecessor_account_id in (select marketplace from marketplaces) \
                                                        group by \
                                                            x.receipt_receiver_account_id \
                                                    ) a \
                                                inner join collections c on c.nft_contract = a.nft_contract_id \
                                                where \
                                                    volumen24h > 0 \
                                                    and volumen48h > 0 \
                                                order by porcentaje desc \
                                                limit $4 \
                                                    ", [fecha48h, fecha24h, fecha24h, top]);*/
        const resultados = await conexion.query(" select \
                            fecha, nft_contract_id, volumen24h, volumen48h, porcentaje, c.name, c.symbol, c.icon, c.base_uri \
                        from ( \
                                SELECT \
                                    max(to_timestamp(x.fecha::numeric/1000000000)) as fecha, \
                                    x.collection as nft_contract_id, \
                                    sum(x.pricenear) as volumen24h, \
                                    max(sub.volumen48h) as volumen48h, \
                                    ((sum(x.pricenear) / max(sub.volumen48h)) * 100) - 100 as porcentaje \
                                FROM nft_sellbuy x \
                                inner join ( \
                                    SELECT \
                                        x.collection, \
                                        sum(x.pricenear) as volumen48h \
                                    FROM nft_sellbuy x \
                                    where \
                                        x.fecha > $1 \
                                        and x.fecha < $2 \
                                        and market in (select marketplace from marketplaces) \
                                    group by \
                                        x.collection \
                                ) sub on x.collection = sub.collection \
                                where \
                                    x.fecha > $2  \
                                    and market in (select marketplace from marketplaces) \
                                group by \
                                    x.collection \
                            ) a \
                        inner join collections c on c.nft_contract = a.nft_contract_id \
                        where \
                            volumen24h > 0 \
                            and volumen48h > 0 \
                        order by porcentaje desc \
                        limit $3 ", [fecha48h, fecha24h, top]);
        
        const datas = resultados.rows;
        res.json(datas)
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}

const Volumen24h = async (req, res) => {
    try {
        const fecha24h = moment().subtract(24, 'h').valueOf()*1000000;
        const fecha48h = moment().subtract(48, 'h').valueOf()*1000000;
        console.log(fecha24h)
        console.log(fecha48h)
        const conexion = await dbConnect2()
        
        /*const resultados = await conexion.query("select \
                                                    volumen24h, \
                                                    volumen48h, \
                                                    ((volumen24h / volumen48h) * 100) - 100 as porcentaje \
                                                from ( \
                                                        select \
                                                            (SELECT \
                                                                sum(cast(cast(convert_from(decode(x.args->>'args_base64', 'base64'), 'UTF8') as json)->> 'balance' as numeric) / 1000000000000000000000000) as volumen \
                                                            FROM transaction x \
                                                            where \
                                                                x.receipt_included_in_block_timestamp > $1 \
                                                                and method_name = 'nft_transfer_payout' \
                                                                and receipt_predecessor_account_id in (select marketplace from marketplaces) \
                                                            ) as volumen24h, \
                                                            (SELECT \
                                                                    sum(cast(cast(convert_from(decode(x.args->>'args_base64', 'base64'), 'UTF8') as json)->> 'balance' as numeric) / 1000000000000000000000000) as volumen48h \
                                                                FROM transaction x \
                                                                where \
                                                                    x.receipt_included_in_block_timestamp > $2 \
                                                                    and x.receipt_included_in_block_timestamp < $3 \
                                                                    and method_name = 'nft_transfer_payout' \
                                                                    and receipt_predecessor_account_id in (select marketplace from marketplaces) \
                                                            ) as volumen48h \
                                                    ) sub \
                                                ", [fecha24h, fecha48h, fecha24h])*/
        const resultados = await conexion.query("   select \
                                                        volumen24h, \
                                                        volumen48h, \
                                                        ((volumen24h / volumen48h) * 100) - 100 as porcentaje \
                                                    from ( \
                                                            select \
                                                                (SELECT \
                                                                    sum(x.pricenear) as volumen \
                                                                FROM nft_sellbuy x \
                                                                where \
                                                                    x.fecha > $1  \
                                                                    and market in (select marketplace from marketplaces) \
                                                                ) as volumen24h, \
                                                                (SELECT \
                                                                        sum(x.pricenear) as volumen48h \
                                                                    FROM nft_sellbuy x \
                                                                    where \
                                                                        x.fecha > $2 \
                                                                        and x.fecha < $1 \
                                                                        and market in (select marketplace from marketplaces) \
                                                                ) as volumen48h \
        ) sub", [fecha24h, fecha48h])
                        
        res.json(resultados.rows)
    } catch (error) {
        res.error
    }
}

const Volumen7d = async (req, res) => {
    try {
        const fecha7d = moment().subtract(7, 'd').valueOf()*1000000;
        const fecha14d = moment().subtract(14, 'd').valueOf()*1000000;
        console.log(fecha7d)
        console.log(fecha14d)
        const conexion = await dbConnect2()
        /*const resultados = await conexion.query("select \
                                                    volumen7d, \
                                                    volumen14d, \
                                                    ((volumen7d / volumen14d) * 100) - 100 as porcentaje \
                                                from ( \
                                                        select \
                                                            (SELECT \
                                                                sum(cast(cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->> 'balance' as numeric) / 1000000000000000000000000) as volumen \
                                                            FROM transaction x \
                                                            where \
                                                                x.receipt_included_in_block_timestamp > $1 \
                                                                and method_name = 'nft_transfer_payout' \
                                                                and receipt_predecessor_account_id in (select marketplace from marketplaces) \
                                                            ) as volumen7d, \
                                                            (SELECT \
                                                                    sum(cast(cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->> 'balance' as numeric) / 1000000000000000000000000) as volumen14d \
                                                                FROM transaction x \
                                                                where \
                                                                    x.receipt_included_in_block_timestamp > $2 \
                                                                    and x.receipt_included_in_block_timestamp < $3 \
                                                                    and method_name = 'nft_transfer_payout' \
                                                                    and receipt_predecessor_account_id in (select marketplace from marketplaces) \
                                                            ) as volumen14d \
                                                    ) sub \
                                                ", [fecha7d, fecha14d, fecha7d])*/
        
        const resultados = await conexion.query("   select \
                                                        volumen7d, \
                                                        volumen14d, \
                                                        ((volumen7d / volumen14d) * 100) - 100 as porcentaje \
                                                    from ( \
                                                            select \
                                                                (SELECT \
                                                                    sum(x.pricenear) as volumen \
                                                                FROM nft_sellbuy x \
                                                                where \
                                                                    x.fecha > $1  \
                                                                    and market in (select marketplace from marketplaces) \
                                                                ) as volumen7d, \
                                                                (SELECT \
                                                                        sum(x.pricenear) as volumen14d \
                                                                    FROM nft_sellbuy x \
                                                                    where \
                                                                        x.fecha > $2 \
                                                                        and x.fecha < $1 \
                                                                        and market in (select marketplace from marketplaces) \
                                                                ) as volumen14d \
        ) sub", [fecha7d, fecha14d])
                        //and 1 = (select case when (select 1 from collections c where c.nft_contract = x.receipt_receiver_account_id) = 1 then 1 else 0 end) \
        res.json(resultados.rows)
    } catch (error) {
        res.error
    }
}


const BuscarCollection = async (req, res) => {
    try {
        const { search, top } = req.body;
        const conexion = await dbConnect2()
        const resultados = await conexion.query("   select \
                                                        c.nft_contract, c.owner_id, c.name, c.symbol, c.icon, c.base_uri , c.reference, c.fecha_creacion, c.total_supply \
                                                    from collections c \
                                                    where upper(c.name) like '%'||upper($1)||'%' \
                                                    limit $2 \
                                                ", [search, top])
                        
        res.json(resultados.rows)
    } catch (error) {
        res.error
    }
}


const Ranking = async (req, res) => {
    try {
        const { horas_vol, horas_floor, top, order, collection, owner } = req.body;

        const fecha1 = moment().subtract(horas_vol, 'h').valueOf()*1000000;
        const fecha2 = moment().subtract(horas_vol * 2, 'h').valueOf()*1000000;
        const fecha3 = moment().subtract(horas_floor, 'h').valueOf()*1000000;
        console.log('horas 1 atras ', fecha1);
        console.log('horas 2 atras ', fecha2);
        console.log('horas 3 atras ', fecha3);
        const conexion = await dbConnect2();
        
        let query = ""
        /*query = "select \
                    c.nft_contract, c.name, c.symbol, c.icon, c.base_uri , c.reference, c.fecha_creacion, c.total_supply as supply, \
                    COALESCE(vol.volumen1,0) as volumen1, \
                    COALESCE(subfloor.floor_price, 0) as floor_price, \
                    COALESCE(vol.porcentaje, 0) porcentaje, \
                    COALESCE(voto.positivo,0) as voto_positivo, \
                    COALESCE(voto.negativo,0) as voto_negativo, \
                    coalesce((select true from nft_collections nc where token_new_owner_account_id = $8 and collection = c.nft_contract group by true), false) as permission_voto_nega, \
                    (select count(owner_id) \
                    from (select distinct token_new_owner_account_id as owner_id  \
                    from nft_collections nc where collection = c.nft_contract) sub4) as owner_for_tokens \
                from collections c \
                left join ( \
                            select \
                                nft_contract, \
                                volumen1, \
                                case \
                                    when volumen1 > 0 and volumen2 > 0 \
                                    then ((volumen1 / volumen2) * 100) - 100 \
                                    else 0 \
                                end as porcentaje \
                            from ( \
                                    SELECT \
                                        x.receipt_receiver_account_id as nft_contract, \
                                        sum(cast(cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->> 'balance' as numeric) / 1000000000000000000000000) as volumen1, \
                                        max(sub.volumen2) as volumen2 \
                                    FROM transaction x \
                                    inner join ( \
                                                    SELECT \
                                                        x.receipt_receiver_account_id as nft_contract, \
                                                        sum(cast(cast(convert_from(decode(x.args->>'args_base64', 'base64'), 'UTF8') as json)->> 'balance' as numeric) / 1000000000000000000000000) as volumen2 \
                                                    FROM transaction x \
                                                    where \
                                                        x.receipt_included_in_block_timestamp > $1 \
                                                        and x.receipt_included_in_block_timestamp < $2 \
                                                        and method_name = 'nft_transfer_payout' \
                                                        and receipt_predecessor_account_id in (select marketplace from marketplaces) \
                                                    group by \
                                                        x.receipt_receiver_account_id \
                                    ) sub on x.receipt_receiver_account_id = sub.nft_contract \
                                    where \
                                        x.receipt_included_in_block_timestamp > $3 \
                                        and method_name = 'nft_transfer_payout' \
                                        and receipt_predecessor_account_id in (select marketplace from marketplaces) \
                                    group by \
                                        x.receipt_receiver_account_id \
                            ) a \
                ) vol on c.nft_contract = vol.nft_contract \
                left join ( \
                            SELECT \
                                x.receipt_receiver_account_id as nft_contract, \
                                min(cast(cast(convert_from(decode(x.args->>'args_base64', 'base64'), 'UTF8') as json)->> 'balance' as numeric) / 1000000000000000000000000) as floor_price \
                            FROM transaction x \
                            where \
                                x.receipt_included_in_block_timestamp > $4 \
                                and method_name = 'nft_transfer_payout' \
                                and receipt_predecessor_account_id in (select marketplace from marketplaces) \
                            group by \
                                x.receipt_receiver_account_id \
                ) subfloor on c.nft_contract = subfloor.nft_contract \
                left join ( \
                    select \
                        collection, \
                        sum(case when voto = 'true' then 1 else 0 end) as positivo, \
                        sum(case when voto = 'false' then 1 else 0 end) as negativo \
                    from votos v \
                    group by collection \
                ) voto on c.nft_contract = voto.collection \
                where ('%' = $5 or c.nft_contract = $6) ";*/
        
        query = "   select \
                        c.nft_contract, c.name, c.symbol, c.icon, c.base_uri , c.reference, c.fecha_creacion, c.total_supply as supply, \
                        COALESCE(vol.volumen1,0) as volumen1, \
                        COALESCE(subfloor.floor_price, 0) as floor_price, \
                        COALESCE(vol.porcentaje, 0) porcentaje, \
                        COALESCE(voto.positivo,0) as voto_positivo, \
                        COALESCE(voto.negativo,0) as voto_negativo, \
                        coalesce((select true from nft_collections nc where token_new_owner_account_id = $6 and collection = c.nft_contract group by true), false) as permission_voto_nega, \
                        (select count(owner_id) \
                        from (select distinct token_new_owner_account_id as owner_id  \
                        from nft_collections nc where collection = c.nft_contract) sub4) as owner_for_tokens \
                    from collections c \
                    left join ( \
                                select \
                                    nft_contract, \
                                    volumen1, \
                                    case \
                                        when volumen1 > 0 and volumen2 > 0 \
                                        then ((volumen1 / volumen2) * 100) - 100 \
                                        else 0 \
                                    end as porcentaje \
                                from ( \
                                        SELECT \
                                            x.collection as nft_contract, \
                                            sum(x.pricenear) as volumen1, \
                                            max(sub.volumen2) as volumen2 \
                                        FROM nft_sellbuy x \
                                        inner join ( \
                                                        SELECT \
                                                            x.collection as nft_contract, \
                                                            sum(x.pricenear) as volumen2 \
                                                        FROM nft_sellbuy x \
                                                        where \
                                                            x.fecha > $1 \
                                                            and x.fecha < $2  \
                                                            and market in (select marketplace from marketplaces) \
                                                        group by \
                                                            x.collection \
                                        ) sub on x.collection = sub.nft_contract \
                                        where \
                                            x.fecha > $2 \
                                            and market in (select marketplace from marketplaces) \
                                        group by \
                                            x.collection \
                                ) a \
                    ) vol on c.nft_contract = vol.nft_contract \
                    left join ( \
                                SELECT \
                                    x.collection as nft_contract, \
                                    min(x.pricenear) as floor_price \
                                FROM nft_sellbuy x \
                                where \
                                    x.fecha > $3 \
                                    and market in (select marketplace from marketplaces) \
                                group by \
                                    x.collection \
                    ) subfloor on c.nft_contract = subfloor.nft_contract \
                    left join ( \
                        select \
                            collection, \
                            sum(case when voto = 'true' then 1 else 0 end) as positivo, \
                            sum(case when voto = 'false' then 1 else 0 end) as negativo \
                        from votos v \
                        group by collection \
                    ) voto on c.nft_contract = voto.collection \
                    where ('%' = $4 or c.nft_contract = $4) "

        switch (order) {
            case "best":
                query += " order by COALESCE(vol.volumen1,0) desc ";       
                break;
            case "volumen":
                query += " order by COALESCE(vol.volumen1,0) desc ";       
                break;
            case "floor":
                query += " order by COALESCE(subfloor.floor_price,0) asc ";       
                break;
            //default:
                //break;
        }
        
        //query += " limit $7 ";
        query += " limit $5 ";

        //const resultados = await conexion.query(query, [fecha2, fecha1, fecha1, fecha3, collection, collection, top, owner]);
        const resultados = await conexion.query(query, [fecha2, fecha1, fecha3, collection, top, owner]);

        res.json(resultados.rows)
    } catch (error) {
        console.log('error 1: ', error)
        res.error
    }
}


const UpcomingListed = async (req, res) => {
    try {
        const { top, order } = req.body;

        const fecha = moment().unix();

        const conexion2 = await dbConnect2();
        
        
        let query = "   select \
                            id_contract_project as collection, project_name, descripcion, fecha_lanzamiento, website, \
                            twiter, telegram, discord_id, discord_server, COALESCE(sub.positivo,0) as voto \
                        from projects p \
                        left join (select collection, \
                                        sum(case when voto = 'true' then 1 else 0 end) as positivo \
                                    from votos_upcoming vu \
                                    group by collection \
                        ) sub on sub.collection = p.id_contract_project \
                        where upcoming = true and already = false and fecha_lanzamiento >= $1 "
        
        if(order) {
            switch (order) {
                case 'voto':
                    query += " ";
                    break;
                
                case 'fecha':
                    query += " order by fecha_lanzamiento asc ";
                    break;
            }
        }   
        
        query += " limit $2 ";
        
        const respuesta = await conexion2.query(query, [fecha, top])

        res.json(respuesta.rows)
    
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: false, error: error})
    }
}

const NewProjectsListed = async (req, res) => {
    try {
        const { top, order } = req.body;

        const fecha = moment().unix();

        const conexion2 = await dbConnect2();
        
        
        let query = "   select \
                            nft_contract, total_supply, fecha_listado, name, \
                            symbol, icon, base_uri, reference, price, fecha_creacion, voto, fecha_lanzamiento \
                        from ( \
                            select c.nft_contract, c.total_supply, p.fecha as fecha_listado, c.name, \
                                c.symbol, c.icon, c.base_uri, c.reference, coalesce(cmp.precio, 0) as price, \
                                c.fecha_creacion, COALESCE(sub.positivo,0) as voto, fecha_lanzamiento \
                            from projects p \
                            inner join collections c on c.nft_contract = p.id_contract_project \
                            left join (select min(cast(precio as numeric)/1000000000000000000000000) as precio, \
                                        collection \
                                        from nft_marketplace \
                                        group by collection \
                            ) cmp on cmp.collection = p.id_contract_project \
                            left join (select collection, \
                                        sum(case when voto = 'true' then 1 else 0 end) as positivo \
                                        from votos vu \
                                        group by collection \
                            ) sub on sub.collection = p.id_contract_project \
                            where p.already = true \
                        ) sub "
                            //--and p.fecha_creacion > $1
                            // "
                        
        
        if(order) {
            switch (order) {
                case 'voto':
                    query += " order by voto desc ";
                    break;
                
                case 'fecha':
                    query += " order by fecha_lanzamiento asc ";
                    break;
                
                case 'supply':
                    query += " order by total_supply desc ";
                    break;

                case 'price':
                    query += " order by price asc ";
                    break;
            }
        }   
        
        query += " limit $1 ";
        
        const respuesta = await conexion2.query(query, [top])

        res.json(respuesta.rows)
    
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: false, error: error})
    }
}

const ActiveWalleHeader = async (req, res) => {
    try {
        const { wallet } = req.body;

        const conexion2 = await dbConnect2();
        
        const fecha = moment().subtract(24, 'h').valueOf()*1000000;

        let query = "   select \
                            (select coalesce(json_agg(jsonlist), '[]') \
                            from (select buyer as wallet, sum(pricenear) as volumen \
                            from nft_sellbuy \
                            where fecha >= $1 \
                            group by buyer \
                            order by sum(pricenear) desc \
                            limit 1) jsonlist) as largest_buyer, \
                            (select coalesce(json_agg(jsonlist), '[]') \
                            from (select seller as wallet, sum(pricenear) as volumen \
                            from nft_sellbuy \
                            where fecha >= $1 \
                            group by seller \
                            order by sum(pricenear) desc \
                            limit 1) jsonlist) as largest_seller, \
                            (select coalesce(json_agg(jsonlist), '[]') \
                            from (select seller as wallet, pricenear as price \
                            from nft_sellbuy \
                            where fecha >= $1 \
                            order by pricenear desc \
                            limit 1) jsonlist) as highest_sale, \
                            (select coalesce(json_agg(jsonlist), '[]') \
                            from (select x.wallet, x.ranking \
                            from ( \
                                select \
                                    ROW_NUMBER () OVER (ORDER BY sub2.total_gastado desc) as ranking, \
                                    sub2.wallet, \
                                    sub2.total_gastado, \
                                    sub2.total_comprado \
                                from ( \
                                    select \
                                        sub.wallet, \
                                        sub.total_gastado, \
                                        sub.total_comprado \
                                    from ( \
                                        select \
                                            sum(t.pricenear) as total_gastado, \
                                            count(t.pricenear) as total_comprado, \
                                            t.buyer as wallet \
                                        from nft_sellbuy t  \
                                        group by \
                                            t.buyer \
                                    ) sub  \
                                ) sub2 \
                                order by \
                                    sub2.total_gastado desc \
                            ) x \
                            where x.wallet = $2 \
                            ) jsonlist) as your_rankinfo "

        const result = await conexion2.query(query, [fecha, wallet])
        console.log(result.rows)
        res.json(result.rows)

    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: false, error: error})
    }
}


const ActiveWallets = async (req, res) => {
    try {
        const { wallet, limit, index, value, time } = req.body;

        const conexion2 = await dbConnect2();
        
        const fecha = moment().subtract(value, time).valueOf()*1000000;

        console.log(fecha);

        /*let query = "   select \
                            sub2.wallet, \
                            sub2.total_gastado, \
                            sub2.total_comprado, \
                            sub2.mayor_compra \
                        from ( \
                            select \
                                sub.wallet, \
                                sub.total_gastado, \
                                sub.total_comprado, \
                                (select coalesce(json_agg(jsonlist), '[]') \
                                from (select  \
                                    nc.collection, c.name as collection_name, nc.token_id as token_id, nc.media as media, nc.titulo as titulo, \
                                    cast(cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'balance' as numeric) / 1000000000000000000000000 as precio \
                                from transaction t \
                                inner join nft_collections nc on nc.collection = t.receipt_receiver_account_id and nc.token_id = cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'token_id' \
                                inner join collections c on c.nft_contract = nc.collection \
                                where receipt_included_in_block_timestamp >= $3 \
                                and t.method_name = 'nft_transfer_payout' \
                                and cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'receiver_id' = sub.wallet \
                                order by \
                                    (cast(cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'balance' as numeric) / 1000000000000000000000000) desc \
                                limit 1) jsonlist) as mayor_compra \
                            from ( \
                                select \
                                    sum(cast(cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'balance' as numeric) / 1000000000000000000000000) as total_gastado, \
                                    count(cast(cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'balance' as numeric) / 1000000000000000000000000) as total_comprado, \
                                    cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'receiver_id' as wallet \
                                from transaction t \
                                where receipt_included_in_block_timestamp >= $3 \
                                and t.method_name = 'nft_transfer_payout' \
                                group by \
                                    cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'receiver_id' \
                            ) sub \
                        ) sub2 \
                        where \
                            cast(sub2.mayor_compra as text) <> '[]' "*/

        /*let query2 = "  select \
                            count(sub2.wallet) as array_size \
                        from ( \
                            select  \
                                sub.wallet, \
                                (select coalesce(json_agg(jsonlist), '[]') \
                                from (select  \
                                    nc.collection, c.name as collection_name \
                                from transaction t \
                                inner join nft_collections nc on nc.collection = t.receipt_receiver_account_id and nc.token_id = cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'token_id' \
                                inner join collections c on c.nft_contract = nc.collection \
                                where receipt_included_in_block_timestamp >= $1 \
                                and t.method_name = 'nft_transfer_payout' \
                                and t.receipt_receiver_account_id <> 'x.paras.near' \
                                and cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'receiver_id' = sub.wallet  \
                                limit 1) jsonlist) as mayor_compra \
                            from ( \
                                select  \
                                    cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'receiver_id' as wallet \
                                from transaction t \
                                where receipt_included_in_block_timestamp >= $1 \
                                and t.method_name = 'nft_transfer_payout' \
                                and t.receipt_receiver_account_id <> 'x.paras.near' \
                                group by \
                                    cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'receiver_id' \
                            ) sub \
                        ) sub2 \
                        where \
                            cast(sub2.mayor_compra as text) <> '[]' "*/
        
        let query = "   select \
                            fin.wallet, \
                            fin.total_gastado, \
                            fin.total_comprado, \
                            case when cast(fin.mayor_compra as text) <> '[]' then fin.mayor_compra \
                                else (select coalesce(json_agg(jsonlist), '[]') \
                                        from (select  \
                                            nc.collection, c.name as collection_name, nc.token_id as token_id, nc.media as media, nc.titulo as titulo, \
                                            t.pricenear as precio \
                                        from nft_sellbuy t \
                                        inner join nft_collections nc on nc.collection = t.collection and nc.token_id = t.tokenid \
                                        inner join collections c on c.nft_contract = nc.collection \
                                        where t.buyer = fin.wallet \
                                        order by \
                                            t.pricenear  desc \
                                        limit 1) jsonlist) \
                            end mayor_compra, \
                            fin.total_ganado, \
                            fin.total_vendido, \
                            case when cast(fin.mayor_venta as text) <> '[]' then fin.mayor_venta \
                                else (select coalesce(json_agg(jsonlist), '[]')  \
                                        from (select  \
                                            nc.collection, c.name as collection_name, nc.token_id as token_id, nc.media as media, nc.titulo as titulo, \
                                            t.pricenear as precio \
                                        from nft_sellbuy t \
                                        inner join nft_collections nc on nc.collection = t.collection and nc.token_id = t.tokenid \
                                        inner join collections c on c.nft_contract = nc.collection \
                                        where t.seller = fin.wallet \
                                        order by \
                                            t.pricenear  desc \
                                        limit 1) jsonlist) \
                            end mayor_venta \
                        from ( \
                            select \
                                sub.wallet, \
                                sub.total_gastado, \
                                sub.total_comprado, \
                                (select coalesce(json_agg(jsonlist), '[]') \
                                from (select  \
                                    nc.collection, c.name as collection_name, nc.token_id as token_id, nc.media as media, nc.titulo as titulo, \
                                    t.pricenear as precio \
                                from nft_sellbuy t \
                                inner join nft_collections nc on nc.collection = t.collection and nc.token_id = t.tokenid \
                                inner join collections c on c.nft_contract = nc.collection \
                                where t.fecha >= $3 \
                                and t.buyer = sub.wallet \
                                order by \
                                    t.pricenear  desc \
                                limit 1) jsonlist) as mayor_compra, \
                                coalesce(sub2.total_ganado, 0) as total_ganado, \
                                coalesce(sub2.total_vendido, 0) as total_vendido, \
                                (select coalesce(json_agg(jsonlist), '[]') \
                                from (select  \
                                    nc.collection, c.name as collection_name, nc.token_id as token_id, nc.media as media, nc.titulo as titulo, \
                                    t.pricenear as precio \
                                from nft_sellbuy t \
                                inner join nft_collections nc on nc.collection = t.collection and nc.token_id = t.tokenid \
                                inner join collections c on c.nft_contract = nc.collection \
                                where t.fecha >= $3 \
                                and t.seller = sub.wallet \
                                order by \
                                    t.pricenear  desc \
                                limit 1) jsonlist) as mayor_venta \
                            from ( \
                                select \
                                    sum(t.pricenear) as total_gastado, \
                                    count(t.pricenear) as total_comprado, \
                                    t.buyer as wallet \
                                from nft_sellbuy t \
                                where t.fecha >= $3 \
                                group by \
                                    t.buyer \
                            ) sub \
                            left join ( \
                                select \
                                    sum(t.pricenear) as total_ganado, \
                                    count(t.pricenear) as total_vendido, \
                                    t.seller as wallet \
                                from nft_sellbuy t \
                                where t.fecha >= $3 \
                                group by \
                                    t.seller \
                            ) sub2 on sub2.wallet = sub.wallet \
                        ) fin  "
        
        let query2 = "  select \
                            count(sub2.wallet) as array_size \
                        from ( \
                            select \
                                sub.wallet \
                            from ( \
                                select \
                                    sum(t.pricenear) as total_gastado, \
                                    count(t.pricenear) as total_comprado, \
                                    t.buyer as wallet \
                                from nft_sellbuy t \
                                where t.fecha >= $1 \
                                group by \
                                    t.buyer \
                            ) sub \
                        ) sub2 "

        switch (wallet) {
            case "%":
                {
                    query += "  order by \
                                    fin.total_gastado desc \
                                limit $1 offset $2 ";
                    
                    const respuesta = await conexion2.query(query, [limit, index, fecha])
                    const rows_count = await conexion2.query(query2, [fecha])

                    res.json({rows_count: rows_count.rows[0].array_size, response: respuesta.rows})
                }
                break;
        
            default:
                {
                    query += "  where upper(fin.wallet) like $4 \
                                order by \
                                    fin.total_gastado desc \
                                limit $1 offset $2 ";
                    
                    query2 += "  where upper(sub2.wallet) like $2 ";

                    let account_id = "%"+wallet.toUpperCase()+"%"
                    const respuesta = await conexion2.query(query, [limit, index, fecha, account_id])
                    const rows_count = await conexion2.query(query2, [fecha, account_id])

                    res.json({rows_count: rows_count.rows[0].array_size, response: respuesta.rows})
                }
                break;
        }
    
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: false, error: error})
    }
}

const ActiveWalletsMarket = async (req, res) => {
    try {
        const { wallet, limit, index, value, time } = req.body;

        const conexion2 = await dbConnect2();
        
        const fecha = moment().subtract(value, time).valueOf()*1000000;

        console.log(fecha)

        /*let query = "   select \
                            sub2.marketplace, \
                            sub2.market_name, \
                            sub2.market_icon, \
                            sub2.market_web, \
                            sub2.wallet, \
                            sub2.total_gastado, \
                            sub2.total_comprado, \
                            sub2.mayor_compra \
                        from ( \
                            select \
                                sub.marketplace, \
                                ma.name as market_name, \
                                ma.icon as market_icon, \
                                ma.web as market_web, \
                                sub.wallet, \
                                sub.total_gastado, \
                                sub.total_comprado, \
                                (select coalesce(json_agg(jsonlist), '[]') \
                                from (select  \
                                    nc.collection, c.name as collection_name, nc.token_id as token_id, nc.media as media, nc.titulo as titulo, \
                                    cast(cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'balance' as numeric) / 1000000000000000000000000 as precio \
                                from transaction t \
                                inner join nft_collections nc on nc.collection = t.receipt_receiver_account_id and nc.token_id = cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'token_id' \
                                inner join collections c on c.nft_contract = nc.collection \
                                where t.receipt_included_in_block_timestamp >= $3 \
                                and t.method_name = 'nft_transfer_payout' \
                                and t.receipt_predecessor_account_id = sub.marketplace \
                                and cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'receiver_id' = sub.wallet \
                                order by \
                                    (cast(cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'balance' as numeric) / 1000000000000000000000000) desc \
                                limit 1) jsonlist) as mayor_compra \
                            from ( \
                                select \
                                    t.receipt_predecessor_account_id as marketplace, \
                                    sum(cast(cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'balance' as numeric) / 1000000000000000000000000) as total_gastado, \
                                    count(cast(cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'balance' as numeric) / 1000000000000000000000000) as total_comprado, \
                                    cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'receiver_id' as wallet \
                                from transaction t \
                                where t.receipt_included_in_block_timestamp >= $3 \
                                and t.method_name = 'nft_transfer_payout' \
                                group by \
                                    t.receipt_predecessor_account_id, \
                                    cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'receiver_id' \
                            ) sub \
                            inner join marketplaces ma on ma.marketplace = sub.marketplace \
                        ) sub2 \
                        where cast(sub2.mayor_compra as text) <> '[]' "*/
        
        /*let query2 = " select \
                            count(sub2.wallet) as array_size \
                        from ( \
                            select \
                                sub.marketplace, \
                                sub.wallet, \
                                (select coalesce(json_agg(jsonlist), '[]') \
                                from (select  \
                                    nc.collection \
                                from transaction t \
                                inner join nft_collections nc on nc.collection = t.receipt_receiver_account_id and nc.token_id = cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'token_id' \
                                where t.receipt_included_in_block_timestamp >= $1 \
                                and t.method_name = 'nft_transfer_payout' \
                                and t.receipt_predecessor_account_id = sub.marketplace \
                                and cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'receiver_id' = sub.wallet  \
                                limit 1) jsonlist) as mayor_compra \
                            from ( \
                                select \
                                    t.receipt_predecessor_account_id as marketplace, \
                                    cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'receiver_id' as wallet \
                                from transaction t \
                                where t.receipt_included_in_block_timestamp >= $1 \
                                and t.method_name = 'nft_transfer_payout' \
                                group by \
                                    t.receipt_predecessor_account_id, \
                                    cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->>'receiver_id' \
                            ) sub \
                        ) sub2 \
                        where cast(sub2.mayor_compra as text) <> '[]' "*/

        /*switch (wallet) {
            case "%":
                {
                    query += "  order by \
                                    sub2.total_gastado desc \
                                limit $1 offset $2 ";
                    
                    const respuesta = await conexion2.query(query, [limit, index, fecha])
                    const rows_count = await conexion2.query(query2, [fecha])

                    res.json({rows_count: rows_count.rows[0].array_size, response: respuesta.rows})
                }
                break;
        
            default:
                {
                    query += " and upper(sub2.wallet) like $4 \
                                order by \
                                    sub2.total_gastado desc \
                                limit $1 offset $2 ";

                    query2 += " and upper(sub2.wallet) like $2 ";
                    
                    let account_id = "%"+wallet.toUpperCase()+"%"
                    const respuesta = await conexion2.query(query, [limit, index, fecha, account_id])
                    const rows_count = await conexion2.query(query2, [fecha, account_id])

                    res.json({rows_count: rows_count.rows[0].array_size, response: respuesta.rows})
                }
                break;
        }*/
        res.json([])
        
    
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: false, error: error})
    }
}


const StastMarket = async (req, res) => {
    try {
        const conexion = await dbConnect2()
                
                    
        queryGql_sales = gql`
            query MyQuery {
                statsmarketplaces {
                    id
                    volumen
                    floor_sales
                    floor_sales_data
                    biggest_sale
                    biggest_sale_data
                    market(first: 1, orderBy: sales, orderDirection: desc) {
                        collection
                        sales
                    }
                }
            }
        `;
        queryGql_volumnen = gql`
        query MyQuery {
                statsmarketplaces {
                    id
                    market(first: 1, orderBy: volumen, orderDirection: desc) {
                        collection
                        volumen
                    }
                }
            }
        `;
        
        const nftmarket_sales = await colsutaGraph(queryGql_sales)
        const nftmarket_volumen = await colsutaGraph(queryGql_volumnen)

        /*--------------------- data marketplace ------------------------------*/
        const resp_dmp = await conexion.query(" select marketplace as market, name, icon, web from marketplaces ")
        const dmp = resp_dmp.rows
        /*---------------------------------------------------------------------*/
        
        let array = []
        for(let i = 0; i < nftmarket_sales.statsmarketplaces.length; i++){
            const ms = nftmarket_sales.statsmarketplaces[i]
            console.log(ms.id)
            const mv = nftmarket_volumen.statsmarketplaces.find(item => item.id == ms.id)
            const mp = dmp.find(item => item.market == ms.id)
            array.push({
                market: ms.id,
                market_name: mp.name ? mp.name : '',
                market_icon: mp.icon ? mp.icon : '',
                market_web: mp.web ? mp.web : '',
                volumen_market: ms.volumen,
                floor_sales: ms.floor_sales,
                floor_sales_data: ms.floor_sales_data,
                biggest_sale: ms.biggest_sale,
                biggest_sale_data: ms.biggest_sale_data,
                best_selling_collection: ms.market,
                collection_more_volumen: mv.market ? mv.market : []
            })
        }
        
        //console.log(array)
        res.json(array)
    } catch (error) {
        console.log(error)
        res.json([])
    } 
}

module.exports = { SalesOfTheDay, HighestVOLGainers, Volumen24h, Volumen7d, BuscarCollection, Ranking, UpcomingListed,
                    NewProjectsListed, ActiveWalleHeader, ActiveWallets, ActiveWalletsMarket, StastMarket }