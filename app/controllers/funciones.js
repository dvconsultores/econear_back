const { CONFIG } = require('../helpers/utils')
const { dbConnect, dbConnect2, dbConnect3 } = require('../../config/postgres')
const nearAPI = require("near-api-js");
const axios = require('axios');
const moment = require('moment');
const { colsutaTheGraph } = require('../graphql/dataThegraph')
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



async function Datasellbuy() {
    try {
        const epoch = moment().subtract(2, 'y').valueOf()*1000000;
        const conn = await dbConnect2()

        while (10000000000000) {
            console.log('inicio ----------------------------------------------------------------------------------------------------')
            const result = await conn.query(" select fecha from nft_sellbuy order by fecha desc limit 1 ")

            let fecha = result.rows.length > 0 ? result.rows[0].fecha : epoch.toString()
            console.log(result.rows)
            console.log(fecha)
            queryGql = gql`
                query MyQuery($fecha: BigInt!) {
                    sellbuynfts(where: {fecha_gt: $fecha} orderBy: fecha, orderDirection: asc, first: 1000) {
                        collection
                        tokenid
                        market
                        seller
                        buyer
                        fecha
                        price
                        pricenear
                    }
                }
            `;

            variables = { fecha: fecha }
            
            const sellbuynft = await colsutaTheGraph(queryGql, variables)
            
            
            let rows = sellbuynft.sellbuynfts
            
            if(rows.length <= 0) {
                console.log('fin -------------------------------------------------------------------------------------------------------')
                break
            }

            let datos = ''
            const tamano_row = rows.length      
            for(var i = 0; i < rows.length; i++) {
            datos += "('"+rows[i].collection.toString()+"', '"+rows[i].tokenid.toString()+"', '"+rows[i].market.toString()+"', '"+rows[i].seller.toString()+"', \
                '"+rows[i].buyer.toString()+"', "+rows[i].fecha.toString()+", '"+rows[i].price.toString()+"', "+rows[i].pricenear.toString()+")";
                
                datos += i != (tamano_row - 1) ? ", " : ""; 
            }
            
            
            let tabla_temp_nft_sellbuy = "  CREATE TEMP TABLE tmp_nft_sellbuy ( \
                collection text NOT NULL, \
                tokenid text not NULL, \
                market text NOT NULL, \
                seller text NOT NULL, \
                buyer text NOT NULL, \
                fecha numeric(20) NOT NULL, \
                price text NOT NULL, \
                pricenear numeric NOT NULL \
            ) ";

            let insert_temp_nft_sellbuy = "INSERT INTO tmp_nft_sellbuy ( \
                collection, \
                tokenid, \
                market, \
                seller, \
                buyer, \
                fecha, \
                price, \
                pricenear \
            ) \
            VALUES " + datos //#+ str(data);

            await conn.query(tabla_temp_nft_sellbuy);
            await conn.query('COMMIT');
            console.log("Tabla temporal creada")
            await conn.query(insert_temp_nft_sellbuy);
            await conn.query('COMMIT');
            console.log("Registros insertados en tabla temporal")

            let insert_nft_sellbuy = "  insert into nft_sellbuy \
                                        select collection, tokenid, market, seller, buyer, fecha, price, pricenear \
                                        from tmp_nft_sellbuy as t1 \
                                        where 0 = (select case when (select 1 from nft_sellbuy t2 \
                                            where t2.fecha = t1.fecha group by 1) = 1 then 1 else 0 end) ";

            await conn.query(insert_nft_sellbuy);
            await conn.query('COMMIT');
            console.log("Registros insertados en tabla final nft_sellbuy")

            let delete_temp_nft_sellbuy = "  drop table tmp_nft_sellbuy "
            await conn.query(delete_temp_nft_sellbuy);
            await conn.query('COMMIT');
            console.log("Tabla temporal eliminada")
            
            //console.log(array)
            console.log('fin -------------------------------------------------------------------------------------------------------')
            
        }
        return 'exito'
    } catch (error) {
        console.log(error)
        return error
    } 
}


async function updateTransactions(epoch_h) {
    try {
        const marketplace_array = "'backend.monkeonnear.near','apollo42.near', 'marketplace.paras.near', 'higgsfield.near', 'zoneart.near'";
        console.log(epoch_h)
        let offset = 0;
        let detener = false;
        for(var i = 0; i < 1000; i++) {
            if(detener){break}
            for(var j = 0; j < 20; j++) {
                try {
                    const conn_origen = await dbConnect();
                    const conn = await dbConnect2();
                    
                    console.log('Tiempo inicio de ejecución  transactions ', ' - offset = ', offset)
                    let query = "   select \
                                x.receipt_id , x.index_in_action_receipt, cast(x.args as text) as args, x.receipt_predecessor_account_id, x.receipt_receiver_account_id, \
                                cast(x.receipt_included_in_block_timestamp as char(20)) as receipt_included_in_block_timestamp, \
                                eo.executed_in_block_hash, cast(eo.executed_in_block_timestamp as char(20)) as executed_in_block_timestamp, \
                                eo.index_in_chunk, eo.executor_account_id \
                                FROM public.action_receipt_actions x \
                                inner join execution_outcomes eo on x.receipt_id = eo.receipt_id \
                                where x.receipt_included_in_block_timestamp >= $1 \
                                and cast(x.action_kind as text) = 'FUNCTION_CALL' \
                                and eo.status = 'SUCCESS_VALUE' \
                                and (receipt_predecessor_account_id in ("+marketplace_array+") \
                                or receipt_receiver_account_id in ("+marketplace_array+") )  \
                                limit 2000 offset $2 ";

                    console.log('Consultando transactions')

                    const resultados = await conn_origen.query(query, [epoch_h, offset]);
                    const rows = resultados.rows;

                    if(rows.length == 0) {
                        detener = true
                        break;
                    }
                    
                    console.log('----------------------------------------------------------------------------------------------------')
                    let datos = ''
                    const tamano_row = rows.length      
                    for(var ii = 0; ii < rows.length; ii++) {
                    datos += "('"+rows[ii].receipt_id.toString()+"', "+rows[ii].index_in_action_receipt.toString()+", '"+rows[ii].args.toString()+"', \
                        '"+rows[ii].receipt_predecessor_account_id.toString()+"', '"+rows[ii].receipt_receiver_account_id.toString()+"', \
                        "+rows[ii].receipt_included_in_block_timestamp.toString()+", '"+rows[ii].executed_in_block_hash.toString()+"', \
                        "+rows[ii].executed_in_block_timestamp.toString()+", "+rows[ii].index_in_chunk.toString()+", '"+rows[ii].executor_account_id.toString()+"')";
                        
                        datos += ii != (tamano_row - 1) ? ", " : ""; 
                    }
                    console.log('----------------------------------------------------------------------------------------------------')
                    
                    if(rows.length == 0) {
                        //cursorO.close  # Cerrar cursor

                        //conn.close()  # Cerrando conección
                        //conn_origen.close()  # Cerrando conección
                        console.log('fin consulta transactions')
                        console.log('Tiempo de ejecución transactions ')
                        break;
                    }
                    
                    let query2 = "  CREATE TEMP TABLE tmp_transaction ( \
                                    receipt_id text NULL, \
                                    index_in_action_receipt int4 NULL, \
                                    args json NULL, \
                                    receipt_predecessor_account_id text NULL, \
                                    receipt_receiver_account_id text NULL, \
                                    receipt_included_in_block_timestamp numeric(20) NULL, \
                                    executed_in_block_hash text NULL, \
                                    executed_in_block_timestamp numeric(20) NULL, \
                                    index_in_chunk int8 NULL, \
                                    executor_account_id text NULL \
                                ) ";

                    let query3 = "INSERT INTO tmp_transaction ( \
                                    receipt_id, \
                                    index_in_action_receipt, \
                                    args, \
                                    receipt_predecessor_account_id, \
                                    receipt_receiver_account_id, \
                                    receipt_included_in_block_timestamp, \
                                    executed_in_block_hash, \
                                    executed_in_block_timestamp, \
                                    index_in_chunk, \
                                    executor_account_id \
                                ) \
                                VALUES " + datos //#+ str(data);
                    
                    console.log("creando tabla temporal")
                    await conn.query(query2);
                    await conn.query('COMMIT');
                    console.log("insertando tabla temporal")
                    await conn.query(query3);
                    await conn.query('COMMIT');
                    console.log("insertando tabla temporal")

                    console.log('insertando en tabla final')
                        
                    let query4 = "  insert into transaction \
                                select receipt_id, index_in_action_receipt , args, receipt_predecessor_account_id, receipt_receiver_account_id, \
                                receipt_included_in_block_timestamp, executed_in_block_hash, executed_in_block_timestamp , index_in_chunk, \
                                executor_account_id, args->>'method_name' as method_name \
                                from tmp_transaction as t \
                                where 0 = (select case when (select 1 from transaction t2 \
                                    where t2.receipt_id = t.receipt_id and t2.index_in_action_receipt = t.index_in_action_receipt \
                                    group by 1) = 1 then 1 else 0 end) ";

                    await conn.query(query4);
                    await conn.query('COMMIT');

                    console.log('fin consulta transactions')
                
                    offset += 2000;
                    console.log('Tiempo de ejecución transactions ', ' - offset = ', offset)
                    
                    break
                } catch (error) {     
                    //conn.close()  //# Cerrando conección
                    //conn_origen.close()  //# Cerrando conección
                    console.log('Error cargaTransaction: ', error)
                }
            }
        }
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: error})
    }
}


async function updateProjects(epoch_h) {    
    console.log(epoch_h)  
    try {
        const conn = await dbConnect2();
        
        console.log('Tiempo inicio de ejecución  transactions ')
        let query = "   insert into projects \
                        select \
                            receipt_predecessor_account_id as user_creation, \
                            cast(receipt_included_in_block_timestamp as numeric(20)) as fecha, \
                            cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->'items'->>'project_name' as project_name, \
                            cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->'items'->>'descripcion' as descripcion, \
                            cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->'items'->>'email' as email, \
                            cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->'items'->>'discord_id' as discord_id, \
                            cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->'items'->>'website' as website, \
                            cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->'items'->>'twiter' as twiter, \
                            cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->'items'->>'telegram' as telegram, \
                            cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->'items'->>'discord_server' as discord_server, \
                            cast(cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->'items'->>'upcoming' as bool) as upcoming, \
	                        cast(cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->'items'->>'already' as bool) as already, \
                            cast(cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->'items'->>'fecha_lanzamiento' as numeric(10)) as fecha_lanzamiento, \
                            cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->'items'->>'id_contract_project' as id_contract_project, \
                            false as listado \
                        from transaction t \
                        where receipt_included_in_block_timestamp >= $1 \
                        and receipt_receiver_account_id = $2 \
                        and method_name = 'addproject' \
                        and 0 = (select case when (select 1 from projects \
                        where id_contract_project = cast(convert_from(decode(t.args->>'args_base64', 'base64'), 'UTF8') as json)->'items'->>'id_contract_project') = 1 then 1 else 0 end) ";

        console.log('Consultando nuevos projectos')

        await conn.query(query, [epoch_h, process.env.CONTRACT_NAME]);
        await conn.query("commit");
        
        await listarCollections();
        console.log('fin update projects')
        console.log('Tiempo de ejecución transactions ')

        return {respuesta: true}
        
    } catch (error) {
        console.log('Error update projects: ', error)
        return {respuesta: false, error: error}
    }
}

async function listarCollections() {
    try {
        //const { top } = req.body
        // const tiempoTranscurrido = moment(moment().subtract(24, 'h').format('DD/MM/YYYY'), 'DD/MM/YYYY').valueOf();
        // const fecha2 = (new Date(new Date(tiempoTranscurrido).toLocaleDateString()).getTime()/1000.0)*1000000000;

        //const fecha = moment(moment().format('DD/MM/YYYY'), 'DD/MM/YYYY').valueOf()*1000000;
        // const fecha = moment().subtract(24, 'h').valueOf()*1000000;
        
        // console.log(fecha)
        // console.log(fecha2)

        const conexion2 = await dbConnect2()
        const conexion = await dbConnect()
        
        const resultados = await conexion2.query("  select id_contract_project as nft_contract from projects p \
                                                    where already = true and listado = false \
                                                    and 0 = (select case when (select 1 from collections where nft_contract = p.id_contract_project) = 1 then 1 else 0 end)")

        try {
            const datas = resultados.rows;
            for(var i = 0; i < datas.length; i++) {
                let fecha_creacion = null;
                let total_supply = "0";

                if(datas[i].fecha_creacion == null){
                    try {
                        const response_account = await conexion.query("select r.included_in_block_timestamp as fecha_creacion from accounts a \
                                                      inner join receipts r on a.created_by_receipt_id = r.receipt_id \
                                                      where account_id = $1 \
                                                      ", [datas[i].nft_contract]);
                        fecha_creacion = response_account.rows[0].fecha_creacion;
                    } catch (error) {
                        console.log(error)
                    }
                } 
                                                      
                console.log(fecha_creacion)
                console.log(datas[i].nft_contract)
                
                const contract = new Contract(account, datas[i].nft_contract, {
                    viewMethods: ['nft_total_supply', 'nft_metadata'],
                    sender: account
                })
                try {
                    const response_supply = await contract.nft_total_supply();
                    total_supply = response_supply.toString();
                } catch (error) {
                    console.log(error)
                }
                try {
                    const response_metadata = await contract.nft_metadata()
                    
                    console.log("total supply:" + total_supply)

                    await conexion2.query("insert into collections values($1, $2, $3, $4, $5, $6, $7, $8, $9, true) \
                                                    ", [datas[i].nft_contract,
                                                        datas[i].nft_contract,
                                                        response_metadata.name,
                                                        response_metadata.symbol,
                                                        response_metadata.icon,
                                                        response_metadata.base_uri,
                                                        response_metadata.reference,
                                                        fecha_creacion,
                                                        total_supply]);
                    await conexion2.query("commit");

                    await conexion2.query(" update projects set listado = true where id_contract_project = $1 ", [datas[i].nft_contract]);
                    await conexion2.query("commit");

                    await listarCollectionsMarketplace(datas[i].nft_contract)

                } catch (error) {
                    console.log(error)
                    console.log('error collection no existe')
                    await conexion2.query(" update projects set listado = true where id_contract_project = $1 ", [datas[i].nft_contract]);
                    await conexion2.query("commit");
                }
                
            }
            
        } catch (error) {
            console.log('error 2: ', error)
        }
        
    } catch (error) {
        console.log('error 1: ', error)
    }
}

async function listarCollectionsMarketplace(collection) {
    try {
        const conexion2 = await dbConnect2()    

        await conexion2.query("     insert into collections_marketplace \
                                    select \
                                        sub.collection, \
                                        sub.marketplace \
                                    from ( \
                                    select \
                                        receipt_predecessor_account_id as collection, \
                                        receipt_receiver_account_id as marketplace \
                                    from transaction \
                                    where receipt_predecessor_account_id = $1 \
                                    and method_name = 'nft_on_approve' \
                                    and cast(cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->>'msg' as json)->>'market_type' = 'sale' \
                                    group by \
                                        receipt_receiver_account_id, receipt_predecessor_account_id \
                                    ) sub \
                                    where sub.collection not in (select collection from collections_marketplace mc \
                                        where mc.collection = sub.collection and mc.marketplace = sub.marketplace) \
        ", [collection]);
        await conexion2.query("commit");
        
    } catch (error) {
        console.log('error 1: ', error)
    }
}


async function UpdateVotes(fecha) {
    console.log('actualizando votos')
    try {
        const conexion2 = await dbConnect2()
        //const conexion = await dbConnect()

        /*query = "   select \
                        xx.args->'args_json'->>'collections' as collection, xx.receipt_predecessor_account_id as user_id \
                        ,xx.receipt_included_in_block_timestamp as fecha, args->'args_json'->>'voto' as voto \
                    FROM public.action_receipt_actions xx \
                    inner join execution_outcomes eo on xx.receipt_id = eo.receipt_id \
                    where \
                        xx.receipt_included_in_block_timestamp >= $1\
                        and xx.receipt_receiver_account_id = $2 \
                        and cast(xx.action_kind as text) = 'FUNCTION_CALL' \
                        and eo.status = 'SUCCESS_VALUE' \
                        and args->>'method_name' = 'votar' \
                        and args->>'args_json' is not null ";*/
        
        let query = "   select \
                            cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->>'collections' as collection, \
                            receipt_predecessor_account_id as user_id, receipt_included_in_block_timestamp as fecha, \
                            cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->>'voto' as voto \
                        from transaction t \
                        where receipt_included_in_block_timestamp >= $1 \
                        and receipt_receiver_account_id = $2 \
                        and method_name = 'votar' "
        ;
        const respuesta = await conexion2.query(query, [fecha, process.env.CONTRACT_NAME])
        const data1 = respuesta.rows;

        for(var i = 0; i < data1.length; i++) {
            //console.log(data1[i].collection, data1[i].user_id, data1[i].fecha)
            query2 = " select collection, user_id, fecha, voto from votos v where collection = $1 and user_id = $2 ";
            const respuesta2 = await conexion2.query(query2, [data1[i].collection, data1[i].user_id])
            //console.log(respuesta2.rows.length)
            //console.log("fecha 1: ", data1[i].fecha, " - fecha 2: ")
            if (respuesta2.rows.length == 0) {
                query3 = "insert into votos values($1, $2, $3, $4)"
                await conexion2.query(query3, [data1[i].collection, data1[i].user_id, data1[i].fecha, data1[i].voto])
            } else {
                //console.log("fecha 1: ", data1[i].fecha, " - fecha 2: ", respuesta2.rows[0].fecha)
                if(data1[i].fecha > respuesta2.rows[0].fecha) {
                    if(data1[i].voto == respuesta2.rows[0].voto) {
                        query3 = "delete from votos where collection = $1 and user_id = $2 "
                        await conexion2.query(query3, [data1[i].collection, data1[i].user_id])
                    } else {
                        update = "  update votos \
                                    set \
                                        fecha = $1, \
                                        voto = $2 \
                                    where \
                                        collection = $3 \
                                        and user_id = $4" 
                        await conexion2.query(update, [data1[i].fecha, data1[i].voto, data1[i].collection, data1[i].user_id])
                    }
                }
            }
        }
        console.log('votos actualizacos')   
        return {respuesta: true}
    } catch (error) {
        console.log('error: ', error)
        console.log('Error al actualizacos votos')
        return {respuesta: false, error: error}
    }
}


async function UpdateVotesUpcoming(fecha) {
    console.log('actualizando votos')
    try {
        const conexion2 = await dbConnect2()
        
        let query = "   select \
                            cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->>'collections' as collection, \
                            receipt_predecessor_account_id as user_id, receipt_included_in_block_timestamp as fecha, \
                            case when cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->>'voto' is null then 'true' \
                            else cast(convert_from(decode(args->>'args_base64', 'base64'), 'UTF8') as json)->>'voto' end as voto \
                        from transaction t \
                        where receipt_included_in_block_timestamp >= $1 \
                        and receipt_receiver_account_id = $2 \
                        and method_name = 'votar_upcoming' "
        ;
        const respuesta = await conexion2.query(query, [fecha, process.env.CONTRACT_NAME])
        const data1 = respuesta.rows;

        for(var i = 0; i < data1.length; i++) {
            //console.log(data1[i].collection, data1[i].user_id, data1[i].fecha)
            query2 = " select collection, user_id, fecha, voto from votos_upcoming v where collection = $1 and user_id = $2 ";
            const respuesta2 = await conexion2.query(query2, [data1[i].collection, data1[i].user_id])
            //console.log(respuesta2.rows.length)
            //console.log("fecha 1: ", data1[i].fecha, " - fecha 2: ")
            if (respuesta2.rows.length == 0) {
                query3 = "insert into votos_upcoming values($1, $2, $3, $4)"
                await conexion2.query(query3, [data1[i].collection, data1[i].user_id, data1[i].fecha, data1[i].voto])
            } else {
                //console.log("fecha 1: ", data1[i].fecha, " - fecha 2: ", respuesta2.rows[0].fecha)
                if(data1[i].fecha > respuesta2.rows[0].fecha) {
                    if(data1[i].voto == respuesta2.rows[0].voto) {
                        query3 = "delete from votos_upcoming where collection = $1 and user_id = $2 "
                        await conexion2.query(query3, [data1[i].collection, data1[i].user_id])
                    } else {
                        update = "  update votos_upcoming \
                                    set \
                                        fecha = $1, \
                                        voto = $2 \
                                    where \
                                        collection = $3 \
                                        and user_id = $4" 
                        await conexion2.query(update, [data1[i].fecha, data1[i].voto, data1[i].collection, data1[i].user_id])
                    }
                }
            }
        }
        console.log('votos actualizacos')   
        return {respuesta: true}
    } catch (error) {
        console.log('error: ', error)
        console.log('Error al actualizacos votos')
        return {respuesta: false, error: error}
    }
}


async function update_masivo_collections() {
    try {
        //const { top } = req.body
        const tiempoTranscurrido = moment(moment().subtract(24, 'h').format('DD/MM/YYYY'), 'DD/MM/YYYY').valueOf();
        const fecha2 = (new Date(new Date(tiempoTranscurrido).toLocaleDateString()).getTime()/1000.0)*1000000000;

        //const fecha = moment(moment().format('DD/MM/YYYY'), 'DD/MM/YYYY').valueOf()*1000000;
        const fecha = moment().subtract(24, 'h').valueOf()*1000000;
        
        console.log(fecha)
        console.log(fecha2)
        const conexion2 = await dbConnect2()
        const conexion = await dbConnect()

        await conexion2.query("insert into collections (nft_contract, owner_id, listar_collections) \
                                                    select t.receipt_receiver_account_id, t.receipt_receiver_account_id, false \
                                                    from transaction t  \
                                                    where t.method_name = 'nft_transfer_payout' \
                                                    and t.receipt_receiver_account_id <> 'x.paras.near' \
                                                    and t.receipt_receiver_account_id not in ((select c.nft_contract from collections c)) \
                                                    group by \
                                                        t.receipt_receiver_account_id")
        
        const resultados = await conexion2.query("select nft_contract, fecha_creacion from collections")

   
        const arreglo = [];
        let fecha_creacion = null;
        let total_supply = "0";
        try {
            const datas = resultados.rows;
            for(var i = 0; i < datas.length; i++) {

                if(datas[i].fecha_creacion == null){

                    const response_account = await conexion.query("select r.included_in_block_timestamp as fecha_creacion from accounts a \
                                                      inner join receipts r on a.created_by_receipt_id = r.receipt_id \
                                                      where account_id = $1 ", [datas[i].nft_contract]);
                    fecha_creacion = response_account.rows[0].fecha_creacion;
                } else {
                    fecha_creacion = datas[i].fecha_creacion;
                }
                                                      
                console.log(fecha_creacion)
                console.log(datas[i].nft_contract)
                
                const contract = new Contract(account, datas[i].nft_contract, {
                    viewMethods: ['nft_total_supply', 'nft_metadata'],
                    sender: account
                })
                try {
                    const response_supply = await contract.nft_total_supply();
                    total_supply = response_supply;
                } catch (error) {
                    total_supply = "0"
                    console.log(error)
                }

                const response_metadata = await contract.nft_metadata()
                
                console.log("total supply:" + total_supply)

                await conexion2.query("update collections \
                                                 set \
                                                 name = $1, \
                                                 symbol = $2, \
                                                 icon = $3, \
                                                 base_uri = $4, \
                                                 reference = $5, \
                                                 fecha_creacion = $6, \
                                                 total_supply = $7 \
                                                 where \
                                                    nft_contract = $8 \
                                                ", [response_metadata.name,
                                                    response_metadata.symbol,
                                                    response_metadata.icon,
                                                    response_metadata.base_uri,
                                                    response_metadata.reference,
                                                    fecha_creacion,
                                                    total_supply,
                                                    datas[i].nft_contract])
                
            }
            console.log(arreglo);
            
        } catch (error) {
            console.log('error 2: ', error)
            return error
        }
        
        return arreglo
    } catch (error) {
        console.log('error 1: ', error)
        return error
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

/*
async function update_collections() {
    try {
        //const { top } = req.body
        const tiempoTranscurrido = moment(moment().subtract(24, 'h').format('DD/MM/YYYY'), 'DD/MM/YYYY').valueOf();
        const fecha2 = (new Date(new Date(tiempoTranscurrido).toLocaleDateString()).getTime()/1000.0)*1000000000;

        //const fecha = moment(moment().format('DD/MM/YYYY'), 'DD/MM/YYYY').valueOf()*1000000;
        const fecha = moment().subtract(24, 'h').valueOf()*1000000;
        
        console.log(fecha)
        console.log(fecha2)
        const conexion2 = await dbConnect2()
        const conexion = await dbConnect()
        
        const resultados = await conexion2.query("select nft_contract, fecha_creacion from collections where fecha_creacion is null")

        const arreglo = [];
        let fecha_creacion = null;
        let total_supply = "0";
        try {
            const datas = resultados.rows;
            for(var i = 0; i < datas.length; i++) {

                if(datas[i].fecha_creacion == null){

                    const response_account = await conexion.query("select r.included_in_block_timestamp as fecha_creacion from accounts a \
                                                      inner join receipts r on a.created_by_receipt_id = r.receipt_id \
                                                      where account_id = $1 ", [datas[i].nft_contract]);
                    fecha_creacion = response_account.rows[0].fecha_creacion;
                } else {
                    fecha_creacion = datas[i].fecha_creacion;
                }
                                                      
                console.log(fecha_creacion)
                console.log(datas[i].nft_contract)
                
                const contract = new Contract(account, datas[i].nft_contract, {
                    viewMethods: ['nft_total_supply', 'nft_metadata'],
                    sender: account
                })
                try {
                    const response_supply = await contract.nft_total_supply();
                    total_supply = response_supply;
                } catch (error) {
                    total_supply = "0"
                    console.log(error)
                }
                const response_metadata = await contract.nft_metadata()
                
                console.log("total supply:" + total_supply)

                await conexion2.query("update collections \
                                                 set \
                                                 name = $1, \
                                                 symbol = $2, \
                                                 icon = $3, \
                                                 base_uri = $4, \
                                                 reference = $5, \
                                                 fecha_creacion = $6, \
                                                 total_supply = $7 \
                                                 where \
                                                    nft_contract = $8 \
                                                ", [response_metadata.name,
                                                    response_metadata.symbol,
                                                    response_metadata.icon,
                                                    response_metadata.base_uri,
                                                    response_metadata.reference,
                                                    fecha_creacion,
                                                    total_supply,
                                                    datas[i].nft_contract])
                
            }
            console.log(arreglo);
            
        } catch (error) {
            console.log('error 2: ', error)
            return error
        }
        
        return arreglo
    } catch (error) {
        console.log('error 1: ', error)
        return error
    }
}

async function update_masivo_collections() {
    try {
        //const { top } = req.body
        const tiempoTranscurrido = moment(moment().subtract(24, 'h').format('DD/MM/YYYY'), 'DD/MM/YYYY').valueOf();
        const fecha2 = (new Date(new Date(tiempoTranscurrido).toLocaleDateString()).getTime()/1000.0)*1000000000;

        //const fecha = moment(moment().format('DD/MM/YYYY'), 'DD/MM/YYYY').valueOf()*1000000;
        const fecha = moment().subtract(24, 'h').valueOf()*1000000;
        
        console.log(fecha)
        console.log(fecha2)
        const conexion2 = await dbConnect2()
        const conexion = await dbConnect()
        
        const resultados = await conexion2.query("select nft_contract, fecha_creacion from collections")

   
        const arreglo = [];
        let fecha_creacion = null;
        let total_supply = "0";
        try {
            const datas = resultados.rows;
            for(var i = 0; i < datas.length; i++) {

                if(datas[i].fecha_creacion == null){

                    const response_account = await conexion.query("select r.included_in_block_timestamp as fecha_creacion from accounts a \
                                                      inner join receipts r on a.created_by_receipt_id = r.receipt_id \
                                                      where account_id = $1 ", [datas[i].nft_contract]);
                    fecha_creacion = response_account.rows[0].fecha_creacion;
                } else {
                    fecha_creacion = datas[i].fecha_creacion;
                }
                                                      
                console.log(fecha_creacion)
                console.log(datas[i].nft_contract)
                
                const contract = new Contract(account, datas[i].nft_contract, {
                    viewMethods: ['nft_total_supply', 'nft_metadata'],
                    sender: account
                })
                try {
                    const response_supply = await contract.nft_total_supply();
                    total_supply = response_supply;
                } catch (error) {
                    total_supply = "0"
                    console.log(error)
                }

                const response_metadata = await contract.nft_metadata()
                
                console.log("total supply:" + total_supply)

                await conexion2.query("update collections \
                                                 set \
                                                 name = $1, \
                                                 symbol = $2, \
                                                 icon = $3, \
                                                 base_uri = $4, \
                                                 reference = $5, \
                                                 fecha_creacion = $6, \
                                                 total_supply = $7 \
                                                 where \
                                                    nft_contract = $8 \
                                                ", [response_metadata.name,
                                                    response_metadata.symbol,
                                                    response_metadata.icon,
                                                    response_metadata.base_uri,
                                                    response_metadata.reference,
                                                    fecha_creacion,
                                                    total_supply,
                                                    datas[i].nft_contract])
                
            }
            console.log(arreglo);
            
        } catch (error) {
            console.log('error 2: ', error)
            return error
        }
        
        return arreglo
    } catch (error) {
        console.log('error 1: ', error)
        return error
    }
}

//update_collections()
//update_masivo_collections()
*/

module.exports = { Datasellbuy, updateTransactions, updateProjects, UpdateVotes, UpdateVotesUpcoming, update_masivo_collections }





















