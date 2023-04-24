const { CONFIG } = require('../helpers/utils')
const { dbConnect, dbConnect2, dbConnect3 } = require('../../config/postgres')
const nearAPI = require("near-api-js");
const axios = require('axios');
const moment = require('moment');
const { colsutaTheGraph, colsutaTheGraph2 } = require('../graphql/dataThegraph')
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



async function Databuy() {
    try {
        const epoch = moment().subtract(2, 'y').valueOf()*1000000;
        const conn = await dbConnect2()

        while (10000000000000) {
            console.log('inicio ----------------------------------------------------------------------------------------------------')
            const result = await conn.query(" select fecha from nft_buy order by fecha desc limit 1 ")

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
            
            const buynft = await colsutaTheGraph(queryGql, variables)
            
            
            let rows = buynft.sellbuynfts
            
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
            
            
            let tabla_temp_nft_buy = "  CREATE TEMP TABLE tmp_nft_buy ( \
                collection text NOT NULL, \
                tokenid text not NULL, \
                market text NOT NULL, \
                seller text NOT NULL, \
                buyer text NOT NULL, \
                fecha numeric(20) NOT NULL, \
                price text NOT NULL, \
                pricenear numeric NOT NULL \
            ) ";

            let insert_temp_nft_buy = "INSERT INTO tmp_nft_buy ( \
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

            await conn.query(tabla_temp_nft_buy);
            await conn.query('COMMIT');
            console.log("Tabla temporal creada")
            await conn.query(insert_temp_nft_buy);
            await conn.query('COMMIT');
            console.log("Registros insertados en tabla temporal")

            let insert_nft_buy = "  insert into nft_buy \
                                        select collection, tokenid, market, seller, buyer, fecha, price, pricenear \
                                        from tmp_nft_buy as t1 \
                                        where 0 = (select case when (select 1 from nft_buy t2 \
                                            where t2.fecha = t1.fecha group by 1) = 1 then 1 else 0 end) ";

            await conn.query(insert_nft_buy);
            await conn.query('COMMIT');
            console.log("Registros insertados en tabla final nft_buy")

            let delete_temp_nft_buy = "  drop table tmp_nft_buy "
            await conn.query(delete_temp_nft_buy);
            await conn.query('COMMIT');
            console.log("Tabla temporal eliminada")
            conn.end()
            //console.log(array)
            console.log('fin -------------------------------------------------------------------------------------------------------')
            
        }
        return 'exito'
    } catch (error) {
        console.log(error)
        return error
    } 
}


async function DataSell() {
    try {
        const epoch = moment().subtract(2, 'y').valueOf()*1000000;
        const conn = await dbConnect2()

        while (10000000000000) {
            console.log('inicio ----------------------------------------------------------------------------------------------------')
            const result = await conn.query(" select fecha from nft_sell order by fecha desc limit 1 ")

            let fecha = result.rows.length > 0 ? result.rows[0].fecha : epoch.toString()
            console.log(result.rows)
            console.log(fecha)
            queryGql = gql`
                query MyQuery($fecha: BigInt!) {
                    nftonsales(where: {fecha_gt: $fecha} orderBy: fecha, orderDirection: asc, first: 1000) {
                        collection
                        tokenid
                        market
                        owner_id
                        fecha
                        price
                        pricenear
                    }
                }
            `;

            variables = { fecha: fecha }
            
            const sellnft = await colsutaTheGraph(queryGql, variables)
            
            let rows = sellnft.nftonsales
            
            if(rows.length <= 0) {
                console.log('fin -------------------------------------------------------------------------------------------------------')
                break
            }

            let datos = ''
            const tamano_row = rows.length      
            for(var i = 0; i < rows.length; i++) {
            datos += "('"+rows[i].collection.toString()+"', '"+rows[i].tokenid.toString()+"', '"+rows[i].market.toString()+"', '"+rows[i].owner_id.toString()+"', \
                "+rows[i].fecha.toString()+", '"+rows[i].price.toString()+"', "+rows[i].pricenear.toString()+")";
                
                datos += i != (tamano_row - 1) ? ", " : ""; 
            }
            
            
            let tabla_temp_nft_sell = "  CREATE TEMP TABLE tmp_nft_sell ( \
                collection text NOT NULL, \
                tokenid text NOT NULL, \
                market text NOT NULL, \
                seller text NOT NULL, \
                fecha numeric(20) NOT NULL, \
                price text NOT NULL, \
                pricenear numeric NOT NULL \
            ) ";

            let insert_temp_nft_sell = "INSERT INTO tmp_nft_sell ( \
                collection, \
                tokenid, \
                market, \
                seller, \
                fecha, \
                price, \
                pricenear \
            ) \
            VALUES " + datos //#+ str(data);

            await conn.query(tabla_temp_nft_sell);
            await conn.query('COMMIT');
            console.log("Tabla temporal creada")
            await conn.query(insert_temp_nft_sell);
            await conn.query('COMMIT');
            console.log("Registros insertados en tabla temporal")

            let insert_nft_sell = "  insert into nft_sell \
                                        select collection, tokenid, market, seller, fecha, price, pricenear \
                                        from tmp_nft_sell as t1 \
                                        where 0 = (select case when (select 1 from nft_sell t2 \
                                            where t2.fecha = t1.fecha group by 1) = 1 then 1 else 0 end) ";

            await conn.query(insert_nft_sell);
            await conn.query('COMMIT');
            console.log("Registros insertados en tabla final nft_sell")

            let delete_temp_nft_sell = "  drop table tmp_nft_sell "
            await conn.query(delete_temp_nft_sell);
            await conn.query('COMMIT');
            console.log("Tabla temporal eliminada")
            conn.end()
            //console.log(array)
            console.log('fin -------------------------------------------------------------------------------------------------------')
            
        }
        return 'exito'
    } catch (error) {
        console.log(error)
        return error
    } 
}

async function DataMarket(date_epoch) {
    try {
        const epoch = moment().subtract(2, 'y').valueOf()*1000000;
        const conn = await dbConnect2()

        let v_date_epoch = date_epoch.toString()
        let v2_date_epoch = date_epoch.toString()

        // ciclo insertado
        let end_fecha = 0
        let count_repit_fecha = 0
        while (1000000000000000000) {
            console.log('inicio ----------------------------------------------------------------------------------------------------')
            const result = await conn.query(" select fecha_insert as fecha from nft_marketplace order by fecha_insert desc limit 1 ")

            let fecha = date_epoch ? v_date_epoch : result.rows.length > 0 ? result.rows[0].fecha : epoch.toString()
            console.log(result.rows)
            console.log(fecha)
            
            console.log("end_fecha: ", end_fecha, " - fecha: ", fecha, " - count: ", count_repit_fecha)
            count_repit_fecha += end_fecha = fecha ? 1 : 0
            
            if(count_repit_fecha == 2) break

            queryGql = gql`
                query MyQuery($fecha: BigInt!) {
                    nftmarkets(where: {fecha_gt: $fecha} orderBy: fecha, orderDirection: asc, first: 1000) {
                        market
                        collection
                        tokenid
                        owner_id
                        fecha
                        price
                        pricenear
                    }
                }
            `;

            variables = { fecha: fecha }
            
            const nftmarkets = await colsutaTheGraph(queryGql, variables)
    
            let rows = nftmarkets.nftmarkets
            //console.log(rows)
            if(rows.length <= 0) {
                console.log('fin -------------------------------------------------------------------------------------------------------')
                break
            }

            let datos = ''
            const tamano_row = rows.length      
            for(var i = 0; i < rows.length; i++) {
            datos += "('"+rows[i].market.toString()+"', '"+rows[i].collection.toString()+"', '"+rows[i].tokenid.toString()+"', '"+rows[i].owner_id.toString()+"', \
                "+rows[i].fecha.toString()+", '"+rows[i].price.toString()+"', "+rows[i].pricenear.toString()+", "+rows[i].fecha.toString()+")";
                
                datos += i != (tamano_row - 1) ? ", " : ""; 
            }

            let tabla_temp_nft_market = "  CREATE TEMP TABLE tmp_nft_market ( \
                market text NOT NULL, \
                collection text NOT NULL, \
                tokenid text NOT NULL, \
                owner_id text NOT NULL, \
                fecha numeric(20) NOT NULL, \
                price text NOT NULL, \
                pricenear numeric NOT NULL, \
                fecha_insert numeric(20) NOT NULL \
            ) ";

            let insert_temp_nft_market = "INSERT INTO tmp_nft_market ( \
                market, \
                collection, \
                tokenid, \
                owner_id, \
                fecha, \
                price, \
                pricenear, \
                fecha_insert \
            ) \
            VALUES " + datos //#+ str(data);


            await conn.query(tabla_temp_nft_market);
            await conn.query('COMMIT');
            console.log("Tabla temporal creada")
            await conn.query(insert_temp_nft_market);
            await conn.query('COMMIT');
            console.log("Registros insertados en tabla temporal")

            if(date_epoch) {
                const result_fecha = await conn.query(" select fecha from tmp_nft_market order by fecha desc limit 1 ");
                v_date_epoch = result_fecha.rows.length > 0 ? result_fecha.rows[0].fecha : v_date_epoch
            }

            let insert_nft_market = "  insert into nft_marketplace \
                                    select \
                                        cast(t.price as text) as precio, t.fecha, t.market as marketplace, t.collection, t.tokenid as token_id, \
                                        t.pricenear as price_near, t.owner_id, t.fecha_insert \
                                    from tmp_nft_market t \
                                    where 0 = ( \
                                                select case when ( \
                                                                    select 1 from nft_marketplace m \
                                                                    where m.collection = t.collection \
                                                                    and m.marketplace = t.market \
                                                                    and m.token_id = t.tokenid \
                                                                  ) = 1 \
                                                        then 1 else 0 end \
                                               ) \
            ";

            await conn.query(insert_nft_market);
            await conn.query('COMMIT');            
            console.log("Registros insertados en tabla final nft_market")


            let delete_temp_nft_market = "  drop table tmp_nft_market "
            await conn.query(delete_temp_nft_market);
            await conn.query('COMMIT');
            console.log("Tabla temporal eliminada")
            
            //console.log(array)
            end_fecha = fecha
            console.log('fin -------------------------------------------------------------------------------------------------------')
            
        }

        /*---------------------------------------------------------------------------------------------------------------------------*/
        // ciclo actualizado
        end_fecha = 0
        count_repit_fecha = 0
        while (10000000000000) {
            console.log('inicio update ----------------------------------------------------------------------------------------------')
            const result = await conn.query(" select fecha from nft_marketplace order by fecha desc limit 1 ")

            let fecha = date_epoch ? v2_date_epoch : result.rows.length > 0 ? result.rows[0].fecha : epoch.toString()
            console.log(result.rows)
            console.log(fecha)
            
            console.log("end_fecha: ", end_fecha, " - fecha: ", fecha, " - count: ", count_repit_fecha)
            count_repit_fecha += end_fecha = fecha ? 1 : 0
            
            if(count_repit_fecha == 2) break

            queryGql = gql`
                query MyQuery($fecha: BigInt!) {
                    nftmarkets(where: {fecha_gt: $fecha} orderBy: fecha, orderDirection: asc, first: 1000) {
                        market
                        collection
                        tokenid
                        owner_id
                        fecha
                        price
                        pricenear
                    }
                }
            `;

            variables = { fecha: fecha }
            
            const nftmarkets = await colsutaTheGraph(queryGql, variables)
    
            let rows = nftmarkets.nftmarkets
            //console.log(rows)
            if(rows.length <= 0) {
                console.log('fin -------------------------------------------------------------------------------------------------------')
                break
            }
//1666420051395903531
//1666815747572434080
            let datos = ''
            const tamano_row = rows.length      
            for(var i = 0; i < rows.length; i++) {
            datos += "('"+rows[i].market.toString()+"', '"+rows[i].collection.toString()+"', '"+rows[i].tokenid.toString()+"', '"+rows[i].owner_id.toString()+"', \
                "+rows[i].fecha.toString()+", '"+rows[i].price.toString()+"', "+rows[i].pricenear.toString()+", "+rows[i].fecha.toString()+")";
                
                datos += i != (tamano_row - 1) ? ", " : ""; 
            }

            let tabla_temp_nft_market = "  CREATE TEMP TABLE tmp_nft_market ( \
                market text NOT NULL, \
                collection text NOT NULL, \
                tokenid text NOT NULL, \
                owner_id text NOT NULL, \
                fecha numeric(20) NOT NULL, \
                price text NOT NULL, \
                pricenear numeric NOT NULL, \
                fecha_insert numeric(20) NOT NULL \
            ) ";

            let insert_temp_nft_market = "INSERT INTO tmp_nft_market ( \
                market, \
                collection, \
                tokenid, \
                owner_id, \
                fecha, \
                price, \
                pricenear, \
                fecha_insert \
            ) \
            VALUES " + datos //#+ str(data);

            await conn.query(tabla_temp_nft_market);
            await conn.query('COMMIT');
            console.log("Tabla temporal creada")
            await conn.query(insert_temp_nft_market);
            await conn.query('COMMIT');
            console.log("Registros insertados en tabla temporal")

            if(date_epoch) {
                const result_fecha = await conn.query(" select fecha from tmp_nft_market order by fecha desc limit 1 ");
                v2_date_epoch = result_fecha.rows.length > 0 ? result_fecha.rows[0].fecha : v2_date_epoch
            }

            let update_nft_market = " update nft_marketplace \
                                        set \
                                            fecha = t.fecha, \
                                            precio = cast(t.price as text), \
                                            precio_near = t.pricenear, \
                                            owner_id = t.owner_id \
                                    from tmp_nft_market t \
                                    where t.tokenid = nft_marketplace.token_id \
                                        and t.collection = nft_marketplace.collection \
                                        and t.market = nft_marketplace.marketplace \
                                    and t.fecha > nft_marketplace.fecha \
            ";

            await conn.query(update_nft_market);
            await conn.query('COMMIT');
            console.log("Registros actualizados en tabla final nft_market")

            let delete_temp_nft_market = "  drop table tmp_nft_market "
            await conn.query(delete_temp_nft_market);
            await conn.query('COMMIT');
            console.log("Tabla temporal eliminada")
            
            //console.log(array)
            end_fecha = fecha
            console.log('fin -------------------------------------------------------------------------------------------------------')
            
        }
        console.log('inicio update final ---------------------------------------------------------------------------------------')
        let update_nft_market_transfer = "   update nft_marketplace \
                                                set \
                                                    fecha = tr.fecha, \
                                                    precio = '0', \
                                                    precio_near = 0, \
                                                    owner_id = tr.token_new_owner_account_id \
                                            from nft_collections_transaction tr \
                                            where tr.event_kind = 'TRANSFER' \
                                            and tr.token_id = nft_marketplace.token_id \
                                            and tr.collection = nft_marketplace.collection \
                                            and tr.fecha > nft_marketplace.fecha \
        ";

        await conn.query(update_nft_market_transfer);
        await conn.query('COMMIT');
        console.log("Registros actualizados en tabla final nft_market")

        let delete_nft_market = " delete from nft_marketplace  nct \
                                    where exists (select 1 from nft_collections_transaction \
                                        where event_kind = 'BURN' and token_id = nct.token_id and collection = nct.collection and fecha > nct.fecha group by 1) \
        ";

        await conn.query(delete_nft_market);
        await conn.query('COMMIT');
        console.log("Registros eliminados en tabla final nft_market")
        console.log('fin update final ------------------------------------------------------------------------------------------')

        return 'exito'
    } catch (error) {
        console.log(error)
        return error
    } 
}


async function HistFloor(epoch_h) {
    try {
        const conn = await dbConnect2();
        
        
        let create_tmp_a = "  CREATE TEMP TABLE tmp_a ( \
                        tokenid text NOT NULL, \
                        fecha numeric(20) NOT NULL \
        ) ";
        
        let create_tmp_b = "  CREATE TEMP TABLE tmp_b ( \
            tokenid text NOT NULL, \
            fecha numeric(20) NOT NULL \
        ) ";

        let create_tmp_c = "  CREATE TEMP TABLE tmp_c ( \
            tokenid text NOT NULL, \
            fecha numeric(20) NOT NULL \
        ) ";

        let create_tmp_d = "  CREATE TEMP TABLE tmp_d ( \
            tokenid text NOT NULL, \
            fecha numeric(20) NOT NULL \
        ) ";

        let create_tmp_e = "  CREATE TEMP TABLE tmp_e ( \
            tokenid text NOT NULL, \
            fecha numeric(20) NOT NULL \
        ) ";

        let create_tmp_f = "  CREATE TEMP TABLE tmp_f ( \
            tokenid text NOT NULL, \
            fecha numeric(20) NOT NULL \
        ) ";

        let create_tmp_g = "  CREATE TEMP TABLE tmp_g ( \
            tokenid text NOT NULL, \
            fecha numeric(20) NOT NULL \
        ) ";

        let insert_tmp_a = "INSERT INTO tmp_a \
                        select tokenid, max(fecha) as fecha \
                        from nft_sell \
                        where collection = 'asac.near' \
                        and fecha <= extract(epoch from to_char('2022-10-27'::date, 'yyyy-mm-dd 23:59:59')::timestamp)*1000000000 \
                        and pricenear <> 0 \
                        group by \
                            tokenid \
        ";
        
        let insert_tmp_b = "INSERT INTO tmp_b \
                            select \
                                tokenid, max(fecha) as fecha \
                            from nft_buy ns \
                            where ns.collection = 'asac.near' \
                            and fecha <= extract(epoch from to_char('2022-10-27'::date, 'yyyy-mm-dd 23:59:59')::timestamp)*1000000000 \
                            group by tokenid \
        ";

        let insert_tmp_c = "INSERT INTO tmp_c \
                            select token_id, max(fecha) as fecha  \
                            from nft_collections_transaction nct \
                            where collection = 'asac.near' \
                            and fecha <= extract(epoch from to_char('2022-10-27'::date, 'yyyy-mm-dd 23:59:59')::timestamp)*1000000000 \
                            group by \
                                token_id \
        ";

        let insert_tmp_f = "INSERT INTO tmp_f \
                            select tokenid, max(fecha) as fecha \
                            from nft_sell \
                            where collection = 'asac.near' \
                            and fecha <= extract(epoch from to_char('2022-10-27'::date, 'yyyy-mm-dd 23:59:59')::timestamp)*1000000000 \
                            and pricenear = 0 \
                            group by \
                                tokenid \
        ";


        await conn.query(create_tmp_a);
        await conn.query('COMMIT');
        console.log("tabla temporal a creada")
        await conn.query(create_tmp_b);
        await conn.query('COMMIT');
        console.log("tabla temporal b creada")
        await conn.query(create_tmp_c);
        await conn.query('COMMIT');
        console.log("tabla temporal c creada")
        await conn.query(create_tmp_d);
        await conn.query('COMMIT');
        console.log("tabla temporal d creada")
        await conn.query(create_tmp_e);
        await conn.query('COMMIT');
        console.log("tabla temporal e creada")
        await conn.query(create_tmp_f);
        await conn.query('COMMIT');
        console.log("tabla temporal f creada")
        await conn.query(create_tmp_g);
        await conn.query('COMMIT');
        console.log("tabla temporal g creada")


        await conn.query(insert_tmp_a);
        await conn.query('COMMIT');
        console.log("registros insertados tabla temporal a")
        await conn.query(insert_tmp_b);
        await conn.query('COMMIT');
        console.log("registros insertados tabla temporal b")
        await conn.query(insert_tmp_c);
        await conn.query('COMMIT');
        console.log("registros insertados tabla temporal c")
        await conn.query(insert_tmp_f);
        await conn.query('COMMIT');
        console.log("registros insertados tabla temporal f")

        let ajuste_a = "    insert into tmp_d \
                            select tokenid, fecha from tmp_a \
                            where 0 = (select case when ( \
                                select 1 from tmp_b \
                                where tmp_b.tokenid = tmp_a.tokenid and tmp_b.fecha > tmp_a.fecha \
                            ) = 1 then 1 else 0 end) "

        let ajuste_b = "    insert into tmp_e \
                            select tokenid, fecha from tmp_d \
                            where 0 = (select case when ( \
                                select 1 from tmp_c \
                                where tmp_c.tokenid = tmp_d.tokenid and tmp_c.fecha > tmp_d.fecha \
                            ) = 1 then 1 else 0 end) "
                            
        let ajuste_c = "    insert into tmp_g \
                            select tokenid, fecha from tmp_e \
                            where 0 = (select case when ( \
                                select 1 from tmp_f \
                                where tmp_f.tokenid = tmp_e.tokenid and tmp_f.fecha > tmp_e.fecha \
                            ) = 1 then 1 else 0 end) "

        await conn.query(ajuste_a);
        await conn.query('COMMIT');
        console.log("ajustes registros a tabla temporal d")

        await conn.query(ajuste_b);
        await conn.query('COMMIT');
        console.log("ajustes registros b tabla temporal e")
        
        const result = await conn.query("select * from nft_sell ns inner join tmp_e te on te.tokenid = ns.tokenid and te.fecha = ns.fecha order by ns.pricenear asc limit 5");
        console.log(result.rows)
        

    } catch (error) {
        console.log('error 1: ', error)
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


async function updateProjects(fecha_epoch) {    
    try {
        const epoch = moment().subtract(200, 'd').valueOf()*1000000;
        const conn = await dbConnect2()

        console.log('inicio ----------------------------------------------------------------------------------------------------')
        const result = await conn.query(" select fecha from projects order by fecha desc limit 1 ")

        let fecha = fecha_epoch ? fecha_epoch.toString() : result.rows.length > 0 ? result.rows[0].fecha : epoch.toString()
        console.log(result.rows)
        console.log(fecha)

        queryGql = gql`
            query MyQuery($fecha: BigInt!) {
                projects(where: {fecha_gt: $fecha} orderBy: fecha, orderDirection: asc, first: 1000) {
                    email
                    fecha
                    twiter
                    already
                    website
                    telegram
                    upcoming
                    discord_id
                    descripcion
                    project_name
                    user_creation
                    discord_server
                    fecha_lanzamiento
                    id_contract_project
                }
            }
        `;        

        variables = { fecha: fecha }
        
        const projects = await colsutaTheGraph2(queryGql, variables)

        let rows = projects.projects

        if(rows.length == 0) {
            console.log('projects actualizacos')
            console.log('fin -------------------------------------------------------------------------------------------------------')
            return {respuesta: true}
        }

        let datos = ''
        const tamano_row = rows.length      
        for(var i = 0; i < rows.length; i++) {
        datos += "('"+rows[i].email.toString()+"', "+rows[i].fecha.toString()+", '"+rows[i].twiter.toString()+"', "+rows[i].already .toString()+", \
                '"+rows[i].website.toString()+"', '"+rows[i].telegram.toString()+"', "+rows[i].upcoming.toString()+", '"+rows[i].discord_id.toString()+"', \
                '"+rows[i].descripcion.toString()+"', '"+rows[i].project_name.toString()+"', '"+rows[i].user_creation.toString()+"', '"+rows[i].discord_server.toString()+"', \
                "+rows[i].fecha_lanzamiento.toString()+", '"+rows[i].id_contract_project.toString()+"')";
            
            datos += i != (tamano_row - 1) ? ", " : ""; 
        }

        let tabla_temp_projects = "  CREATE TEMP TABLE tmp_projects ( \
            email text NULL, \
            fecha numeric(20) NOT NULL, \
            twiter text NULL, \
            already bool NOT NULL, \
            website text NULL, \
            telegram text NULL, \
            upcoming bool NOT NULL, \
            discord_id text NULL, \
            descripcion text NULL, \
            project_name text NOT NULL, \
            user_creation text NOT NULL, \
            discord_server text NULL, \
            fecha_lanzamiento numeric(20) NULL, \
            id_contract_project text NOT NULL, \
        ) ";

        let insert_temp_projects = "INSERT INTO tmp_projects ( \
            email, \
            fecha, \
            twiter, \
            already, \
            website, \
            telegram, \
            upcoming, \
            discord_id, \
            descripcion, \
            project_name, \
            user_creation, \
            discord_server, \
            fecha_lanzamiento, \
            id_contract_project \
        ) \
        VALUES " + datos //#+ str(data);


        await conn.query(tabla_temp_projects);
        await conn.query('COMMIT');
        console.log("Tabla temporal creada")
        await conn.query(insert_temp_projects);
        await conn.query('COMMIT');
        console.log("Registros insertados en tabla temporal")

        let insert_projects = "  insert into projects \
                                select \
                                    user_creation, fecha, project_name, descripcion, email, discord_id, website, twiter, telegram, discord_server, \
                                    upcoming, already, fecha_lanzamiento, id_contract_project, false as listado \
                                from tmp_projects t \
                                where 0 = ( \
                                            select case when ( \
                                                                select 1 from projects m \
                                                                where m.id_contract_project = t.id_contract_project \
                                                                ) = 1 \
                                                    then 1 else 0 end \
                                            ) \
        ";

        await conn.query(insert_projects);
        await conn.query('COMMIT');            
        console.log("Registros insertados en tabla final projects")

        let delete_temp_projects = "  drop table tmp_projects "
        await conn.query(delete_temp_projects);
        await conn.query('COMMIT');
        console.log("Tabla temporal eliminada")
        
        //console.log(array)
        await listarCollections();

        console.log('fin update projects')
        console.log('projects actualizacos')
        console.log('fin -------------------------------------------------------------------------------------------------------')



        /*const conn = await dbConnect2();
        
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
        await conn.query("commit");*/
        
        
        

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
                                            collection, \
                                            market as marketplace \
                                        from nft_sell ns \
                                        where collection = $1 \
                                        group by \
                                            collection, market \
                                    ) sub \
                                    where sub.collection not in (select collection from collections_marketplace mc \
                                        where mc.collection = sub.collection and mc.marketplace = sub.marketplace) \
        ", [collection]);
        await conexion2.query("commit");
        
    } catch (error) {
        console.log('error 1: ', error)
    }
}


async function UpdateVotes(fecha_epoch) {
    console.log('actualizando votos')
    try {
        const epoch = moment().subtract(200, 'd').valueOf()*1000000;
        const conn = await dbConnect2()

        console.log('inicio ----------------------------------------------------------------------------------------------------')
        const result = await conn.query(" select fecha from votos order by fecha desc limit 1 ")

        let fecha = fecha_epoch ? fecha_epoch.toString() : result.rows.length > 0 ? result.rows[0].fecha : epoch.toString()
        console.log(result.rows)
        console.log(fecha)

        queryGql = gql`
            query MyQuery($fecha: BigInt!) {
                votos(where: {fecha_gt: $fecha} orderBy: fecha, orderDirection: asc, first: 1000) {
                    voto
                    fecha
                    user_id
                    collection
                }
            }
        `;        

        variables = { fecha: fecha }
        
        const votos = await colsutaTheGraph2(queryGql, variables)

        let rows = votos.votos

        if(rows.length == 0) {
            console.log('votos actualizacos')
            console.log('fin -------------------------------------------------------------------------------------------------------')
            return {respuesta: true}
        }

        let datos = ''
        const tamano_row = rows.length      
        for(var i = 0; i < rows.length; i++) {
        datos += "('"+rows[i].voto.toString()+"', "+rows[i].fecha.toString()+", '"+rows[i].user_id.toString()+"', '"+rows[i].collection .toString()+"')";
            
            datos += i != (tamano_row - 1) ? ", " : ""; 
        }

        let tabla_temp_votos = "  CREATE TEMP TABLE tmp_votos ( \
            voto text NOT NULL, \
            fecha numeric(20) NOT NULL, \
            user_id text NOT NULL, \
            collection text NOT NULL \
        ) ";

        let insert_temp_votos = "INSERT INTO tmp_votos ( \
            voto, \
            fecha, \
            user_id, \
            collection \
        ) \
        VALUES " + datos //#+ str(data);


        await conn.query(tabla_temp_votos);
        await conn.query('COMMIT');
        console.log("Tabla temporal creada")
        await conn.query(insert_temp_votos);
        await conn.query('COMMIT');
        console.log("Registros insertados en tabla temporal")

        let insert_votos = "  insert into votos \
                                select \
                                    collection, user_id, fecha, voto \
                                from tmp_votos t \
                                where 0 = ( \
                                            select case when ( \
                                                                select 1 from votos m \
                                                                where m.collection = t.collection \
                                                                and m.user_id = t.user_id \
                                                                ) = 1 \
                                                    then 1 else 0 end \
                                            ) \
        ";

        let update_votos = "   update votos \
                                    set \
                                        voto = t.voto, \
                                        fecha = t.fecha \
                                    from tmp_votos t \
                                    where t.collection = votos.collection \
                                    and t.user_id = votos.user_id \
                                    and t.fecha > votos.fecha \
        ";

        await conn.query(insert_votos);
        await conn.query('COMMIT');            
        console.log("Registros insertados en tabla final votos")

        await conn.query(update_votos);
        await conn.query('COMMIT');            
        console.log("actualizar registros en tabla final votos")


        let delete_temp_votos = "  drop table tmp_votos "
        await conn.query(delete_temp_votos);
        await conn.query('COMMIT');
        console.log("Tabla temporal eliminada")
        
        //console.log(array)
        
        console.log('votos actualizacos')
        console.log('fin -------------------------------------------------------------------------------------------------------')
            
        return {respuesta: true}
    } catch (error) {
        console.log('error: ', error)
        console.log('Error al actualizacos votos')
        return {respuesta: false, error: error}
    }
}


async function UpdateVotesUpcoming(fecha_epoch) {
    console.log('actualizando votos_upcoming')
    try {
        const epoch = moment().subtract(200, 'd').valueOf()*1000000;
        const conn = await dbConnect2()

        console.log('inicio ----------------------------------------------------------------------------------------------------')
        const result = await conn.query(" select fecha from votos_upcoming order by fecha desc limit 1 ")

        let fecha = fecha_epoch ? fecha_epoch.toString() : result.rows.length > 0 ? result.rows[0].fecha : epoch.toString()
        console.log(result.rows)
        console.log(fecha)

        queryGql = gql`
            query MyQuery($fecha: BigInt!) {
                votoupcomings(where: {fecha_gt: $fecha} orderBy: fecha, orderDirection: asc, first: 1000) {
                    voto
                    fecha
                    user_id
                    collection
                }
            }
        `;        

        variables = { fecha: fecha }
        
        const votoupcomings = await colsutaTheGraph2(queryGql, variables)

        let rows = votoupcomings.votoupcomings

        if(rows.length == 0) {
            console.log('votos_upcoming actualizacos')
            console.log('fin -------------------------------------------------------------------------------------------------------')
            return {respuesta: true}
        }

        let datos = ''
        const tamano_row = rows.length      
        for(var i = 0; i < rows.length; i++) {
        datos += "('"+rows[i].voto.toString()+"', "+rows[i].fecha.toString()+", '"+rows[i].user_id.toString()+"', '"+rows[i].collection .toString()+"')";
            
            datos += i != (tamano_row - 1) ? ", " : ""; 
        }

        let tabla_temp_votos = "  CREATE TEMP TABLE tmp_votos_upcoming ( \
            voto text NOT NULL, \
            fecha numeric(20) NOT NULL, \
            user_id text NOT NULL, \
            collection text NOT NULL \
        ) ";

        let insert_temp_votos = "INSERT INTO tmp_votos_upcoming ( \
            voto, \
            fecha, \
            user_id, \
            collection \
        ) \
        VALUES " + datos //#+ str(data);


        await conn.query(tabla_temp_votos);
        await conn.query('COMMIT');
        console.log("Tabla temporal creada")
        await conn.query(insert_temp_votos);
        await conn.query('COMMIT');
        console.log("Registros insertados en tabla temporal")

        let insert_votos = "  insert into votos_upcoming \
                                select \
                                    collection, user_id, fecha, voto \
                                from tmp_votos_upcoming t \
                                where 0 = ( \
                                            select case when ( \
                                                                select 1 from votos_upcoming m \
                                                                where m.collection = t.collection \
                                                                and m.user_id = t.user_id \
                                                                ) = 1 \
                                                    then 1 else 0 end \
                                            ) \
        ";

        let update_votos = "   update votos_upcoming \
                                    set \
                                        voto = t.voto, \
                                        fecha = t.fecha \
                                    from tmp_votos_upcoming t \
                                    where t.collection = votos_upcoming.collection \
                                    and t.user_id = votos_upcoming.user_id \
                                    and t.fecha > votos_upcoming.fecha \
        ";

        await conn.query(insert_votos);
        await conn.query('COMMIT');            
        console.log("Registros insertados en tabla final votos_upcoming")

        await conn.query(update_votos);
        await conn.query('COMMIT');            
        console.log("actualizar registros en tabla final votos_upcoming")


        let delete_temp_votos = "  drop table tmp_votos_upcoming "
        await conn.query(delete_temp_votos);
        await conn.query('COMMIT');
        console.log("Tabla temporal eliminada")
        
        //console.log(array)
        
        console.log('votos_upcoming actualizacos')
        console.log('fin -------------------------------------------------------------------------------------------------------')
            
        return {respuesta: true}
    } catch (error) {
        console.log('error: ', error)
        console.log('Error al actualizacos votos_upcoming')
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

module.exports = { Databuy, DataSell, DataMarket, HistFloor, updateTransactions, updateProjects, UpdateVotes, UpdateVotesUpcoming, update_masivo_collections }





















