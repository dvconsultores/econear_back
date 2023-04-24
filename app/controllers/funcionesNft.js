const { CONFIG } = require('../helpers/utils')
const { dbConnect, dbConnect2 } = require('../../config/postgres')
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

const { listarCollectionsMarketplace } = require('./funciones')


const { utils, Contract, keyStores, KeyPair, Near, Account } = nearAPI


const SIGNER_ID = process.env.SIGNER_ID;
const SIGNER_PRIVATEKEY = process.env.SIGNER_PRIVATEKEY;
const NETWORK = process.env.NETWORK;

const keyStore = new keyStores.InMemoryKeyStore()
const keyPair = KeyPair.fromString(SIGNER_PRIVATEKEY)
keyStore.setKey(NETWORK, SIGNER_ID, keyPair)
const near = new Near(CONFIG(keyStore))
const account = new Account(near.connection, SIGNER_ID)


async function ListarNft() {
    try {
        const conn_inicial = await dbConnect2()
        
        const resultados = await conn_inicial.query(" select nft_contract from collections where listar_collections = false ")
        
        console.log('aqui paso')
        const row = resultados.rows
        const conn_destino = await dbConnect2()
        const conn_origen = await dbConnect()
        for(var x = 0; x < row.length; x++ ) {
            console.log("este es el contrato: ", row[x].nft_contract)
            let offset = 0;
            let detener = false;
            for(var i = 0; i < 100; i++) {
                if(detener) { break }
                for(var j = 0; j < 20; j++) {
                    try {
                        
                        
                        console.log('Tiempo inicio de ejecución  NftTemporal ', ' - offset = ', offset)
                        let query = " select emitted_for_receipt_id, cast(emitted_at_block_timestamp as char(20)) as emitted_at_block_timestamp, ";
                        query += " emitted_by_contract_account_id, token_id, event_kind, token_new_owner_account_id, token_old_owner_account_id ";
                        query += " from assets__non_fungible_token_events anfte ";
                        query += " where emitted_by_contract_account_id = '"+row[x].nft_contract.toString()+"'";
                        query += " limit 20000 offset ";
                        query += offset.toString();

                        console.log('aqui paso 1')

                        console.log('Consultando nft')
                        
                        const nft_encontrados = await conn_origen.query(query)
                        
                        let rows = nft_encontrados.rows;

                        if(rows.length == 0) {
                            detener = true
                            console.log('fin consulta NftTemporal')
                            console.log('Tiempo de ejecución NftTemporal ', ' - offset = ', offset)
                            break;
                        }
                  
                        console.log('----------------------------------------------------------------------------------------------------')
                        let datos = ''
                        const tamano_row = rows.length      
                        for(var ii = 0; ii < rows.length; ii++) {
                        datos += "('"+rows[ii].emitted_for_receipt_id.toString()+"', "+rows[ii].emitted_at_block_timestamp.toString()+", \
                            '"+rows[ii].emitted_by_contract_account_id.toString()+"', '"+rows[ii].token_id.toString()+"', '"+rows[ii].event_kind.toString()+"', \
                            '"+rows[ii].token_new_owner_account_id.toString()+"', '"+rows[ii].token_old_owner_account_id.toString()+"')";
                            
                            datos += ii != (tamano_row - 1) ? ", " : ""; 
                        }
                        console.log('----------------------------------------------------------------------------------------------------')
                
                        let query2 = "  CREATE TEMP TABLE tmp_nft_a ( \
                                emitted_for_receipt_id text NULL, \
                                emitted_at_block_timestamp numeric(20) NULL, \
                                emitted_by_contract_account_id text NULL, \
                                token_id text NULL, \
                                event_kind text NULL, \
                                token_new_owner_account_id text NULL, \
                                token_old_owner_account_id text null \
                            ) ";

                        let query3 = "INSERT INTO tmp_nft_a ( \
                                        emitted_for_receipt_id, \
                                        emitted_at_block_timestamp, \
                                        emitted_by_contract_account_id, \
                                        token_id, \
                                        event_kind, \
                                        token_new_owner_account_id, \
                                        token_old_owner_account_id \
                                    ) \
                                    VALUES " + datos.toString();

                        
                        console.log("creando tabla temporal")
                        await conn_destino.query(query2)
                        console.log("insertando tabla temporal")
                        await conn_destino.query(query3)
                        await conn_destino.query("commit")
                        
                        //#-----------------------------------------------------------------------------------------------------------------------------#
                        
                        console.log('actualizando nft')
                 
                        let query4 = " insert into nft_collections \
                                    select \
                                        emitted_for_receipt_id, emitted_at_block_timestamp, emitted_by_contract_account_id, temp1.token_id, token_new_owner_account_id, \
                                        null as titulo, null as descripcion, null as media, null as extra, null as reference \
                                    from tmp_nft_a temp1 \
                                    inner join ( \
                                        select \
                                            max(emitted_at_block_timestamp) as fecha, \
                                            emitted_by_contract_account_id as collection, \
                                            token_id \
                                        from tmp_nft_a \
                                        where event_kind = 'MINT' \
                                        and emitted_by_contract_account_id = $1 \
                                        group by emitted_by_contract_account_id, token_id \
                                    ) temp2 on temp2.fecha = temp1.emitted_at_block_timestamp \
                                        and temp2.collection = temp1.emitted_by_contract_account_id \
                                        and temp2.token_id = temp1.token_id \
                                    where emitted_by_contract_account_id = $2 \
                                    and 0 = (select case when (select 1 from nft_collections where collection = temp1.emitted_by_contract_account_id and token_id = temp1.token_id group by 1) = 1 then 1 else 0 end) ";

                        let query5 = "   insert into nft_collections_transaction \
                                    select \
                                        emitted_for_receipt_id, emitted_at_block_timestamp, emitted_by_contract_account_id, temp1.token_id, \
                                        token_new_owner_account_id, event_kind, token_old_owner_account_id \
                                    from tmp_nft_a temp1 \
                                    where emitted_by_contract_account_id = $1 \
                                    and event_kind <> 'MINT' \
                                    and 0 = (select case when (select 1 from nft_collections_transaction sub where \
                                    sub.emitted_for_receipt_id = temp1.emitted_for_receipt_id and sub.collection = temp1.emitted_by_contract_account_id \
                                    and sub.token_id = temp1.token_id and sub.event_kind = temp1.event_kind \
                                    and sub.token_new_owner_account_id = temp1.token_new_owner_account_id \
                                    and sub.fecha = temp1.emitted_at_block_timestamp group by 1) = 1 then 1 else 0 end) ";



                        let query6 = " DELETE FROM nft_collections \
                                    WHERE EXISTS \
                                        (SELECT 1 \
                                        from tmp_nft_a temp1 \
                                        inner join ( \
                                            select \
                                                max(emitted_at_block_timestamp) as fecha, \
                                                emitted_by_contract_account_id as collection, \
                                                token_id \
                                            from tmp_nft_a \
                                            where event_kind = 'BURN' and  emitted_by_contract_account_id = $1 \
                                            group by emitted_by_contract_account_id, token_id \
                                        ) temp2 on temp2.fecha = temp1.emitted_at_block_timestamp \
                                            and temp2.collection = temp1.emitted_by_contract_account_id \
                                            and temp2.token_id = temp1.token_id \
                                        where temp1.event_kind = 'BURN' \
                                        and temp1.token_id = nft_collections.token_id \
                                        and temp1.emitted_by_contract_account_id = nft_collections.collection \
                                        AND temp1.emitted_at_block_timestamp > nft_collections.fecha) ";

                        let query7 = " update nft_collections \
                                    set \
                                        fecha = temp1.emitted_at_block_timestamp, \
                                        token_new_owner_account_id = temp1.token_new_owner_account_id \
                                    from tmp_nft_a temp1 \
                                    inner join ( \
                                        select \
                                            max(emitted_at_block_timestamp) as fecha, \
                                            emitted_by_contract_account_id as collection, \
                                            token_id \
                                        FROM tmp_nft_a \
                                        WHERE tmp_nft_a.event_kind = 'TRANSFER' \
                                        and emitted_by_contract_account_id = $1 \
                                        group by emitted_by_contract_account_id, token_id \
                                    ) temp2 on temp2.collection = temp1.emitted_by_contract_account_id \
                                        and temp2.fecha = temp1.emitted_at_block_timestamp \
                                        and temp2.token_id = temp1.token_id \
                                    where temp1.event_kind = 'TRANSFER' \
                                    and temp1.token_id = nft_collections.token_id \
                                    and temp1.emitted_by_contract_account_id = nft_collections.collection \
                                    and temp1.emitted_at_block_timestamp > nft_collections.fecha ";
                        

                        await conn_destino.query(query4, [row[x].nft_contract, row[x].nft_contract])
                        await conn_destino.query("commit")
                        console.log('paso 1')
                        await conn_destino.query(query5, [row[x].nft_contract])
                        await conn_destino.query("commit")
                        console.log('paso 2')
                        await conn_destino.query(query6, [row[x].nft_contract])
                        await conn_destino.query("commit")
                        console.log('paso 3')
                        await conn_destino.query(query7, [row[x].nft_contract])
                        await conn_destino.query("commit")
                        console.log('paso 4')
                        
                        await listarCollectionsMarketplace(row[x].nft_contract)

                        console.log('fin consulta transactions')

                        offset += 20000;
                        console.log('fin consulta NftTemporal')
                        console.log('Tiempo de ejecución NftTemporal ', ' - offset = ', offset)
                            
                        break
                        pool.end().then(() => console.log('pool has ended'))
                    } catch (error) {
                        console.log('error consulta origen NftTemporal')
                        console.log('error NftTemporal: ', error)
                        //if "User query might have needed to see row versions that must be removed" in str(e):
                        //    print("----funciono")
                    }
                }
            }
        }
        console.log('Culminado listado de nft')
        console.log('Tiempo de ejecución listado nft ')
        conn_inicial.end()
        conn_destino.end()
        conn_origen.end()
    } catch (error) {  
        console.log("error general listar nft: ", error)
    }
}


async function CargarRutaIfsImgNft() {
    try {
        const conexion = await dbConnect2()

        const response_nft = await conexion.query("select \
                                                            nc.collection, nc.token_id, nc.token_new_owner_account_id as owner_id, c.base_uri \
                                                        from nft_collections nc \
                                                        inner join collections c on c.nft_contract = nc.collection \
                                                        where titulo is null"
                                                    );
        
        const data = response_nft.rows;

        console.log('corriendo actualizacion datos nft')
        for(var i = 0; i < data.length; i++) {
            const contract = new Contract(account, data[i].collection, {
                viewMethods: ["nft_token"],
                sender: account,
            });

            await contract.nft_token({
                token_id: data[i].token_id
            }).then(async (response) => {
                const item = response;
                const conexion2 = await dbConnect2()
                if(item == null) {
                    await conexion2.query("  delete from nft_collections where collection = $1 and token_id = $2 ", [ data[i].collection, data[i].token_id]);
                    await conexion2.query("commit")    
                    console.log('Eliminado')
                } else {
                    //console.log(item.metadata)
                    if(item.metadata.media.split(':')[0].toUpperCase() != 'HTTPS') {
                        item.metadata.media = data[i].base_uri + '/' + item.metadata.media
                    }
            
                    await conexion2.query("  update nft_collections \
                                                set \
                                                    titulo = $1, \
                                                    descripcion = $2, \
                                                    media = $3, \
                                                    extra = $4, \
                                                    reference = $5 \
                                            where collection = $6 and token_id = $7 \
                                        ", [item.metadata.title, item.metadata.description, item.metadata.media,
                                            item.metadata.extra, item.metadata.reference, data[i].collection, data[i].token_id]);
                    await conexion2.query("commit")
                }
            }).catch(err => {
                console.log("error 1 actualizacion de datos nft: ", err)
            });
        }
        console.log("culminado actualizacion datos nft")
        conexion.end()
        conexion2.end()
    } catch (error) {
        console.log('error 2 actualizacion de datos nft: ', error)
        return error
    }
}

async function CargarJsonAtributosNft2() {
    try {
        const conexion = await dbConnect2() 
        const intervalo = await setInterval(async function () { 
        /*const response_nft = await conexion.query("select \
                                                            nc.collection, nc.token_id, c.base_uri,  \
                                                        from nft_collections3 nc \
                                                        inner join collections c on c.nft_contract = nc.collection \
                                                        where titulo is null"
        );*/
        
        let ciclos = 1
        //while (ciclos) {                                           
            console.log('ciclo #', ciclos)
            const nfts = await conexion.query(" select collection, descripcion, token_id, atributos from ( \
                                                    select nc.collection, descripcion, nc.token_id, nc.reference as atributos, atributos as campo_atributos \
                                                    from nft_collections3 nc \
                                                    where substring(reference, 1, 5) = 'https' \
                                                    union all \
                                                    select nc.collection, descripcion, nc.token_id, c.base_uri||'/'||nc.reference as atributos, atributos as campo_atributos \
                                                    from nft_collections3 nc \
                                                    inner join collections c on c.nft_contract = nc.collection \
                                                    where upper(split_part(nc.reference, '.', 2)) = upper('json') and substring(nc.reference, 1, 5) <> 'https' \
                                                ) sub where campo_atributos is null limit 100 \
            ");        
            
            if(nfts.rows.length == 0) {
                 clearInterval(intervalo) 
            }

            //const intervalo = setTimeout(async function () {
                for(let i = 0; i < nfts.rows.length; i++){
                    axios.get(nfts.rows[i].atributos).then(async item => {
                        let descripcion = nfts.rows[i].descripcion
                        if(item.data.description) {
                            if(descripcion == null) {
                                descripcion = item.data.description
                            }
                        }
                        console.log(descripcion)
                        console.log(nfts.rows[i].token_id)
                        if(item.data.attributes){
                            await conexion.query("  update nft_collections3 \
                                                    set \
                                                        descripcion = $1, \
                                                        atributos =$2 \
                                                    where \
                                                        collection = $3 \
                                                        and token_id = $4 \
                            ", [descripcion, JSON.stringify(item.data.attributes), nfts.rows[i].collection, nfts.rows[i].token_id])
                        }  
                    }).catch(error => {
                        console.log('Error consulta axios: ', error)
                    })
                }
                ciclos += 1
            }, 30000)

            //clearTimeout(intervalo)
        
        console.log("culminado actualizacion datos nft")
        conexion.end()
    } catch (error) {
        console.log('error 2 actualizacion de datos nft: ', error)
        return error
    }
}


async function CargarJsonAtributosNft() {
    try {
        console.log('inicio ')
        const conexion = await dbConnect2() 
        const intervalo = await setInterval(async function () { 
        /*const response_nft = await conexion.query("select \
                                                            nc.collection, nc.token_id, c.base_uri,  \
                                                        from nft_collections3 nc \
                                                        inner join collections c on c.nft_contract = nc.collection \
                                                        where titulo is null"
        );*/
        
        //let ciclos = 1
        //while (ciclos) {                                           
            const nfts = await conexion.query(" select collection, descripcion, token_id, atributos from ( \
                                                    select nc.collection, descripcion, nc.token_id, nc.reference as atributos, atributos as campo_atributos \
                                                    from nft_collections3 nc \
                                                    where substring(reference, 1, 5) = 'https' \
                                                    union all \
                                                    select nc.collection, descripcion, nc.token_id, c.base_uri||'/'||nc.reference as atributos, atributos as campo_atributos \
                                                    from nft_collections3 nc \
                                                    inner join collections c on c.nft_contract = nc.collection \
                                                    where upper(split_part(nc.reference, '.', 2)) = upper('json') and substring(nc.reference, 1, 5) <> 'https' \
                                                ) sub where campo_atributos is null limit 100 \
            ");        
            console.log(nfts.rows.length)
            if(nfts.rows.length == 0) {
                 clearInterval(intervalo) 
            }
            //and collection = 'asac.near'
            //const intervalo = setTimeout(async function () {
                for(let i = 0; i < nfts.rows.length; i++){
                    axios.get(nfts.rows[i].atributos).then(async item => {
                        let descripcion = nfts.rows[i].descripcion
                        if(item.data.description) {
                            if(descripcion == null) {
                                descripcion = item.data.description
                            }
                        }
                        console.log(descripcion)
                        console.log(nfts.rows[i].token_id)
                        
                        if(item.data.attributes && item.data.attributes.length > 0){
                            if(item.data.attributes[0].trait_type && item.data.attributes[0].value){
                                const result = await conexion.query("  update nft_collections3 \
                                                        set \
                                                            descripcion = $1, \
                                                            atributos =$2 \
                                                        where \
                                                            collection = $3 \
                                                            and token_id = $4 \
                                ", [descripcion, JSON.stringify(item.data.attributes), nfts.rows[i].collection, nfts.rows[i].token_id])
                                let atributos = item.data.attributes
                                let datos = ''
                                const tamano_row = atributos.length
                                for(var ii = 0; ii < tamano_row; ii++) {
                                datos += "('"+nfts.rows[i].collection.toString()+"', '"+nfts.rows[i].token_id.toString()+"', \
                                    '"+atributos[ii].trait_type.toString()+"', '"+atributos[ii].value.toString()+"')";
                                    
                                    datos += ii != (tamano_row - 1) ? ", " : ""; 
                                }

                                console.log(result.rowCount)
                                if(result.rowCount == 1) {
                                    let insert_tabla_atributos = "INSERT INTO atributos ( \
                                        collection, \
                                        token_id, \
                                        trait_type, \
                                        value \
                                    ) \
                                    VALUES " + datos.toString();

                                    await conexion.query(insert_tabla_atributos)
                                    console.log('atributos insertados')
                                }
                            }

                        }
                    }).catch(error => {
                        console.log('Error consulta axios: ', error)
                    })
                }
                //ciclos += 1
            }, 20000)
            //clearInterval(intervalo) 
        console.log("culminado actualizacion datos nft")
        conexion.end()
    } catch (error) {
        console.log('error 2 actualizacion de datos nft: ', error)
        return error
    }
}
async function UpdateNft(epoch_h) {
    console.log("-------------------------- UpdateNft ----------------------------------------------------------")
    try {
        console.log('aqui paso')
        const conn_destino = await dbConnect2()
        const conn_origen = await dbConnect()
        let offset = 0;
        let detener = false;
        for(var i = 0; i < 100; i++) {
            if(detener) { break }
            for(var j = 0; j < 20; j++) {
                try {
                    
                    
                    console.log('Tiempo inicio de ejecución  NftTemporal ', ' - offset = ', offset)
                    let query = " select emitted_for_receipt_id, cast(emitted_at_block_timestamp as char(20)) as emitted_at_block_timestamp, ";
                    query += " emitted_by_contract_account_id, token_id, event_kind, token_new_owner_account_id, token_old_owner_account_id ";
                    query += " from assets__non_fungible_token_events anfte ";
                    query += " where emitted_at_block_timestamp >= $1 ";
                    query += " limit 7000 offset ";
                    query += offset.toString();

                    console.log('aqui paso')

                    console.log('Consultando nft')
                    
                    const nft_encontrados = await conn_origen.query(query, [epoch_h])
                    
                    let rows = nft_encontrados.rows;

                    if(rows.length == 0) {
                        detener = true
                        console.log('fin consulta NftTemporal')
                        console.log('Tiempo de ejecución NftTemporal ', ' - offset = ', offset)
                        break;
                    }

                    console.log('----------------------------------------------------------------------------------------------------')
                    let datos = ''
                    const tamano_row = rows.length      
                    for(var ii = 0; ii < rows.length; ii++) {
                    datos += "('"+rows[ii].emitted_for_receipt_id.toString()+"', "+rows[ii].emitted_at_block_timestamp.toString()+", \
                        '"+rows[ii].emitted_by_contract_account_id.toString()+"', '"+rows[ii].token_id.toString()+"', '"+rows[ii].event_kind.toString()+"', \
                        '"+rows[ii].token_new_owner_account_id.toString()+"', '"+rows[ii].token_old_owner_account_id.toString()+"')";
                        
                        datos += ii != (tamano_row - 1) ? ", " : ""; 
                    }
                    console.log('----------------------------------------------------------------------------------------------------')
            
                    let query2 = "  CREATE TEMP TABLE tmp_nft_a ( \
                            emitted_for_receipt_id text NULL, \
                            emitted_at_block_timestamp numeric(20) NULL, \
                            emitted_by_contract_account_id text NULL, \
                            token_id text NULL, \
                            event_kind text NULL, \
                            token_new_owner_account_id text NULL, \
                            token_old_owner_account_id text NULL \
                        ) ";
 
                    let query3 = "INSERT INTO tmp_nft_a ( \
                                    emitted_for_receipt_id, \
                                    emitted_at_block_timestamp, \
                                    emitted_by_contract_account_id, \
                                    token_id, \
                                    event_kind, \
                                    token_new_owner_account_id, \
                                    token_old_owner_account_id \
                                ) \
                                VALUES " + datos.toString();

                    
                    console.log("creando tabla temporal")
                    await conn_destino.query(query2)
                    console.log('se creo la tabla temporal')
                    console.log("insertando tabla temporal")
                    await conn_destino.query(query3)
                    await conn_destino.query("commit")
                    
                    //#-----------------------------------------------------------------------------------------------------------------------------#
                    
                    console.log('actualizando nft')
                
                    let query4 = " insert into nft_collections \
                                select \
                                    emitted_for_receipt_id, emitted_at_block_timestamp, emitted_by_contract_account_id, temp1.token_id, token_new_owner_account_id, \
                                    null as titulo, null as descripcion, null as media, null as extra, null as reference \
                                from tmp_nft_a temp1 \
                                inner join ( \
                                    select \
                                        max(emitted_at_block_timestamp) as fecha, \
                                        emitted_by_contract_account_id as collection, \
                                        token_id \
                                    from tmp_nft_a \
                                    where event_kind = 'MINT' \
                                    group by emitted_by_contract_account_id, token_id \
                                ) temp2 on temp2.fecha = temp1.emitted_at_block_timestamp \
                                    and temp2.collection = temp1.emitted_by_contract_account_id \
                                    and temp2.token_id = temp1.token_id \
                                where 0 = (select case when (select 1 from nft_collections where collection = temp1.emitted_by_contract_account_id and token_id = temp1.token_id group by 1) = 1 then 1 else 0 end) ";

                    let query5 = "   insert into nft_collections_transaction \
                                select \
                                    emitted_for_receipt_id, emitted_at_block_timestamp, emitted_by_contract_account_id, temp1.token_id, \
                                    token_new_owner_account_id, event_kind, token_old_owner_account_id \
                                from tmp_nft_a temp1 \
                                where event_kind <> 'MINT' \
                                and 0 = (select case when (select 1 from nft_collections_transaction sub where \
                                sub.emitted_for_receipt_id = temp1.emitted_for_receipt_id and sub.collection = temp1.emitted_by_contract_account_id \
                                and sub.token_id = temp1.token_id and sub.event_kind = temp1.event_kind \
                                and sub.token_new_owner_account_id = temp1.token_new_owner_account_id \
                                and sub.fecha = temp1.emitted_at_block_timestamp group by 1) = 1 then 1 else 0 end) ";



                    let query6 = " DELETE FROM nft_collections \
                                WHERE EXISTS \
                                    (SELECT 1 \
                                    from tmp_nft_a temp1 \
                                    inner join ( \
                                        select \
                                            max(emitted_at_block_timestamp) as fecha, \
                                            emitted_by_contract_account_id as collection, \
                                            token_id \
                                        from tmp_nft_a \
                                        where event_kind = 'BURN' \
                                        group by emitted_by_contract_account_id, token_id \
                                    ) temp2 on temp2.fecha = temp1.emitted_at_block_timestamp \
                                        and temp2.collection = temp1.emitted_by_contract_account_id \
                                        and temp2.token_id = temp1.token_id \
                                    where temp1.event_kind = 'BURN' \
                                    and temp1.token_id = nft_collections.token_id \
                                    and temp1.emitted_by_contract_account_id = nft_collections.collection \
                                    AND temp1.emitted_at_block_timestamp > nft_collections.fecha) ";

                    let query7 = " update nft_collections \
                                set \
                                    fecha = temp1.emitted_at_block_timestamp, \
                                    token_new_owner_account_id = temp1.token_new_owner_account_id \
                                from tmp_nft_a temp1 \
                                inner join ( \
                                    select \
                                        max(emitted_at_block_timestamp) as fecha, \
                                        emitted_by_contract_account_id as collection, \
                                        token_id \
                                    FROM tmp_nft_a \
                                    WHERE tmp_nft_a.event_kind = 'TRANSFER' \
                                    group by emitted_by_contract_account_id, token_id \
                                ) temp2 on temp2.collection = temp1.emitted_by_contract_account_id \
                                    and temp2.fecha = temp1.emitted_at_block_timestamp \
                                    and temp2.token_id = temp1.token_id \
                                where temp1.event_kind = 'TRANSFER' \
                                and temp1.token_id = nft_collections.token_id \
                                and temp1.emitted_by_contract_account_id = nft_collections.collection \
                                and temp1.emitted_at_block_timestamp > nft_collections.fecha ";
                    

                    await conn_destino.query(query4)
                    await conn_destino.query("commit")
                    console.log('paso 1')
                    await conn_destino.query(query5)
                    await conn_destino.query("commit")
                    console.log('paso 2')
                    await conn_destino.query(query6)
                    await conn_destino.query("commit")
                    console.log('paso 3')
                    await conn_destino.query(query7)
                    await conn_destino.query("commit")
                    console.log('paso 4')
                    
                    
                    console.log('fin consulta transactions')

                    offset += 7000;
                    console.log('fin consulta NftTemporal')
                    console.log('Tiempo de ejecución NftTemporal ', ' - offset = ', offset)
                        
                    break
                } catch (error) {
                    console.log('error consulta origen NftTemporal')
                    console.log('error NftTemporal: ', error)
                    //if "User query might have needed to see row versions that must be removed" in str(e):
                    //    print("----funciono")
                }
            }
        }
        
        console.log('Culminado update de nft')
        console.log('Tiempo de ejecución listado nft ')
        conn_origen.end()
        conn_destino.end()
    } catch (error) {  
        console.log("error general listar nft: ", error)
    }
}

module.exports = { ListarNft, CargarRutaIfsImgNft, CargarJsonAtributosNft, UpdateNft }





















