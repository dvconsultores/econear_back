const { dbConnect2 } = require('../../config/postgres')
 
const NftCollection = async (collection, tokens) => {
    try {
        console.log(tokens)
        const conexion = await dbConnect2()
        let token = ''
        for(let i = 0; i < tokens.length; i++) {
            token += "'"+tokens[i]+"'"
            if(i != tokens.length -1) {
                token += ","
            }
        }

        const resultados = await conexion.query("   select nc.token_id, nc.token_new_owner_account_id  as owner, titulo, coalesce(nc.media, '') as media, \
                                                        coalesce(nc.extra, '') as extra, coalesce(nc.reference, '') as reference, coalesce(nc.media_pinata, '') as media_pinata, \
                                                        nc.collection, c.name as name_collection, coalesce(c.icon, '') as icon_collection \
                                                    from nft_collections nc \
                                                    inner join collections c on c.nft_contract = nc.collection \
                                                    where collection = $1 \
                                                    and token_id in ("+token+") ", [collection])
        
        //console.log(resultados.rows)
        return resultados.rows
    } catch (error) {
        console.log(error)
        return []
    } 
}

const DeleteMarket = async (collection, tokens) => {
    try {
        const conexion = await dbConnect2()
        let token = ''
        for(let i = 0; i < tokens.length; i++) {
            token += "'"+tokens[i]+"'"
            if(i != tokens.length -1) {
                token += ","
            }
        }
        
        let query = "   select max(fecha) as fecha, token_id  \
                        from nft_collections_transaction nct \
                        where collection = $1 and token_id in ("+token+") \
                        group by token_id "
        
        const resultados = await conexion.query(query, [collection])
        
        //console.log(resultados.rows)
        return resultados.rows
    } catch (error) {
        console.log(error)
        return []
    } 
}



module.exports = { NftCollection, DeleteMarket }