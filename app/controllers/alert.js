const { dbConnect, dbConnect2, dbConnect3 } = require('../../config/postgres')

const axios = require('axios');
const moment = require('moment');




const AlertPrice = async (req, res) => {
    try {
        const { collection } = req.body;

        const conexion = await dbConnect2();
        
        let query = "   select \
                            precio_near as price, \
                            collection, case when c.flag_pinata = true then icon_pinata else icon end as icon \
                        from nft_marketplace nm \
                        inner join collections c on c.nft_contract = nm.collection \
                        where precio_near > 0 \
                        and collection = $1 \
                        order by \
                            precio_near asc \
                        limit 1 ";
        
        const resultados = await conexion.query(query, [collection]);
        const data = resultados.rows;
        conexion.end();
        res.json(data)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: "error", error: error})
    }
}


const AlertVolumen = async (req, res) => {
    try {
        const { collection } = req.body;

        const conexion = await dbConnect2();
        
        let query = "   select \
                            collection, sum(pricenear) as volumen, \
                            case when c.flag_pinata = true then c.icon_pinata else c.icon end as icon \
                        from nft_buy ns \
                        inner join collections c on c.nft_contract = ns.collection \
                        where collection = $1 \
                        group by \
                            ns.collection, c.flag_pinata, c.icon_pinata, c.icon ";
        
        const resultados = await conexion.query(query, [collection]);
        const data = resultados.rows;
        conexion.end();
        res.json(data)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: "error", error: error})
    }
}





module.exports = { AlertPrice, AlertVolumen }





















