const { dbConnect, dbConnect2, dbConnect3 } = require('../../config/postgres')

const axios = require('axios');
const moment = require('moment');




const AlertPrice = async (req, res) => {
    try {
        const { collection } = req.body;

        const conexion = await dbConnect2();
        
        let query = "   select \
                            cast(precio as numeric) / 1000000000000000000000000 as price, \
                            collection \
                        from nft_marketplace nm \
                        where cast(precio as numeric) > 0 \
                        and collection = $1 \
                        order by \
                            cast(precio as numeric) / 1000000000000000000000000 asc \
                        limit 1 ";
        
        const resultados = await conexion.query(query, [collection]);
        const data = resultados.rows;
        
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
        
        let query = "   select collection, sum(pricenear) as volumen \
                        from nft_sellbuy ns \
                        where collection = $1 \
                        group by \
                            collection ";
        
        const resultados = await conexion.query(query, [collection]);
        const data = resultados.rows;
        
        res.json(data)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: "error", error: error})
    }
}





module.exports = { AlertPrice, AlertVolumen }





















