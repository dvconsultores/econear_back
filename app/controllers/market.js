const { dbConnect, dbConnect2, dbConnect3 } = require('../../config/postgres')

const axios = require('axios');
const moment = require('moment');


const MarketplaceVolumen = async (req, res) => {
    try {
        const { market, value, time } = req.body;

        const conexion = await dbConnect2();
        
        const fecha = moment().subtract(value, time).valueOf()*1000000;
        const fecha_hasta = moment().valueOf()*1000000;
        console.log(fecha)
        console.log(fecha_hasta)
        let data = []
        
        let queryDia = "   select \
                                c.fecha,  \
                                coalesce(vol.volumen, 0) as volumen  \
                            from (  \
                                select i::date as fecha   \
                                from generate_series(to_timestamp($2::numeric/1000000000)::DATE,  \
                                to_timestamp($3::numeric/1000000000)::DATE, '1 day'::interval) i  \
                            ) c  \
                            left join (  \
                                select  \
                                    to_timestamp(fecha::numeric/1000000000)::DATE as fecha, \
                                    sum(pricenear) as volumen  \
                                from nft_buy nb  \
                                where market = $1 \
                                group by \
                                    to_timestamp(fecha::numeric/1000000000)::DATE \
                            ) vol on vol.fecha = c.fecha  \
                            order by  \
                                c.fecha asc  \
        ";
        
        let queryHora = "   select \
                                c.fecha,  \
                                coalesce(vol.volumen, 0) as volumen  \
                            from (  \
                                select fecha from generate_series(  \
                            to_char(to_timestamp($2::numeric/1000000000), 'yyyy-mm-dd hh:00:00')::timestamp,  \
                            to_char(to_timestamp($3::numeric/1000000000), 'yyyy-mm-dd hh:00:00')::timestamp,  \
                            '1 hour') as fecha  \
                            ) c  \
                            left join (  \
                                select  \
                                    to_char(to_timestamp(nb.fecha::numeric/1000000000), 'yyyy-mm-dd HH24:00:00')::timestamp  as fecha, \
                                    sum(pricenear) as volumen  \
                                from nft_buy nb  \
                                where market = $1 \
                                group by \
                                    to_char(to_timestamp(nb.fecha::numeric/1000000000), 'yyyy-mm-dd HH24:00:00')::timestamp  \
                            ) vol on vol.fecha = c.fecha   \
                            order by  \
                                c.fecha asc  \
        ";
        
        let queryFinal = value == 'h' ? queryHora : value == 'd' ? queryDia : ""
        console.log(market, fecha, fecha_hasta)
        const resultados = await conexion.query(queryFinal, [market, fecha, fecha_hasta]);
        data = resultados.rows
        
        res.json(data)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: "error", error: error})
    }
}




module.exports = { MarketplaceVolumen }





















