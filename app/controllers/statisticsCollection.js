const { dbConnect, dbConnect2, dbConnect3 } = require('../../config/postgres')

const axios = require('axios');
const moment = require('moment');


const StastPriceCollection = async (req, res) => {
    try {
        const { collection, value, time } = req.body;

        const conexion = await dbConnect2();
        
        const fecha = moment().subtract(time, value).valueOf()*1000000;
        const fecha_hasta = moment().valueOf()*1000000;
        console.log(fecha)
        console.log(fecha_hasta)
        let data = []
        
        let queryDia = "   select \
                                c.fecha, \
                                coalesce(sub.floor_price, 0) as floor_price, \
                                coalesce(sub.average_price, 0) as average_price \
                            from ( \
                                select i::date as fecha  \
                                from generate_series(to_timestamp($2::numeric/1000000000)::date, \
                                to_timestamp($3::numeric/1000000000)::date, '1 day'::interval) i \
                            ) c \
                            left join ( \
                                select \
                                    to_timestamp(fecha::numeric/1000000000)::DATE as fecha, \
                                    min(cast(precio as numeric)/1000000000000000000000000) as floor_price, \
                                    avg(cast(precio as numeric)/1000000000000000000000000) as average_price \
                                from nft_marketplace nm \
                                where collection = $1 \
                                and fecha >= $2 \
                                and cast(precio as numeric) > 0 \
                                group by \
                                    to_timestamp(fecha::numeric/1000000000)::DATE \
                                order by \
                                    to_timestamp(fecha::numeric/1000000000)::DATE asc \
                            ) sub on sub.fecha = c.fecha \
                            order by \
                                c.fecha asc ";
        
        let queryHora = "   select \
                                fechacalendario as fecha, \
                                ( \
                                    select \
                                        min(subfloor.precio) as floor_price \
                                    from ( \
                                        select \
                                            cast(precio as numeric)/1000000000000000000000000 as precio \
                                        from nft_marketplace nm \
                                        where collection = $1 \
                                        and fecha >= extract(epoch from to_char(fechacalendario, 'yyyy-mm-dd 00:00:00')::timestamp)*1000000000 \
                                        and fecha <= extract(epoch from to_char(fechacalendario, 'yyyy-mm-dd 23:59:59')::timestamp)*1000000000 \
                                        and precio <> '0' \
                                        union all \
                                        select \
                                            pricenear as precio \
                                        from nft_sellbuy ns \
                                        where ns.collection = $1 \
                                        and fecha >= extract(epoch from to_char(fechacalendario, 'yyyy-mm-dd 00:00:00')::timestamp)*1000000000 \
                                        and fecha <= extract(epoch from to_char(fechacalendario, 'yyyy-mm-dd 23:59:59')::timestamp)*1000000000 \
                                    ) subfloor \
                                ) as floo_price, \
                                ( \
                                    select \
                                        avg(subfloor.precio) as average_price \
                                    from ( \
                                        select \
                                            cast(precio as numeric)/1000000000000000000000000 as precio \
                                        from nft_marketplace nm \
                                        where collection = $1 \
                                        and fecha >= extract(epoch from to_char(fechacalendario, 'yyyy-mm-dd 00:00:00')::timestamp)*1000000000 \
                                        and fecha <= extract(epoch from to_char(fechacalendario, 'yyyy-mm-dd 23:59:59')::timestamp)*1000000000 \
                                        and precio <> '0' \
                                        union all \
                                        select \
                                            pricenear as precio \
                                        from nft_sellbuy ns \
                                        where ns.collection = $1 \
                                        and fecha >= extract(epoch from to_char(fechacalendario, 'yyyy-mm-dd 00:00:00')::timestamp)*1000000000 \
                                        and fecha <= extract(epoch from to_char(fechacalendario, 'yyyy-mm-dd 23:59:59')::timestamp)*1000000000 \
                                    ) subfloor \
                                ) as average_price \
                            from generate_series( \
                            to_char(to_timestamp($2::numeric/1000000000), 'yyyy-mm-dd hh:00:00')::timestamp, \
                            to_char(to_timestamp($3::numeric/1000000000), 'yyyy-mm-dd hh:00:00')::timestamp, \
                            '1 hour') as fechacalendario \
                            order by \
                                fecha asc ";
        
        let queryFinal = value == 'h' ? queryHora : value == 'd' ? queryDia : ""
        const resultados = await conexion.query(queryFinal, [collection, fecha, fecha_hasta]);
        data = resultados.rows
        
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





module.exports = { StastPriceCollection, AlertVolumen }





















