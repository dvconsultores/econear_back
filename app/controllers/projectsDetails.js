const { dbConnect, dbConnect2, dbConnect3 } = require('../../config/postgres')

const axios = require('axios');
const moment = require('moment');



const ProjectsDetailsHeader = async (req, res) => {
    try {
        const { collection } = req.body;

        const conexion = await dbConnect2();
        
        const fecha24h = moment().subtract(24, 'h').valueOf()*1000000;
        const fecha48h = moment().subtract(48, 'h').valueOf()*1000000;

        let query = "   select  \
        (select coalesce(json_agg(jsonlist), '[]') from (  \
            select  \
                (select cast(total_supply as numeric) from collections c where nft_contract = sub.collection) * sub.floor_price as market_cap, \
                coalesce(market_cap48h, 0) as market_cap48h,  \
                coalesce(((((select cast(total_supply as numeric) from collections c where nft_contract = sub.collection) * sub.floor_price) / market_cap48h) * 100) - 100, 0) as porcentaje  \
            from (  \
                    select  \
                        hc.market_cap as market_cap48h \
                    from history_collections hc  \
                    where hc.collection = sub.collection \
                    and to_char(to_timestamp(hc.fecha::numeric/1000000000), 'yyyy-mm-dd hh:00:00')::timestamp = to_char(to_timestamp($3::numeric/1000000000), 'yyyy-mm-dd hh:00:00')::timestamp  \
                    order by \
                        hc.fecha desc \
                    limit 1 \
                ) market_c  \
        ) jsonlist  \
        ) as JsonMarket_cap, \
        (select coalesce(json_agg(jsonlist), '[]') from (  \
            select  \
                sub.floor_price, \
                coalesce(floor_price48h, 0) as floor_price48h,  \
                coalesce(((sub.floor_price / floor_price48h) * 100) - 100, 0) as porcentaje  \
            from (  \
                    select  \
                        floor_price as floor_price48h \
                    from history_collections hc  \
                    where hc.collection = sub.collection \
                    and to_char(to_timestamp(hc.fecha::numeric/1000000000), 'yyyy-mm-dd hh:00:00')::timestamp = to_char(to_timestamp($3::numeric/1000000000), 'yyyy-mm-dd hh:00:00')::timestamp  \
                    order by \
                        hc.fecha desc \
                    limit 1 \
                ) floor_p  \
        ) jsonlist  \
        ) as JsonFloor_price, \
        (select coalesce(json_agg(jsonlist), '[]') from (  \
            select  \
                volumen24h,  \
                coalesce(volumen48h, 0) as volumen48h,  \
                coalesce(((volumen24h / volumen48h) * 100) - 100, 0) as porcentaje  \
            from (  \
                    select  \
                        (SELECT \
                            sum(x.pricenear) as volumen  \
                        FROM nft_buy x  \
                        where  \
                            x.collection = sub.collection  \
                            and x.fecha >= $2  \
                        ) as volumen24h,  \
                        (SELECT  \
                                sum(x.pricenear) as volumen48h  \
                            FROM nft_buy x  \
                            where  \
                                x.collection  = sub.collection  \
                                and x.fecha > $3  \
                                and x.fecha < $2   \
                        ) as volumen48h  \
                ) vol  \
        ) jsonlist  \
        ) as JsonVolumen24h,  \
        (select count(owner_id) as owner_id from (  \
            select token_new_owner_account_id as owner_id from nft_collections nc \
            where collection = sub.collection group by token_new_owner_account_id) sub ) as holders  \
        from (select min(precio_near) as floor_price, collection from nft_marketplace nm where collection = $1 and precio_near <> 0 group by collection) sub \
        ";
        
        const resultados = await conexion.query(query, [collection, fecha24h, fecha48h]);
        const data = resultados.rows;
        
        res.json(data)
        conexion.end();
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: "error", error: error})
    }
}


const StastMarketCapVolumenCollection = async (req, res) => {
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
                                coalesce(volumen.volumen, 0) as volumen, \
                                coalesce(( \
                                	select \
	                                    hc.market_cap  \
	                                from history_collections hc \
	                                where collection = $1  \
	                                and to_timestamp(hc.fecha::numeric/1000000000)::DATE = c.fecha  \
	                                order by \
	                                	fecha desc \
	                                limit 1 \
                                ), 0) as market_cap \
                            from ( \
                                select i::date as fecha  \
                                from generate_series(to_timestamp($2::numeric/1000000000)::DATE, \
                                to_timestamp($3::numeric/1000000000)::DATE, '1 day'::interval) i \
                            ) c \
                            left join ( \
                                select \
                                    to_timestamp(fecha::numeric/1000000000)::DATE as fecha, \
                                    sum(pricenear) as volumen \
                                from nft_buy nb \
                                where collection = $1 \
                                and fecha >= $2 \
                                and pricenear > 0 \
                                group by \
                                    to_timestamp(fecha::numeric/1000000000)::DATE \
                                order by \
                                    to_timestamp(fecha::numeric/1000000000)::DATE asc \
                            ) volumen on volumen.fecha = c.fecha \
                            order by \
                                c.fecha asc \
        ";
        
        let queryHora = "   select \
                                c.fecha, \
                                coalesce(volumen.volumen, 0) as volumen, \
                                coalesce(hc.market_cap, 0) as market_cap \
                            from ( \
                                select fecha from generate_series( \
                            to_char(to_timestamp($2::numeric/1000000000), 'yyyy-mm-dd hh:00:00')::timestamp, \
                            to_char(to_timestamp($3::numeric/1000000000), 'yyyy-mm-dd hh:00:00')::timestamp, \
                            '1 hour') as fecha \
                            ) c \
                            left join ( \
                                select \
                                    to_char(to_timestamp(fecha::numeric/1000000000), 'yyyy-mm-dd HH24:00:00')::timestamp  as fecha, \
                                    sum(pricenear) as volumen \
                                from nft_buy nb \
                                where collection = $1 \
                                and fecha >= $2 \
                                and pricenear > 0 \
                                group by \
                                    to_char(to_timestamp(fecha::numeric/1000000000), 'yyyy-mm-dd HH24:00:00')::timestamp \
                            ) volumen on volumen.fecha = c.fecha \
                            left join history_collections hc on hc.collection = $1 \
	                        and to_char(to_timestamp(hc.fecha::numeric/1000000000), 'yyyy-mm-dd HH24:00:00')::timestamp = c.fecha \
                            order by \
                                c.fecha asc \
        ";
        
        let queryFinal = value == 'h' ? queryHora : value == 'd' ? queryDia : ""
        const resultados = await conexion.query(queryFinal, [collection, fecha, fecha_hasta]);
        data = resultados.rows
        
        res.json(data)
        conexion.end();
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: "error", error: error})
    }
}


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
                                fecha, case when dataPrice is not null then split_part(dataPrice, '#', 1)::numeric else 0 end as floor_price, \
                                case when dataPrice is not null then split_part(dataPrice, '#', 2)::numeric else 0 end as average_price \
                            from ( \
                                select  \
                                    c.fecha,  \
                                    ( \
                                        select \
                                            hc.floor_price||'#'||hc.average_price  \
                                        from history_collections hc \
                                        where collection = $1  \
                                        and to_timestamp(hc.fecha::numeric/1000000000)::DATE = c.fecha  \
                                        order by \
                                            fecha desc \
                                        limit 1 \
                                    ) as dataPrice \
                                from (  \
                                    select i::date as fecha   \
                                    from generate_series(to_timestamp($2::numeric/1000000000)::date,  \
                                    to_timestamp($3::numeric/1000000000)::date, '1 day'::interval) i  \
                                ) c   \
                            ) sub \
                            order by  \
                                fecha asc  \
        ";
        
        let queryHora = "   select \
                                fechacalendario as fecha, coalesce(hc.floor_price, 0) as floor_price,  \
                                coalesce(hc.average_price, 0) as average_price \
                            from generate_series(  \
                                to_char(to_timestamp($2::numeric/1000000000), 'yyyy-mm-dd hh:00:00')::timestamp,  \
                                to_char(to_timestamp($3::numeric/1000000000), 'yyyy-mm-dd hh:00:00')::timestamp,  \
                                '1 hour') as fechacalendario  \
                            left join history_collections hc on hc.collection = $1  \
                            and to_char(to_timestamp(hc.fecha::numeric/1000000000), 'yyyy-mm-dd HH24:00:00')::timestamp = fechacalendario \
                            order by  \
                                fechacalendario  asc  \
        ";
        
        let queryFinal = value == 'h' ? queryHora : value == 'd' ? queryDia : ""
        const resultados = await conexion.query(queryFinal, [collection, fecha, fecha_hasta]);
        data = resultados.rows
        
        res.json(data)
        conexion.end();
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: "error", error: error})
    }
}

const StastSalesLiquidCollection = async (req, res) => {
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
                                coalesce(sub.sales, 0) as sales, \
                                coalesce(sub.liquidez, 0) as liquidez \
                            from ( \
                                select i::date as fecha  \
                                from generate_series(to_timestamp($2::numeric/1000000000)::DATE, \
                                to_timestamp($3::numeric/1000000000)::DATE, '1 day'::interval) i \
                            ) c \
                            left join ( \
                                select \
                                    to_timestamp(fecha::numeric/1000000000)::DATE as fecha, \
                                    count(pricenear) as sales, \
                                    sum(pricenear) as liquidez \
                                from nft_buy nb \
                                where collection = $1 \
                                and fecha >= $2 \
                                and pricenear > 0 \
                                group by \
                                    to_timestamp(fecha::numeric/1000000000)::DATE \
                                order by \
                                    to_timestamp(fecha::numeric/1000000000)::DATE asc \
                            ) sub on sub.fecha = c.fecha \
                            order by \
                                c.fecha asc \
        ";
        
        let queryHora = "   select \
                                c.fecha, \
                                coalesce(sub.sales, 0) as sales, \
                                coalesce(sub.liquidez, 0) as liquidez \
                            from ( \
                                select fecha from generate_series( \
                            to_char(to_timestamp($2::numeric/1000000000), 'yyyy-mm-dd hh:00:00')::timestamp, \
                            to_char(to_timestamp($3::numeric/1000000000), 'yyyy-mm-dd hh:00:00')::timestamp, \
                            '1 hour') as fecha \
                            ) c \
                            left join ( \
                                select \
                                    to_char(to_timestamp(ns.fecha::numeric/1000000000), 'yyyy-mm-dd HH24:00:00')::timestamp  as fecha, \
                                    count(pricenear) as sales, \
                                    sum(pricenear) as liquidez \
                                from nft_buy ns \
                                where ns.collection = $1 \
                                and ns.fecha >= $2 \
                                and ns.pricenear > 0 \
                                group by \
                                    to_char(to_timestamp(ns.fecha::numeric/1000000000), 'yyyy-mm-dd HH24:00:00')::timestamp, ns.seller \
                            ) sub on sub.fecha = c.fecha \
                            order by \
                                c.fecha asc \
        ";
        
        let queryFinal = value == 'h' ? queryHora : value == 'd' ? queryDia : ""
        const resultados = await conexion.query(queryFinal, [collection, fecha, fecha_hasta]);
        data = resultados.rows
        console.log(resultados.rows)
        conexion.end();
        res.json(data)
        
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: "error", error: error})
    }
}


const StastBuyersTradersCollection = async (req, res) => {
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
                                coalesce(sellers.sellers, 0) as sellers, \
                                coalesce(buyers.buyers, 0) as buyers \
                            from ( \
                                select i::date as fecha  \
                                from generate_series(to_timestamp($2::numeric/1000000000)::DATE, \
                                to_timestamp($3::numeric/1000000000)::DATE, '1 day'::interval) i \
                            ) c \
                            left join ( \
                                select fecha, count(seller) as sellers from ( \
                                    select \
                                        to_timestamp(fecha::numeric/1000000000)::DATE as fecha, \
                                        ns.seller \
                                    from nft_sell ns \
                                    where collection = $1 \
                                    and fecha >= $2 \
                                    and ns.pricenear > 0 \
                                    group by \
                                        to_timestamp(fecha::numeric/1000000000)::DATE, ns.seller \
                                    order by  \
                                        to_timestamp(fecha::numeric/1000000000)::DATE asc \
                                ) sub_sellers group by fecha \
                            ) sellers on sellers.fecha = c.fecha \
                            left join ( \
                                select fecha, count(buyer) as buyers from ( \
                                    select \
                                        to_timestamp(fecha::numeric/1000000000)::DATE as fecha, \
                                        ns.buyer \
                                    from nft_buy ns  \
                                    where collection = $1 \
                                    and fecha >= $2 \
                                    and ns.pricenear > 0 \
                                    group by  \
                                        to_timestamp(fecha::numeric/1000000000)::DATE, ns.buyer \
                                    order by \
                                        to_timestamp(fecha::numeric/1000000000)::DATE asc \
                                ) sub_buyers group by fecha \
                            ) buyers on buyers.fecha = c.fecha \
                            order by \
                                c.fecha asc \
        ";
        
        let queryHora = "   select \
                                c.fecha, \
                                coalesce(sellers.sellers, 0) as sellers, \
                                coalesce(buyers.buyers, 0) as buyers \
                            from ( \
                                select fecha from generate_series( \
                            to_char(to_timestamp($2::numeric/1000000000), 'yyyy-mm-dd hh:00:00')::timestamp, \
                            to_char(to_timestamp($3::numeric/1000000000), 'yyyy-mm-dd hh:00:00')::timestamp, \
                            '1 hour') as fecha \
                            ) c \
                            left join ( \
                                select fecha, count(seller) as sellers from ( \
                                    select \
                                        to_char(to_timestamp(ns.fecha::numeric/1000000000), 'yyyy-mm-dd HH24:00:00')::timestamp  as fecha, \
                                        ns.seller \
                                    from nft_sell ns \
                                    where ns.collection = $1 \
                                    and ns.fecha >= $2 \
                                    and ns.pricenear > 0 \
                                    group by \
                                        to_char(to_timestamp(ns.fecha::numeric/1000000000), 'yyyy-mm-dd HH24:00:00')::timestamp, ns.seller \
                                ) sub_sellers group by fecha \
                            ) sellers on sellers.fecha = c.fecha \
                            left join ( \
                                select fecha, count(buyer) as buyers from ( \
                                    select \
                                        to_char(to_timestamp(ns.fecha::numeric/1000000000), 'yyyy-mm-dd HH24:00:00')::timestamp  as fecha, \
                                        ns.buyer \
                                    from nft_buy ns \
                                    where ns.collection = $1 \
                                    and ns.fecha >= $2 \
                                    and ns.pricenear > 0 \
                                    group by \
                                        to_char(to_timestamp(ns.fecha::numeric/1000000000), 'yyyy-mm-dd HH24:00:00')::timestamp, ns.buyer \
                                ) sub_buyers group by fecha \
                            ) buyers on buyers.fecha = c.fecha \
                            order by \
                                c.fecha asc \
        ";
        
        let queryFinal = value == 'h' ? queryHora : value == 'd' ? queryDia : ""
        const resultados = await conexion.query(queryFinal, [collection, fecha, fecha_hasta]);
        data = resultados.rows
        
        res.json(data)
        conexion.end();
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: "error", error: error})
    }
}


const StastTopSalesCollection = async (req, res) => {
    try {
        const { collection, value, time, limit, index } = req.body;

        const fecha = moment().subtract(time, value).valueOf()*1000000;

        const conexion = await dbConnect2();
        
        let query = "   select \
                            nb.collection, nb.tokenid as token_id, nc.titulo, nc.descripcion, \
                            case when nc.flag_pinata = true then nc.media_pinata else nc.media end as media, \
                            buyer, seller, pricenear, nb.fecha \
                        from nft_buy nb \
                        left join nft_collections nc on nc.collection = nb.collection and nc.token_id = nb.tokenid \
                        where nb.collection = $1 \
                        and nb.fecha >= $2 \
                        group by \
                        	nb.collection, nb.tokenid, nc.titulo, nc.descripcion, \
                            case when nc.flag_pinata = true then nc.media_pinata else nc.media end, \
                            buyer, seller, pricenear, nb.fecha \
                        order by pricenear desc \
                        limit $3 offset $4 \
        ";
        
        
        const resultados = await conexion.query(query, [collection, fecha, limit, index]);
        
        
        res.json(resultados.rows)
        conexion.end();
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: "error", error: error})
    }
}

const StastSearchNftCollection = async (req, res) => {
    try {
        const { collection, search, limit, index } = req.body;

        const conexion = await dbConnect2();
        
        let query = "   select \
                            collection, token_id, titulo, descripcion, \
                            case when flag_pinata = true then media_pinata else media end as media, \
                            reference, extra \
                        from nft_collections nc \
                        where collection = $1 \
                        and (upper(titulo) like '%'||upper($2)||'%' or upper(token_id) like '%'||upper($2)||'%') \
                        order by token_id \
                        limit $3 offset $4 \
        ";
        
        const resultados = await conexion.query(query, [collection, search, limit, index]);
        
        
        res.json(resultados.rows)
        conexion.end();
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: "error", error: error})
    }
}

const StastNftCollection = async (req, res) => {
    try {
        const { collection, token_id, limit, index } = req.body;

        const conexion = await dbConnect2();
        
        let query = "   select \
                            nc.collection, nc.token_id, titulo, descripcion,  \
                            case when flag_pinata = true then media_pinata else media end as media,  \
                            reference, extra,  \
                            case when rarity.rarity_score is null then 0 else rarity.rarity_score end as rarity_score, \
                            case when rarity.ranking is null then 0 else rarity.ranking end as ranking, \
                            case when rarity.rareza is null then 'common' else rarity.rareza end as rareza, \
                            case when nm.precio_near is null then 0 else nm.precio_near end as precio_near \
                        from nft_collections nc  \
                        left join ( \
                            select \
                                collection, token_id, rarity_score, ranking, \
                                case  \
                                    when rarity_score > 0 and rarity_score < 35.9 then 'common' \
                                    when rarity_score > 35 and rarity_score < 37.9 then 'uncommon' \
                                    when rarity_score > 37 and rarity_score < 39.9 then 'rare' \
                                    when rarity_score > 39 and rarity_score < 40.9 then 'epic' \
                                    when rarity_score > 40 then 'legendary' \
                                end as rareza \
                            from ( \
                                select  \
                                    a2.collection, a2.token_id,  \
                                    avg(round(atr.porcentaje)) as rarity_score, \
                                    ROW_NUMBER () OVER (ORDER BY avg(round(atr.porcentaje)) desc) as ranking \
                                from atributos a2  \
                                inner join ( \
                                    select  \
                                        collection, trait_type, value,  \
                                        (count(value)::numeric / max(c.total_supply)::numeric) * 100 as porcentaje \
                                    from atributos a \
                                    inner join collections c on c.nft_contract = a.collection  \
                                    where collection = $1 \
                                    group by  \
                                        collection, trait_type, value \
                                ) atr on atr.collection = a2.collection and atr. trait_type = a2.trait_type and atr.value = a2.value \
                                group by \
                                    a2.collection, a2.token_id \
                                order by  \
                                    avg(round(atr.porcentaje)) asc \
                            ) rareza \
                        ) rarity on rarity.token_id = nc.token_id  \
                        left join nft_marketplace nm on nm.collection = nc.collection and nm.token_id = nc.token_id \
                        where nc.collection = $1  \
                        and ('%' = $2 or nc.token_id = $2)  \
                        order by nc.token_id  \
                        limit $3 offset $4 \
        ";
        
        let queryCount = "   select \
                                count(token_id) as total_nfts \
                            from nft_collections nc \
                            where collection = $1 \
        ";

        const resultados = await conexion.query(query, [collection, token_id, limit, index]);
        const resul = await conexion.query(queryCount, [collection]);
        
        res.json({total_nfts: resul.rows[0].total_nfts, data: resultados.rows})
        conexion.end();
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: "error", error: error})
    }
}


const StastTopBuyersCollection = async (req, res) => {
    try {
        const { collection, limit, index } = req.body;

        const conexion = await dbConnect2();

        const fecha = moment().subtract(24, 'h').valueOf()*1000000;
        
        let query = "   select \
                            buyer, \
                            count(tokenid) as bought, \
                            sum(pricenear) as volumen \
                        from nft_buy nb \
                        where collection = $1 \
                        and fecha >= $2 \
                        group by \
                            buyer \
                        order by \
                            sum(pricenear) desc \
                        limit $3 offset $4 \
        ";
        
        const resultados = await conexion.query(query, [collection, fecha, limit, index]);
        
        res.json(resultados.rows)
        conexion.end();
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: "error", error: error})
    }
}

const StastTopSellersCollection = async (req, res) => {
    try {
        const { collection, limit, index } = req.body;

        const conexion = await dbConnect2();

        const fecha = moment().subtract(24, 'h').valueOf()*1000000;
        
        let query = "   select \
                            seller, \
                            count(tokenid) as sold, \
                            sum(pricenear) as volumen \
                        from nft_sell nb \
                        where collection = $1 \
                        and fecha >= $2 \
                        group by \
                            seller \
                        order by \
                            sum(pricenear) desc \
                        limit $3 offset $4 \
        ";
        
        const resultados = await conexion.query(query, [collection, fecha, limit, index]);
        
        res.json(resultados.rows)
        conexion.end();
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: "error", error: error})
    }
}


const StastActivityCollection = async (req, res) => {
    try {
        const { collection, limit, index } = req.body;

        const conexion = await dbConnect2();
        
        let query = "   select \
                            sub.collection, sub.token_id, nc.titulo, nc.descripcion, \
                            case when nc.flag_pinata = true then nc.media_pinata else nc.media end as media, \
                            actions, price, owner_id, new_owner_id, sub.fecha \
                        from ( \
                            select  \
                                collection, tokenid as token_id, 'Buy' as actions, pricenear as price, \
                                seller as owner_id, seller as new_owner_id, fecha \
                            from nft_buy ns \
                            where collection = $1 \
                            union all \
                            select  \
                                collection, tokenid as token_id, 'Sale' as actions, pricenear as price, \
                                seller as owner_id, '' as new_owner_id, fecha \
                            from nft_sell ns \
                            where collection = $1 \
                            union all \
                            select \
                                collection, token_id, event_kind as actions, null as price, \
                                coalesce(token_old_owner_account_id, '') as owner_id, \
                                token_new_owner_account_id as new_owner_id, \
                                fecha \
                            from nft_collections_transaction nct \
                            where collection = $1 \
                        ) sub \
                        left join nft_collections nc on nc.collection = sub.collection and nc.token_id = sub.token_id  \
                        order by sub.fecha desc \
                        limit $2 offset $3 \
        ";
        
        
        const resultados = await conexion.query(query, [collection, limit, index]);
        
        
        res.json(resultados.rows)
        conexion.end();
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: "error", error: error})
    }
}

const StastRarityDistributionCollection = async (req, res) => {
    try {
        const { collection } = req.body;

        const conexion = await dbConnect2();
        
        let query = "   select  \
                            rareza, count(rareza) as cantidad \
                        from ( \
                            select  \
                                collection, token_id, rarity_score, \
                                case  \
                                    when rarity_score > 0 and rarity_score < 35.9 then 'common' \
                                    when rarity_score > 35 and rarity_score < 37.9 then 'uncommon' \
                                    when rarity_score > 37 and rarity_score < 39.9 then 'rare' \
                                    when rarity_score > 39 and rarity_score < 40.9 then 'epic' \
                                    when rarity_score > 40 then 'legendary' \
                                end as rareza, \
                                case  \
                                    when rarity_score > 0 and rarity_score < 35.9 then 1 \
                                    when rarity_score > 35 and rarity_score < 37.9 then 2 \
                                    when rarity_score > 37 and rarity_score < 39.9 then 3 \
                                    when rarity_score > 39 and rarity_score < 40.9 then 4 \
                                    when rarity_score > 40 then 5 \
                                end as OrderRareza \
                            from ( \
                                select  \
                                        a2.collection, a2.token_id,  \
                                        avg(round(atr.porcentaje)) as rarity_score, \
                                        ROW_NUMBER () OVER (ORDER BY avg(round(atr.porcentaje)) desc) as ranking \
                                    from atributos a2  \
                                    inner join ( \
                                        select  \
                                            collection, trait_type, value,  \
                                            (count(value)::numeric / max(c.total_supply)::numeric) * 100 as porcentaje \
                                        from atributos a \
                                        inner join collections c on c.nft_contract = a.collection  \
                                        where collection = $1 \
                                        group by  \
                                            collection, trait_type, value \
                                    ) atr on atr.collection = a2.collection and atr. trait_type = a2.trait_type and atr.value = a2.value \
                                    group by \
                                        a2.collection, a2.token_id \
                                    order by  \
                                        avg(round(atr.porcentaje)) desc	 \
                            ) rarity \
                        ) sub \
                        group by  \
                            rareza, OrderRareza \
                        order by \
                            OrderRareza \
        ";
        
        
        const resultados = await conexion.query(query, [collection]);
        
        
        res.json(resultados.rows)
        conexion.end();
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: "error", error: error})
    }
}


module.exports = { ProjectsDetailsHeader, StastMarketCapVolumenCollection, StastPriceCollection, StastBuyersTradersCollection, StastSalesLiquidCollection,
    StastTopSalesCollection, StastSearchNftCollection, StastNftCollection, StastTopBuyersCollection, StastTopSellersCollection, StastActivityCollection,
    StastRarityDistributionCollection }





















