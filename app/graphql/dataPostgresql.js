const { dbConnect, dbConnect2, dbConnect3 } = require('../../config/postgres')

const Collections = async () => {
    try {
        const conexion = await dbConnect2()
        
        const resultados = await conexion.query("select nft_contract, name, coalesce(icon, '') as icon from collections")
                        
        return resultados.rows
    } catch (error) {
        return []
    }
}

module.exports = { Collections }