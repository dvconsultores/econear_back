const { dbConnect2 } = require('../../config/postgres')

const axios = require('axios');

const saveWalletNft = async (req, res) => {
    try {
      const { wallet } = req.body
      var result

      if (wallet) {
          const conexion = await dbConnect2()
          const resultados = await conexion.query("select * \
                                                    from wallets_nfts where \
                                                    wallet = $1\
                                                    ", [wallet])

          if(resultados.rows.length === 0) {
            result = await conexion.query(`insert into wallets_nfts
                  (wallet)
                  values ($1)`, [wallet])
              .then(() => {
                  result = true
              }).catch(() => {
                  result = false
              })
            res.json({respuesta: "ok", data: result})
          }
      } else {
          res.status(404).json({respuesta: "wallet invalido"})
      }
    } catch (error) {
        console.log('error 1: ', error)
        res.json({respuesta: "error", error: error})
    }
}

module.exports = { saveWalletNft }





















