const { Pool } = require("pg")

const connectionData2 = {
  user: process.env.USER_DB2,
  host: process.env.HOST2,
  database: process.env.DATABASE2,
  password: process.env.PASSWORD_DB2,
  port: process.env.PORT2,
  idleTimeoutMillis: 1000, // close idle clients after 1 second
  connectionTimeoutMillis: 1000, // return an error after 1 second if connection could not be established
  allowExitOnIdle: true,
}

const pool = new Pool(connectionData2)


const saveWalletNft = async (req, res) => {

  const client = await pool.connect();
  try {
    const { wallet, addressSUI, discord, nfts } = req.body
    //console.log(req.body)
    await client.query('BEGIN')
    var query = "select add_wallet ('" + wallet + "','" + addressSUI + "','" + discord + "', ARRAY['" + nfts.join("','") +  "'])"
    console.log(query)
    await client.query(query)
    .then(() => {
          res.status(200).send()
     }).catch(() => {
          res.status(500).send()
     })
    await client.query('COMMIT') 
  } catch (error) {
      console.log('error 1: ', error)
      await client.query('ROLLBACK')
      res.status(500).json({respuesta: "Error"})
  } finally {
    await client.release();
  }
}

const getWalletsNfts = async (req, res) => {
  try {
      const client = await pool.connect();
      const response = await client.query("select id, wallet as NEAR_Wallet, address_sui as SUI_Wallet, discord as Discord_User,  nfts as nft_token_id \
                                          from wallets_nfts")

      res.json(response.rows)
      client.release();
  } catch (error) {
      res.status(404).json()
  }
}

module.exports = { saveWalletNft, getWalletsNfts }





















