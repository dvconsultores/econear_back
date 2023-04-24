const { Pool } = require("pg")
// Coloca aquí tus credenciales

const dbConnect2 = async () => {
  try {
      const connectionData = {
        user: process.env.USER_DB2,
        host: process.env.HOST2,
        database: process.env.DATABASE2,
        password: process.env.PASSWORD_DB2,
        port: process.env.PORT2,
        idleTimeoutMillis: 1000, // close idle clients after 1 second
        connectionTimeoutMillis: 1000, // return an error after 1 second if connection could not be established
        allowExitOnIdle: true,
        min: 1,
        max: 3,
      }
      //const client = new Client(connectionData)
      //return client

      const pool = await new Pool(connectionData)
      return pool
  } catch (error) {
     return error
  }
  
}

module.exports = { dbConnect2 };