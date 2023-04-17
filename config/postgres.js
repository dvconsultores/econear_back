const { Pool } = require("pg")
// Coloca aquí tus credenciales

const dbConnect = async () => {
  try {
      const connectionData = {
        user: process.env.USER_DB,
        host: process.env.HOST,
        database: process.env.DATABASE,
        password: process.env.PASSWORD_DB,
        port: process.env.PORT,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 2000,
        allowExitOnIdle: true,
        min: 10,
        max: 20,
      }
      //const client = new Client(connectionData)
      //return client

      const pool = await new Pool(connectionData)
      return pool
  } catch (error) {
     return error
  }
  
}

const dbConnect2 = async () => {
  try {
      const connectionData = {
        user: process.env.USER_DB2,
        host: process.env.HOST2,
        database: process.env.DATABASE2,
        password: process.env.PASSWORD_DB2,
        port: process.env.PORT2,
      }
      //const client = new Client(connectionData)
      //return client

      const pool = await new Pool(connectionData)
      return pool
  } catch (error) {
     return error
  }
  
}

const dbConnect3 = async () => {
  try {
      const connectionData = {
        user: process.env.USER_DB3,
        host: process.env.HOST3,
        database: process.env.DATABASE3,
        password: process.env.PASSWORD_DB3,
        port: process.env.PORT3,
      }
      //const client = new Client(connectionData)
      //return client

      const pool = await new Pool(connectionData)
      return pool
  } catch (error) {
     return error
  }
  
}


module.exports = { dbConnect, dbConnect2, dbConnect3 };