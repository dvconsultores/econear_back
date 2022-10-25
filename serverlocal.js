const express = require('express');
const morgan = require('morgan');
const app = express(),
      bodyParser = require("body-parser");
      port = 3072;

const cors = require('cors');

const { ApolloServer } = require('apollo-server-express')
const { typeDefs } = require('./app/graphql/TypeDefs')
const { resolvers } = require('./app/graphql/resolvers')



require('dotenv').config()

app.use(cors({
  origin: '*'
}));



app.use(bodyParser.json());
app.use(express.static(process.cwd() + '/my-app/dist'));
app.use(morgan('dev'))

app.use('/api/v1', require('./app/routes'))

//const { Collections } = require('./app/graphql/dataPostgresql')

async function startGraphql() {
  const apolloServer = new ApolloServer ({
    typeDefs,
    resolvers
  })

  await apolloServer.start()

  apolloServer.applyMiddleware({app})
}

startGraphql()

app.listen(port, () => {  
    console.log(`Server listening on the port::${port} http://localhost:${port}/graphql`);
});