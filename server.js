const fs = require('fs');
const https = require('https');
const express = require('express');
const morgan = require('morgan');
const app = express(),
      bodyParser = require("body-parser");

// Certificate
const privateKey = fs.readFileSync('/etc/letsencrypt/live/econear.in/privkey.pem', 'utf8');
const certificate = fs.readFileSync('/etc/letsencrypt/live/econear.in/cert.pem', 'utf8');
const ca = fs.readFileSync('/etc/letsencrypt/live/econear.in/chain.pem', 'utf8');

const credentials = {
	key: privateKey,
	cert: certificate,
	ca: ca
};

const cors = require('cors');

const { ApolloServer } = require('apollo-server-express')
const { typeDefs } = require('./app/graphql/TypeDefs')
const { resolvers } = require('./app/graphql/resolvers')

require('dotenv').config()

app.use(cors({
  origin: '*'
}));


// Starting both http & https servers
const httpsServer = https.createServer(credentials, app);



app.use(bodyParser.json());
app.use(express.static(process.cwd() + '/my-app/dist'));
app.use(morgan('dev'))

app.use('/api/v1', require('./app/routes'))

async function startGraphql() {
	const apolloServer = new ApolloServer ({
		typeDefs,
		resolvers
	})

	await apolloServer.start()

	apolloServer.applyMiddleware({app})
}

startGraphql()


httpsServer.listen(3070, () => {
	console.log('HTTPS Server running on port 3070');
});

