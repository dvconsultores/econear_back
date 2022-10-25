const gql = require('graphql-tag');
const ApolloClient = require('apollo-client').ApolloClient;
const fetch = require('node-fetch');
const createHttpLink = require('apollo-link-http').createHttpLink;
const InMemoryCache = require('apollo-cache-inmemory').InMemoryCache;



const client = new ApolloClient({
    link: createHttpLink({
        uri: "https://api.thegraph.com/subgraphs/name/hrpalencia/pruebas2",//"https://api.thegraph.com/subgraphs/name/hrpalencia/pruebas2",
        fetch: fetch
    }),
    cache: new InMemoryCache()
});


const pru = async (collection) => {
    const query = gql`
        query MyQuery($collection: String!) {
            nftmarkets(first: 30, skip: 2, where: {collection: $collection, pricenear_not: 0, pricenear_gt: 109}) {
                id
                collection
                tokenid
                market
                price
                pricenear
                fecha
            }
        }
    `;

    try {
        const result = await client.query({
            query,
            variables: {
                collection
            }
        })
        return result.data.nftmarkets
    } catch(error) {
        return []
    }
}

const colsutaGraph = async (query, variables) => {
    try {
        if(!variables) {
            console.log('paso por no variable')
            const result = await client.query({
                query
            })
            return result.data
        } else {
            const result = await client.query({
                query,
                variables
            })
            //console.log(result.data.nftmarkets)
            return result.data.nftmarkets
        }
    } catch(error) {
        //console.log('----------------------------------------------------------------------------------------')
        //console.log(error)
        //console.log('----------------------------------------------------------------------------------------')
        return []
    }
}

const colsutaTheGraph = async (query, variables) => {
    try {
        if(!variables) {
            console.log('paso por no variable')
            const result = await client.query({
                query
            })
            return result.data
        } else {
            const result = await client.query({
                query,
                variables
            })
            //console.log(result.data.nftmarkets)
            return result.data
        }
    } catch(error) {
        //console.log('----------------------------------------------------------------------------------------')
        //console.log(error)
        //console.log('----------------------------------------------------------------------------------------')
        return []
    }
}

module.exports = { pru, colsutaGraph, colsutaTheGraph }