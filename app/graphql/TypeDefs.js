const { gql } = require('apollo-server-express')


const typeDefs = gql`

  type collectionss {
    collection: String!
    collection_name: String!
    collection_icon: String!
    token_id: String!
    titulo: String!
    price: String!
    price_near: Float!
  }

  type Query{
    books: [Book]
    getNftCollection(collection: String!): [collectionss]
  }

  type Book{
    title: String!,
    description: String!
    authorName: String!
  }
`


module.exports = { typeDefs }
