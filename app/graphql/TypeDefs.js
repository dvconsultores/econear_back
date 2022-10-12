const { gql } = require('apollo-server-express')


const typeDefs = gql`
  type collections {
    nft_contract: String!
    name: String!
    icon: String!
  }
  
  type Query{
    books: [Book]
    getAllCollections: [collections]
  }

  type Book{
    title: String!,
    description: String!
    authorName: String!
  }
`


module.exports = { typeDefs }
