const { Collections } = require('./dataPostgresql')

const Books = [
    {
      title: 'El principito',
      description: 'El principito es una novela corta y la obra más famosa del escritor y aviador francés Antoine de Saint-Exupéry',
      authorName: 'Antoine de Saint-Exupéry'
    },
    {
      title: 'las aventuras de tom sawyer',
      description: 'Las aventuras de Tom Sawyer es una novela del autor estadounidense Mark Twain publicada en 1876',
      authorName: 'Mark Twain'
    }
  ]

const resolvers = {
    Query:{
      books: () => Books,
      getAllCollections: async () => {
        const collections = await Collections()
        return collections
      }
    },
    /*collections: {
      id: (root) => root.collection
    }*/
  }

module.exports = { resolvers }