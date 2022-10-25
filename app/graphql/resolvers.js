const { NftCollection, DeleteMarket } = require('./dataPostgresql')
const { pru } = require('./dataThegraph')

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

async function cargar(collection) {
  try {
    const nftmarket = await pru(collection)

    const tokens_deleteMarket = nftmarket.map(item => {return item.tokenid })
    const deleteMarket = await DeleteMarket(collection, tokens_deleteMarket)
    
    const nftmarketfinal = nftmarket.map(item => {
      if(!deleteMarket.includes(item2 => item2.token_id == item.tokenid && item2.fecha > item.fecha)) {
        return item
      }      
    })

    const tokens = nftmarketfinal.map(item => {return item.tokenid})
    const nftcolecion = await NftCollection(collection, tokens)

    const array = []
    const array2 = []
    for(let i = 0; i < nftcolecion.length; i++){
      const result = nftmarketfinal.filter(item => item.tokenid == nftcolecion[i].token_id)
      result.forEach(item => {
        array.push({
          collection: nftcolecion[i].collection,
          collection_name: nftcolecion[i].name_collection,
          collection_icon: nftcolecion[i].icon_collection,
          token_id: nftcolecion[i].token_id,
          titulo: nftcolecion[i].titulo,
          price: item.price ? item.price : '',
          price_near: item.pricenear ? item.pricenear : 0,
        })
      });
    }
    console.log(array2)
    /*console.log('paso')
    console.log(colecion)

    const array = await nftmarket.map(async item => {
      const re = await colecion.filter(item => item.nft_contract == item.collection)
      const res = re.length > 0 ? re[0] : {}

      return {
        collection: item.collection,
        collection_name: item.name_collection,
        collection_icon: item.icon_collection,
        token_id: item.token_id,
        titulo: item.titulo,
        price: res.price ? res.price : '',
        price_near: res.pricenear ? res.pricenear : 0,
      }
    }) */
    
    /*const nft_market = {}
    nftmarket.forEach(m => nft_market[m.idMarca] = m.nombre )


    // Luego construye el array aplicando una transformación a 
    // cada uno de los elementos de auto
    const autosConNombresMarcas = auto.map(aut => {
        const nuevo = {
            ...aut, // Todo lo que tenía el auto
            nombreMarca: nft_market[aut.idMarca]
        }
        // Borrar idMarca en el nuevo.
        delete nuevo.idMarca
        return nuevo
    })*/


    console.log(array)
    return array
  } catch(err) {
    console.log('eror: ', err)
    return []
  }

}


const resolvers = {
    Query:{
      books: () => Books,
      getNftCollection: async (parent, args) => {
        const { collection } = args
        const collections = await cargar(collection)
        return collections
      }
    },
    /*collections: {
      id: (root) => root.collection
    }*/
  }

module.exports = { resolvers }