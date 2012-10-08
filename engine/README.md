# Structure

La fase di feeding consiste in un approccio a due vie. Prima un grafo delle amicizie sociali deve essere costruito e mando in pasto all'indexer.

In seguito l'indexer dovrebbe prendere in input uno stream di oggetti JSON contenente le seguenti informazioni:

 - screen_name
 - time (espressa come intero int32)
 - user_id (int64)
 - text (varlength string)
 - entities ()
 
Esempio JSON stream in ingresso:


È un semplice oggetto JSON come quelli ritornati da twitter con la sola differenza che ha come informazione supplementare l'attributo entities, che potrebbe venire costruito da un semplice classificatore.

# Nodes (Subjects)

Elenco nodi supportati:

 - users
 - tweets
 - entities

# Edges (Relationsips)

Elenco archi supportati:

 - users [:follows:unidirectional] users
 - users [:tweeted:bidirectional] tweets
 - users [:retweeted:bidirectional] tweets
 - users [:linked:bidirecitonal:(weight,)] entities
 
 - tweet
 
# Properties

Le proprietà sono indexate in un due file:

 - `com.social.indexer.property.idx`: contenente un indice delle proprietà
 - `com.social.indexer.property.store`: contenente le proprietà stesse

Esempio: se si volesse fare una query temporale per avere uno snapshot su tutti i tweet scritti in uno lasso di tempo:

	>>> db.query('users:date').between(a, b)
	    \__ query sul file .idx che estrae tutte le possibili posizioni dove le proprieta' sono storate
		   \_ filtra ogni proprieta' ed elimina le cose inutili ritornando le posizioni degli user da querare
	>>> db.query('users:date:screen_name:pisapia')

In dettaglio:

 - users:
   - followers:int32 (estraibile)
   - following:int32 (estraibile)
   - tweets:int32    (estraibile)
   - retweets:int32  (estraibile)
   - date:int32
 
 - tweets:
   - geolocation:str
   - retweets:int32 (estraibile)
   - date:int32
 
 - entities:
   - weight:int32
   - rho:int32
   

## Entities

Dovremmo avere un indice per le entità estratte da TagME. Le entities sono comunque prioprieta' di un

## Implementation

Classi:

	struct node {
		uint64 id;
		property* getProperty(char *); // -> property index
	}
	
	struct property { // qvariant
		uint64 type;
		char *name;
		void *value; // variable
	}
	
	struct relationship {
		char *lbl;
		node *src;
		node *dst;
	}
