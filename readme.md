#JADB (Just Another DataBase)
JADB is a pure Go database for storing objects by key. The database is made up of collections of certain types of objects. Each collection exists in its own directory and disk writes are synchronized in a separate goroutine. My goal is to have a database made FOR Go, so that it is very easy to use.

A certain level of generality is achieved by using an interface with a 'New' method.
This allows the database to allocate the correct structs in the form of interfaces
for unmarshalling data into.

Example Usage:

    //create the database object
	db,err := jadb.NewJadb("data")
	
	//Appropriate error checking
	if err != nil {
		panic(err)
	}

	//Create a collection of 'Players'
	//The second argument is the template type
	players := db.Collection("Players", new(Player))

	players.Save(GetSomePlayer()) //Repeat as needed

	mypl := players.FindByID("Joe").(*Player)
