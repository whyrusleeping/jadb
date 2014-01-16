#JADB (Just Another DataBase)
JADB is a pure Go database for storing objects by key. The database is made up of collections of certain types of objects. Each collection exists in its own directory and disk writes are synchronized in a separate goroutine. My goal is to have a database made FOR Go, so that it is very easy to use.

A certain level of generality is achieved by using an interface with a 'New' method.
This allows the database to allocate the correct structs in the form of interfaces
for unmarshalling data into.

Jadb is similar to SQLite in that its run with your program, not as a separate daemon, and is backed by flat files. 

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

	//Get Joe
	mypl := players.FindByID("Joe").(*Player)

	//Get all players higher than level 10
	highlevel := players.FindWhere(func (i jadb.I) bool {
		return i.(*Player).Level > 10
	})

	//Always close the database to ensure caches get written
	db.Close()

##Still to be done
- Profile everything (including memory)
- Find a real world application to test in
- Better error handling
