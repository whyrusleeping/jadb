package main

import (
	"github.com/whyrusleeping/jadb"
	"fmt"
)

type Player struct {
	Name string
	Level int
}

//Identity function
func iPlayer(i interface{}) *Player {
	p := i.(*Player)
	return p
}

func (p *Player) GetID() string {
	return p.Name
}

func (p *Player) New() jadb.I {
	return new(Player)
}

func main() {
	db := jadb.MakeSomnDB("data")
	players := db.Collection("Players", &Player{})
	players.Save(&Player{"Joe",16})
	players.Save(&Player{"Steve",2})
	players.Save(&Player{"whyrusleeping",9001})
	players.Save(&Player{"SomeNoob",1})

	highlev := players.FindWhere(func (i jadb.I) bool {
		return iPlayer(i).Level > 10
	})

	for _,v := range highlev {
		fmt.Println(iPlayer(v).Name)
	}

	joe := players.FindByID("Joe")
	if joe != nil {
		fmt.Printf("Joes level is: %d\n", iPlayer(joe).Level)
	} else {
		fmt.Println("Could not find Joe...")
	}

	db.Close()
}
