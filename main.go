package main

import (
	"github.com/KumKeeHyun/godis/cmd"
	"log"
)

func main() {
	if err := cmd.New().Execute(); err != nil {
		log.Fatal(err)
	}
}
