package main

import (
	"github.com/HavvokLab/true-solar/api/growatt"
	"github.com/HavvokLab/true-solar/pkg/util"
	"github.com/rs/zerolog/log"
)

func main() {
	client := growatt.NewGrowattClient("Trueupc1", "33bfe28df3f24cd42a8de64de0e7036e")
	plants, err := client.GetPlantDeviceList(2027820)
	if err != nil {
		log.Panic().Err(err).Msg("error get plant list")
	}

	util.PrintJSON(map[string]any{
		"plants": plants,
	})
}
