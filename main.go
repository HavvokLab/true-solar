package main

import (
	"github.com/HavvokLab/true-solar/collector"
	"github.com/HavvokLab/true-solar/infra"
	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/repo"
)

func main() {
	serv := collector.NewHuawei2Collector(
		repo.NewSolarMockRepo(),
		repo.NewSiteRegionMappingRepo(infra.GormDB),
	)

	serv.Execute(&model.HuaweiCredential{
		Username: "True_API",
		Password: "true2040",
		Owner:    "TRUE",
	})
}
