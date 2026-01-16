package main

import (
	"log"

	srvConfig "github.com/CHESSComputing/golib/config"
	docdb "github.com/CHESSComputing/golib/docdb"
	server "github.com/CHESSComputing/golib/server"
	"github.com/CHESSComputing/golib/services"
	"github.com/gin-gonic/gin"
)

// Verbose defines verbosity level
var Verbose int

// global variables
var _foxdenUser services.UserAttributes

// metaDB object
var metaDB docdb.DocDB

// helper function to setup our router
func setupRouter() *gin.Engine {
	routes := []server.Route{
		{Method: "GET", Path: "/record", Handler: GetHandler, Authorized: false},
		{Method: "POST", Path: "/record", Handler: PostHandler, Authorized: true, Scope: "write"},
		{Method: "POST", Path: "/search", Handler: SearchHandler, Authorized: true},
		{Method: "POST", Path: "/count", Handler: CountHandler, Authorized: true},
		{Method: "DELETE", Path: "/record", Handler: DeleteHandler, Authorized: true, Scope: "delete"},
	}
	r := server.Router(routes, nil, "static", srvConfig.Config.UserMetaData.WebServer)
	return r
}

// Server defines our HTTP server
func Server() {
	var err error

	// init docdb
	metaDB, err = docdb.InitializeDocDB(srvConfig.Config.UserMetaData.MongoDB.DBUri)
	if err != nil {
		log.Fatal(err)
	}

	// init Verbose
	Verbose = srvConfig.Config.UserMetaData.WebServer.Verbose

	// make a choice of foxden user
	switch srvConfig.Config.UserMetaData.FoxdenUser.User {
	case "Maglab":
		_foxdenUser = &services.MaglabUser{}
	case "CHESS":
		_foxdenUser = &services.CHESSUser{}
	default:
		_foxdenUser = &services.CHESSUser{}
	}
	_foxdenUser.Init()

	// setup web router and start the service
	r := setupRouter()
	webServer := srvConfig.Config.UserMetaData.WebServer
	server.StartServer(r, webServer)
}
