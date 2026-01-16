package main

import (
	"errors"
	"fmt"

	srvConfig "github.com/CHESSComputing/golib/config"
)

func insertData(rec map[string]any) (string, error) {

	var did string
	if val, ok := rec["did"]; ok {
		did = fmt.Sprintf("%v", val)
	} else {
		msg := "provided metadata record does not contain did attribute"
		return "", errors.New(msg)
	}

	// insert record to metaDB
	err := metaDB.InsertRecord(
		srvConfig.Config.UserMetaData.MongoDB.DBName,
		srvConfig.Config.UserMetaData.MongoDB.DBColl,
		rec)

	return did, err
}
