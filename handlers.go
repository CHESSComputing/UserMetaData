package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	srvConfig "github.com/CHESSComputing/golib/config"
	ql "github.com/CHESSComputing/golib/ql"
	"github.com/CHESSComputing/golib/server"
	services "github.com/CHESSComputing/golib/services"
	"github.com/gin-gonic/gin"
)

// MetaParams represents /record?did=bla end-point
type MetaParams struct {
	DID string `form:"did"`
}

// GetHandler handles queries via GET requests
func GetHandler(c *gin.Context) {
	var params MetaParams
	err := c.Bind(&params)
	if err != nil {
		rec := services.Response("MetaData", http.StatusBadRequest, services.BindError, err)
		c.JSON(http.StatusBadRequest, rec)
		return
	}
	var records []map[string]any
	spec := map[string]any{"did": params.DID}
	records = metaDB.Get(
		srvConfig.Config.UserMetaData.DBName,
		srvConfig.Config.UserMetaData.DBColl,
		spec, 0, -1)
	if Verbose > 0 {
		log.Println("RecordHandler", spec, records)
	}
	c.JSON(http.StatusOK, records)
}

// helper function to exgract JSON dict from HTTP request
func parseRequest(c *gin.Context) (map[string]any, error) {
	var spec map[string]any
	defer c.Request.Body.Close()
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return spec, err
	}
	err = json.Unmarshal(body, &spec)
	if err != nil {
		return spec, err
	}
	return spec, nil
}

// helper function to parse incoming HTTP request into ServiceRequest structure
func parseQueryRequest(c *gin.Context) (services.ServiceRequest, error) {
	var rec services.ServiceRequest
	defer c.Request.Body.Close()
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return rec, err
	}
	err = json.Unmarshal(body, &rec)
	if err != nil {
		log.Printf("ERROR: unable to unmarshal response body %s, error %v", string(body), err)
		return rec, err
	}
	if Verbose > 0 {
		log.Printf("QueryHandler received request %+s", rec.String())
	}
	return rec, nil
}

// PostHandler handles POST upload of meta-data record
func PostHandler(c *gin.Context) {
	rec, err := parseRequest(c)
	if err != nil {
		log.Println("ERROR:", err)
		rec := services.Response("MetaData", http.StatusInternalServerError, services.ParseError, err)
		c.JSON(http.StatusInternalServerError, rec)
		return
	}

	// insert record to meta-data database
	did, err := insertData(rec)
	if err != nil {
		log.Println("ERROR:", err)
		rec := services.Response("MetaData", http.StatusInternalServerError, services.InsertError, err)
		c.JSON(http.StatusInternalServerError, rec)
		return
	}
	var records []map[string]any
	resp := services.Response("MetaData", http.StatusOK, services.OK, nil)
	r := make(map[string]any)
	r["did"] = did
	records = append(records, r)
	resp.Results = services.ServiceResults{NRecords: 1, Records: records}
	c.JSON(http.StatusOK, resp)
}

// SearchHandler handles POST queries
func SearchHandler(c *gin.Context) {

	rec, err := parseQueryRequest(c)
	if err != nil {
		log.Println("ERROR:", err)
		rec := services.Response("MetaData", http.StatusInternalServerError, services.ParseError, err)
		c.JSON(http.StatusInternalServerError, rec)
		return
	}

	// get all attributes we need
	query := rec.ServiceQuery.Query
	idx := rec.ServiceQuery.Idx
	limit := rec.ServiceQuery.Limit
	sortOrder := rec.ServiceQuery.SortOrder
	sortKeys := rec.ServiceQuery.SortKeys

	spec := rec.ServiceQuery.Spec
	if spec != nil {
		if Verbose > 0 {
			log.Printf("use rec.ServiceQuery.Spec=%+v", spec)
		}
	} else {
		spec, err = ql.ParseQuery(query)
		if Verbose > 0 {
			log.Printf("search query='%s' spec=%+v", query, spec)
		}
		if err != nil {
			log.Println("ERROR:", err)
			rec := services.Response("MetaData", http.StatusInternalServerError, services.ParseError, err)
			c.JSON(http.StatusInternalServerError, rec)
			return
		}
	}

	var records []map[string]any
	nrecords := 0
	if spec != nil {
		nrecords = metaDB.Count(srvConfig.Config.UserMetaData.DBName, srvConfig.Config.UserMetaData.DBColl, spec)
		if len(sortKeys) > 0 {
			records = metaDB.GetSorted(
				srvConfig.Config.UserMetaData.DBName,
				srvConfig.Config.UserMetaData.DBColl,
				spec, sortKeys, sortOrder, idx, limit)
		} else {
			records = metaDB.Get(
				srvConfig.Config.UserMetaData.DBName,
				srvConfig.Config.UserMetaData.DBColl,
				spec, idx, limit)
		}
	}
	if Verbose > 0 {
		log.Printf("spec %v sortedKeys %v nrecords %d return idx=%d limit=%d", spec, sortKeys, nrecords, idx, limit)
	}
	c.JSON(http.StatusOK, records)
}

// CountHandler handles POST queries
func CountHandler(c *gin.Context) {

	rec, err := parseQueryRequest(c)
	if err != nil {
		log.Println("ERROR:", err)
		rec := services.Response("MetaData", http.StatusInternalServerError, services.ParseError, err)
		c.JSON(http.StatusInternalServerError, rec)
		return
	}

	// get all attributes we need
	query := rec.ServiceQuery.Query
	idx := rec.ServiceQuery.Idx
	limit := rec.ServiceQuery.Limit

	spec := rec.ServiceQuery.Spec
	if spec != nil {
		if Verbose > 0 {
			log.Printf("use rec.ServiceQuery.Spec=%+v", spec)
		}
	} else {
		spec, err = ql.ParseQuery(query)
		if Verbose > 0 {
			log.Printf("search query='%s' spec=%+v", query, spec)
		}
		if err != nil {
			log.Println("ERROR:", err)
			rec := services.Response("MetaData", http.StatusInternalServerError, services.ParseError, err)
			c.JSON(http.StatusInternalServerError, rec)
			return
		}
	}

	nrecords := 0
	if spec != nil {
		nrecords = metaDB.Count(srvConfig.Config.UserMetaData.DBName, srvConfig.Config.UserMetaData.DBColl, spec)
	}
	if Verbose > 0 {
		log.Printf("spec %v nrecords %d return idx=%d limit=%d", spec, nrecords, idx, limit)
	}
	c.JSON(http.StatusOK, nrecords)
}

// DeleteHandler handles POST queries
func DeleteHandler(c *gin.Context) {
	did := c.Request.FormValue("did")
	_, _, err := server.GetAuthTokenUser(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no user found in token", "message": "no user found", "code": services.RemoveError})
		return
	}
	spec := make(map[string]any)
	spec["did"] = did
	err = metaDB.Remove(
		srvConfig.Config.UserMetaData.DBName,
		srvConfig.Config.UserMetaData.DBColl,
		spec)
	status := http.StatusOK
	srvCode := services.OK
	if err != nil {
		status = http.StatusBadRequest
		srvCode = services.RemoveError
	}
	rec := services.Response("MetaData", status, srvCode, err)
	c.JSON(status, rec)
}
