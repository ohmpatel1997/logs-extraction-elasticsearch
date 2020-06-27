package elasticsearch

import (
	"context"
	"fmt"
	"github.com/ohmpatel1997/logs-extraction-elasticsearch/common"

	"time"
)

//can be done better dynamically but its fine for now
const (
	MAPPINGS = ` {
		"settings" : {
		    "index" : {
			  "sort.field" : "created_on",
				  "sort.order" : "desc"
			  },
			  "number_of_shards": 14,
			  "number_of_replicas": 1
		},
		"mappings" : {
		    "properties": {
			  "created_on": {
				"type": "date"
			  },
				  "message":{
					  "type":"text"
				  },
				  "line_no":{
					  "type":"integer"
				  }
		    }
		},
		"aliases": {
			  "logs_write": {}
		}
	}`
)

func CreateIndex() {

	currTime := time.Now().Format("2006-01-02")
	client, err := common.GetClient()
	if err != nil {
		fmt.Printf("Could not able to get new client :%s", err.Error())
		return
	}
	ctx := context.Background()
	resp, err := client.CreateIndex(fmt.Sprintf("logs-%s-000001", currTime)).Body(MAPPINGS).Do(ctx)
	if err != nil {
		fmt.Printf("Could not ablen to create index :%s", err.Error())
		return
	}
	fmt.Printf("Successfully created %v", resp)
}
