package elasticsearch

import (
	"context"
	"fmt"
	"github.com/ohmpatel1997/logs-extraction-elasticsearch/common"
	"github.com/olivere/elastic"
	"reflect"
)

func QueryLogs(startDate string, endDate string) error {

	client, err := common.GetClient()
	if err != nil {
		fmt.Printf("\n Could not able to get new client :%s", err.Error())
		return err
	}
	ctx := context.Background()
	query := fmt.Sprintf(`{
		"query": {
		    "range" : {
			  "created_on" : {
				"gte" : %s,
				"lte" : %s
			  }
		    }
		}
	  }`, startDate, endDate)
	resp, err := client.Search().Index("search_logs").Query(elastic.NewRawStringQuery(query)).Pretty(true).Do(ctx)

	if err != nil {
		fmt.Printf("\n Cannot able to search : %s \n", err.Error())
		return err
	}
	for result := range resp.Each(reflect.TypeOf([]interface{}{})) {
		fmt.Println("\n ", result)
	}
	return nil
}
