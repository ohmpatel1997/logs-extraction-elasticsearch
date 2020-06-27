package elasticsearch

import (
	"context"
	"fmt"
	"github.com/ohmpatel1997/logs-extraction-elasticsearch/common"
)

// InitializeReadTemplate adds index template with alias name `logs_search`
func InitializeReadTemplate() {

	client, err := common.GetClient()
	if err != nil {
		fmt.Printf("Could not able to get new client :%s", err.Error())
		return
	}
	ctx := context.Background()

	settings := `{
		"template": "logs-*",
		"settings": {
			"number_of_shards": 7,
			"number_of_replicas": 0,
			"codec": "best_compression"
		},
		"aliases": {
			"logs_search": {}
		}
	}`

	resp, err := client.IndexPutTemplate("search_logs").BodyString(settings).Do(ctx)

	if err != nil {
		fmt.Printf("Could not able to create read index template : %s", err.Error())
		return
	}
	fmt.Printf("Successfully creted read index template : %v", resp.Index)
}
