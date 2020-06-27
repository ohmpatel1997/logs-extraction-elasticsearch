package elasticsearch

import (
	"context"
	"fmt"
	"github.com/ohmpatel1997/logs-extraction-elasticsearch/common"
)

//RolloverIndexAPI roll over the index and credate new index based on considitions given
func RolloverIndexAPI() {
	client, err := common.GetClient()
	if err != nil {
		fmt.Printf("Could not able to get new client :%s", err.Error())
		return
	}
	ctx := context.Background()
	considitions := map[string]interface{}{
		"max_age":  "1d",
		"max_docs": "3",
	}
	resp, err := client.RolloverIndex("logs_write").Conditions(considitions).Do(ctx)

	if err != nil {
		fmt.Printf("Could not able to rollover index: %s", err.Error())
		return
	}
	fmt.Printf("Successfully Rolled Over from old index: %s to new index :%s", resp.OldIndex, resp.NewIndex)
}
