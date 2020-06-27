package elasticsearch

import (
	"context"
	"fmt"
	"github.com/ohmpatel1997/logs-extraction-elasticsearch/common"
)

//RolloverIndexAPI roll over the index and credate new index based on considitions given
func RolloverIndexAPI() error {
	client, err := common.GetClient()
	if err != nil {
		fmt.Printf("Could not able to get new client :%s", err.Error())
		return err
	}
	ctx := context.Background()
	conditions := map[string]interface{}{
		"max_age":  "1d",
		"max_docs": "3",
	}
	resp, err := client.RolloverIndex("write_logs").Conditions(conditions).Do(ctx)

	if err != nil {
		fmt.Printf("\n Could not able to rollover index: %s", err.Error())
		return err
	}

	if resp.Acknowledged {
		fmt.Printf("\n Successfully Rolled Over from old index: %s to new index :%s", resp.OldIndex, resp.NewIndex)
	} else {
		fmt.Printf("\n Could not able to satisfy the roll over conditions:%v", resp.Conditions)
	}
	return nil
}
