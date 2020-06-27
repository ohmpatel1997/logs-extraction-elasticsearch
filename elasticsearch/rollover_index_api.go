package elasticsearch

import (
	"context"
	"fmt"
	"github.com/ohmpatel1997/logs-extraction-elasticsearch/common"
)

func RolloverIndexAPI() {
	client, err := common.GetClient()
	if err != nil {
		fmt.Printf("Could not able to get new client :%s", err.Error())
		return
	}
	ctx := context.Background()
	client.RolloverIndex(common.)
