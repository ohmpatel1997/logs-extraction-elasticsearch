package elasticsearch

import (
	"context"
	"fmt"
	"github.com/ohmpatel1997/logs-extraction-elasticsearch/common"
)

const (
	NAME = "timestamp"
)

func AddInjestPipeline() (err error) {

	client, err := common.GetClient()
	if err != nil {
		fmt.Printf("Could not able to get new client :%s", err.Error())
		return
	}
	settings := `{
		"index.default_pipeline": "timestamp"
	    }`
	ctx := context.Background()

	_, err = client.IngestPutPipeline(NAME).BodyString(settings).Do(ctx)

	if err != nil {
		fmt.Printf("Could not able to injest pipleine %s into cluster: %s", NAME, err.Error())
	}
	fmt.Printf("Successfully injested pipeline %s into cluster ", NAME)
	return nil
}
