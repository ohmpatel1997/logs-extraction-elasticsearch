package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ohmpatel1997/logs-extraction-elasticsearch/common"
	"github.com/olivere/elastic"
)

const (
	NAME = "timestamp"
)

func AddInjestPipeline() (err error) {

	client, err := common.GetClient()
	if err != nil {
		fmt.Printf("\n Could not able to get new client :%s", err.Error())
		return err
	}

	settings := `{
		"processors": [
		  {
		    "set": {
			"field": "_source.timestamp",
			"value": "{{_ingest.timestamp}}"
		    }
		  }
		],
		"description": "Adds timestamp to documents"
	    }`
	req := elastic.PerformRequestOptions{
		Method:      "PUT",
		Path:        fmt.Sprintf("http://localhost:9200/_ingest/pipeline/%s", NAME),
		ContentType: "encoding/json",
		Body:        string(settings),
	}
	ctx := context.Background()

	if _, err := client.PerformRequest(ctx, req); err != nil {
		fmt.Printf("\n Could not able to create the %s pipeline", NAME)
		return err
	}

	fmt.Printf("\n Successfully injested pipeline %s into cluster\n ", NAME)

	if err := MakePipelineDefault(ctx, client, NAME, "write_logs"); err != nil {
		fmt.Printf("\n Could not able to make the %s pipeline default\n", NAME)
		return err
	}
	return nil
}

func MakePipelineDefault(ctx context.Context, client *elastic.Client, name string, forIndex string) error {
	body := map[string]interface{}{
		"index.default_pipeline": name,
	}
	var jsonBody []byte
	var err error

	if jsonBody, err = json.Marshal(&body); err != nil {
		fmt.Printf(" \n Could not able marshal the body %v", body)
		return err
	}

	req := elastic.PerformRequestOptions{
		Method:      "PUT",
		Path:        fmt.Sprintf("http://localhost:9200/%s/_settings", forIndex),
		ContentType: "encoding/json",
		Body:        string(jsonBody),
	}

	if _, err := client.PerformRequest(ctx, req); err != nil {
		fmt.Printf("\n Could not able to set the %s pipeline as default", name)
		return err
	}

	fmt.Printf("\n Successfully set the injest pipleline %s as default", name)
	return nil
}
