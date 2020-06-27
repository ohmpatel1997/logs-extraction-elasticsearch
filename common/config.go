package common

import (
	elastic "github.com/olivere/elastic"
	"time"
)

//GetClient return the new elasticsearch client with default con figuration
func GetClient() (client *elastic.Client, err error) {
	client, err = elastic.NewClient(
		elastic.SetSniff(true),
		elastic.SetURL("http://localhost:9200"),
		elastic.SetHealthcheckInterval(5*time.Second), // quit trying after 5 seconds
	)
	return
}
