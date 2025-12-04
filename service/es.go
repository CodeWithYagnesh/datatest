package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"
	"io"

	// "net/http"
	// "errors"
	"data-check-all/model"
	"strings"
	"time"
	"log"
)

func TestES(ess []model.ESConfig) {
	ctx := context.Background()

	for i, esConfig := range ess {
		fmt.Printf("\n=== Testing Elasticsearch %d: %s:%d ===\n", i+1, esConfig.Host, esConfig.Port)

		// Create client
		addresses := []string{fmt.Sprintf("https://%s:%d", esConfig.Host, esConfig.Port)}
		cfg := elasticsearch.Config{
			Addresses: addresses,
			Username:  esConfig.Username,
			Password:  esConfig.Password,
		}

		client, err := elasticsearch.NewClient(cfg)
		if err != nil {
			log.Printf("❌ Failed to create client: %v", err)
			continue
		}

		indexName := "test-1"

		// 1. CREATE INDEX
		fmt.Println("1. Creating index...")
		createReq := esapi.IndicesCreateRequest{
			Index: indexName,
			Body:  strings.NewReader(`{"mappings":{"properties":{"name":{"type":"text"},"value":{"type":"integer"},"timestamp":{"type":"date"}}}}`),
		}
		createRes, err := createReq.Do(ctx, client)
		if err != nil {
			log.Printf("❌ Create index error: %v", err)
			continue
		}
		defer createRes.Body.Close()

		if createRes.IsError() {
			log.Printf("❌ Create index failed: %s", createRes.Status())
			continue
		}
		fmt.Println("✓ Index 'test-1' created successfully")

		// 2. CREATE (Insert) Documents
		fmt.Println("2. Inserting test documents...")
		testDocs := []model.TestDocument{
			{ID: "1", Name: "Document One", Value: 100, Ts: time.Now()},
			{ID: "2", Name: "Document Two", Value: 200, Ts: time.Now()},
			{ID: "3", Name: "Document Three", Value: 300, Ts: time.Now()},
		}

		for _, doc := range testDocs {
			docJSON, _ := json.Marshal(doc)
			createDocReq := esapi.IndexRequest{
				Index:      indexName,
				DocumentID: doc.ID,
				Body:       bytes.NewReader(docJSON),
				Refresh:    "true", // Make document immediately searchable
			}
			res, err := createDocReq.Do(ctx, client)
			if err != nil {
				log.Printf("❌ Insert document %s error: %v", doc.ID, err)
				continue
			}
			defer res.Body.Close()

			if res.IsError() {
				log.Printf("❌ Insert document %s failed: %s", doc.ID, res.Status())
			} else {
				fmt.Printf("✓ Document %s inserted\n", doc.ID)
			}
		}

		// 3. READ (Search) Documents
		fmt.Println("3. Reading documents...")
		searchReq := esapi.SearchRequest{
			Index: []string{indexName},
			Body:  strings.NewReader(`{"query":{"match_all":{}}}`),
			Size:  &[]int{10}[0],
		}
		searchRes, err := searchReq.Do(ctx, client)
		if err != nil {
			log.Printf("❌ Search error: %v", err)
		} else {
			defer searchRes.Body.Close()
			if searchRes.IsError() {
				log.Printf("❌ Search failed: %s", searchRes.Status())
			} else {
				body, _ := io.ReadAll(searchRes.Body)
				fmt.Printf("✓ Found %d documents: %s\n",
					len(testDocs), string(body[:min(100, len(body))])+"...")
			}
		}

		// 4. UPDATE Document
		fmt.Println("4. Updating document...")
		updatedDoc := model.TestDocument{ID: "1", Name: "Updated Document One", Value: 150, Ts: time.Now()}

		docJSON, err := json.Marshal(updatedDoc)
		if err != nil {
			log.Printf("❌ JSON marshal error for update: %v", err)
			continue
		}
		updateReq := esapi.IndexRequest{
			Index:      indexName,
			DocumentID: "1",
			Body:       bytes.NewReader(docJSON),
			Refresh:    "true",
		}
		updateRes, err := updateReq.Do(ctx, client)
		if err != nil {
			log.Printf("❌ Update error: %v", err)
		} else {
			defer updateRes.Body.Close()
			if updateRes.IsError() {
				log.Printf("❌ Update failed: %s", updateRes.Status())
			} else {
				fmt.Println("✓ Document 1 updated successfully")
			}
		}

		// 5. DELETE Document
		fmt.Println("5. Deleting document...")
		deleteReq := esapi.DeleteRequest{
			Index:      indexName,
			DocumentID: "2",
		}
		deleteRes, err := deleteReq.Do(ctx, client)
		if err != nil {
			log.Printf("❌ Delete error: %v", err)
		} else {
			defer deleteRes.Body.Close()
			if deleteRes.IsError() {
				log.Printf("❌ Delete failed: %s", deleteRes.Status())
			} else {
				fmt.Println("✓ Document 2 deleted successfully")
			}
		}

		// 6. DELETE INDEX (Cleanup)
		fmt.Println("6. Cleaning up - deleting index...")
		deleteIndexReq := esapi.IndicesDeleteRequest{
			Index: []string{indexName},
		}
		deleteIndexRes, err := deleteIndexReq.Do(ctx, client)
		if err != nil {
			log.Printf("❌ Delete index error: %v", err)
		} else {
			defer deleteIndexRes.Body.Close()
			if deleteIndexRes.IsError() {
				log.Printf("❌ Delete index failed: %s", deleteIndexRes.Status())
			} else {
				fmt.Println("✓ Index 'test-1' deleted successfully")
			}
		}

		fmt.Printf("✅ Elasticsearch %d test completed\n", i+1)
	}
}
