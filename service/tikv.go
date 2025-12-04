package service
import (
	"context"
	"fmt"
	"log"
	// "net/http"
	// "errors"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
	"data-check-all/model"
	"strings"
	"time"
)
// Test function for TiKV CRUD operations
func TestTiKV(tikvConfigs []model.TiKVConfig) {
	ctx := context.Background()
	prefix := "devops" + ":"
	addPrefix :=
		func(key string) string {
			return prefix + key
			// return key
		}
	removePrefix :=
		func(key string) string {
			return strings.TrimPrefix(key, prefix)
			// return key
		}
	getClient := func(pdAddr string, TLS bool, ClusterSSLCA, ClusterSSLCert, ClusterSSLKey string) (*rawkv.Client, error) {
		log.Printf("Connecting to TiKV at %s (TLS: %v)", pdAddr, TLS)
		if pdAddr == "" {
			return nil, fmt.Errorf("URL environment variable not set")
		}
		var client *rawkv.Client
		var err error
		// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		// defer cancel()
		if TLS {
			client, err = rawkv.NewClient(ctx, []string{pdAddr}, config.Security{
				ClusterSSLCA:   ClusterSSLCA,
				ClusterSSLCert: ClusterSSLCert,
				ClusterSSLKey:  ClusterSSLKey,
			})
		} else {
			client, err = rawkv.NewClient(ctx, []string{pdAddr}, config.DefaultConfig().Security)
		}
		if err != nil {
			log.Printf("Failed to connect to TiKV: %v", err)
			return nil, fmt.Errorf("failed to connect to TiKV: %w", err)
		}
		return client, nil

	}

	for i, tikvConfig := range tikvConfigs {
		fmt.Printf("\n=== Testing TiKV %d: %s:%d ===\n", i+1, tikvConfig.Host, tikvConfig.Port)

		// Set PD address for testing
		pdAddr := fmt.Sprintf("%s:%d", tikvConfig.Host, tikvConfig.Port)
		prefix = tikvConfig.Prefix + ":"
		TLS := true // Enable TLS for testing if certificates are provided

		ClusterSSLCert := ""
		ClusterSSLKey := ""
		ClusterSSLCA := ""

		if tikvConfig.SSLClientCRT != "" && tikvConfig.SSLClientKey != "" && tikvConfig.SSLCACRT != "" {
			ClusterSSLCert = tikvConfig.SSLClientCRT
			ClusterSSLKey = tikvConfig.SSLClientKey
			ClusterSSLCA = tikvConfig.SSLCACRT
		}

		client, err := getClient(pdAddr, TLS, ClusterSSLCA, ClusterSSLCert, ClusterSSLKey)
		if err != nil {
			log.Printf("❌ Failed to create TiKV client: %v", err)
			continue
		}
		defer client.Close()

		// Test keys with prefix
		baseKey := "test_key"
		testKeys := []string{"1", "2", "3"}
		testValues := []string{
			"Initial value for test key 1",
			"Initial value for test key 2",
			"Initial value for test key 3",
		}

		// 1. CREATE (Insert) Keys
		fmt.Println("1. Creating test keys...")
		testItems := make([]model.TestKeyValue, len(testKeys))

		for j, key := range testKeys {
			fullKey := addPrefix(baseKey + "_" + key)
			value := testValues[j]

			err = client.Put(ctx, []byte(fullKey), []byte(value))
			if err != nil {
				log.Printf("❌ Insert key %s error: %v", fullKey, err)
				continue
			}

			testItems[j] = model.TestKeyValue{
				Key:   baseKey + "_" + key,
				Value: value,
				Ts:    time.Now(),
			}
			fmt.Printf("✓ Key %s inserted with value: %s\n", baseKey+"_"+key, value)
		}

		// 2. READ (Get) Keys
		fmt.Println("2. Reading test keys...")
		for _, item := range testItems {
			if item.Key == "" {
				continue // Skip if insert failed
			}

			fullKey := addPrefix(item.Key)
			valueBytes, err := client.Get(ctx, []byte(fullKey))
			if err != nil {
				log.Printf("❌ Get key %s error: %v", item.Key, err)
				continue
			}

			if valueBytes == nil {
				log.Printf("❌ Key %s not found", item.Key)
				continue
			}

			retrievedValue := string(valueBytes)
			fmt.Printf("✓ Key %s retrieved: %s\n", item.Key, retrievedValue)
		}

		// 3. UPDATE Key
		fmt.Println("3. Updating test key...")
		updateKey := baseKey + "_1"
		updatedValue := "Updated value for test key 1 - modified at " + time.Now().Format(time.RFC3339)

		fullUpdateKey := addPrefix(updateKey)
		err = client.Put(ctx, []byte(fullUpdateKey), []byte(updatedValue))
		if err != nil {
			log.Printf("❌ Update key %s error: %v", updateKey, err)
		} else {
			// Verify update
			updatedBytes, err := client.Get(ctx, []byte(fullUpdateKey))
			if err == nil && updatedBytes != nil {
				fmt.Printf("✓ Key %s updated to: %s\n", updateKey, string(updatedBytes))
			} else {
				log.Printf("❌ Verification failed for updated key %s", updateKey)
			}
		}

		// 4. SCAN (Range Query) Test Keys
		fmt.Println("4. Scanning test keys...")
		startKey := addPrefix(baseKey + "_")
		endKey := addPrefix(baseKey + "_z") // End key for scan range

		keys, values, err := client.Scan(ctx, []byte(startKey), []byte(endKey), 100)
		if err != nil {
			log.Printf("❌ Scan error: %v", err)
		} else {
			fmt.Printf("✓ Scan found %d keys in range\n", len(keys))
			for j, key := range keys {
				if j >= len(values) {
					break
				}
				cleanKey := removePrefix(string(key))
				fmt.Printf("  - %s: %s\n", cleanKey, string(values[j]))
			}
		}

		// 5. DELETE Key
		fmt.Println("5. Deleting test key...")
		deleteKey := baseKey + "_2"
		fullDeleteKey := addPrefix(deleteKey)

		// Verify exists before delete
		existsValue, err := client.Get(ctx, []byte(fullDeleteKey))
		if err != nil || existsValue == nil {
			log.Printf("❌ Key %s not found for deletion", deleteKey)
		} else {
			err = client.Delete(ctx, []byte(fullDeleteKey))
			if err != nil {
				log.Printf("❌ Delete key %s error: %v", deleteKey, err)
			} else {
				// Verify deletion
				afterDelete, _ := client.Get(ctx, []byte(fullDeleteKey))
				if afterDelete == nil {
					fmt.Printf("✓ Key %s deleted successfully\n", deleteKey)
				} else {
					log.Printf("❌ Key %s still exists after deletion", deleteKey)
				}
			}
		}

		// 6. CLEANUP - Delete remaining test keys
		fmt.Println("6. Cleaning up test keys...")
		for _, item := range testItems {
			if item.Key == "" {
				continue
			}
			// Skip the key we're keeping for final verification
			if strings.Contains(item.Key, "2") {
				continue
			}

			fullCleanupKey := addPrefix(item.Key)
			err = client.Delete(ctx, []byte(fullCleanupKey))
			if err != nil {
				log.Printf("⚠️ Cleanup warning for %s: %v", item.Key, err)
			}
		}

		// Final verification - check one key still exists
		finalCheckKey := baseKey + "_3"
		checkValue, err := client.Get(ctx, []byte(addPrefix(finalCheckKey)))
		if err == nil && checkValue != nil {
			fmt.Printf("✓ Final verification: key %s still exists with value %s\n", finalCheckKey, string(checkValue))
		}

		fmt.Printf("✅ TiKV %d test completed\n", i+1)
	}

}
