package service

import (
	"context"
	"crypto/tls"
	"data-check-all/model"
	"database/sql"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"log"
	"crypto/x509"
	"os"
	"strings"
	"time"
)

func TestClickHouse(chs []model.ClickHouseConfig) {
	ctx := context.Background()
	prefix := "devops" + ":"

	addPrefix := func(key string) string {
		return prefix + key
	}

	removePrefix := func(key string) string {
		return strings.TrimPrefix(key, prefix)
	}
	loadCertFromFile := func(filePath string) ([]byte, error) {
		if filePath == "" {
			return nil, errors.New("empty file path")
		}
		data, err := os.ReadFile(filePath) // Use os.ReadFile instead of ioutil
		if err != nil {
			return nil, fmt.Errorf("failed to read cert file %s: %w", filePath, err)
		}
		return data, nil
	}

	getTLSConfig := func(ch model.ClickHouseConfig) *tls.Config {
		if !ch.TLS {
			return nil
		}
		certPEM, err := loadCertFromFile(ch.SSLClientCRT)
		if err != nil {
			log.Fatalf("Failed to load client cert: %v", err)
		}
		keyPEM, err := loadCertFromFile(ch.SSLClientKey)
		if err != nil {
			log.Fatalf("Failed to load client key: %v", err)
		}
		caPEM, err := loadCertFromFile(ch.SSLCACRT)
		if err != nil {
			log.Fatalf("Failed to load CA: %v", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caPEM)

		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			log.Fatalf("Failed to parse client cert/key: %v", err)
		}

		tlsConfig := &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			ServerName:         ch.Host,     // Use host for SNI
			InsecureSkipVerify: true, // Set true for self-signed/internal certs
		}
		return tlsConfig
	}

	getClient := func(config model.ClickHouseConfig) (*sql.DB, error) {
		log.Printf("Connecting to ClickHouse at %s:%d (TLS: %v)", config.Host, config.Port, config.TLS)

		opts := &clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%d", config.Host, config.Port)},
			Auth: clickhouse.Auth{
				Database: config.Database,
				Username: config.Username,
				Password: config.Password,
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name, Version string
				}{
					{Name: "clickhouse-test", Version: "1.0"},
				},
			},
			DialTimeout:      time.Second * 10,
			ConnOpenStrategy: clickhouse.ConnOpenInOrder,
			Settings: clickhouse.Settings{
				"max_execution_time": 60,
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			ReadTimeout: time.Second * 300,
		}

		if config.TLS {
			// Simple TLS config - in production use proper certificates
			opts.TLS = getTLSConfig(config)

			// If certificates provided, use them (simplified)
			if config.SSLCACRT != "" {
				// Add certificate parsing here if needed
				log.Println("TLS with certificates enabled")
			}
		}

		db := clickhouse.OpenDB(opts)

		// Configure connection pool
		db.SetMaxIdleConns(5)
		db.SetMaxOpenConns(10)
		db.SetConnMaxLifetime(time.Hour)

		// Test connection
		var version string
		err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("connection test failed: %w", err)
		}

		log.Printf("✓ Connected to ClickHouse %s", version)
		return db, nil
	}

	for i, chConfig := range chs {
		fmt.Printf("\n=== Testing ClickHouse %d: %s:%d ===\n", i+1, chConfig.Host, chConfig.Port)

		// Set configuration for testing
		prefix = chConfig.Database + ":"
		tableName := chConfig.DBTableName
		if tableName == "" {
			tableName = chConfig.LocalTableName
			if tableName == "" {
				tableName = "main_dist"
			}
		}

		db, err := getClient(chConfig)
		if err != nil {
			log.Printf("❌ Failed to create ClickHouse client: %v", err)
			continue
		}
		defer db.Close()

		// Test keys with prefix
		baseKey := "test_key"
		testKeys := []string{"1", "2", "3"}
		testValues := []string{
			"Initial value for test key 1",
			"Initial value for test key 2",
			"Initial value for test key 3",
		}
		// 0. CREATE TABLE IF NOT EXISTS ON CLUSTER
		fmt.Println("0. Creating table if not exists on cluster...")
		createLocalTableSQL := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s.%s ON CLUSTER '%s' (
				key String,
				value String
			) ENGINE = ReplicatedMergeTree
			ORDER BY key
		`, chConfig.Database, tableName, "{cluster}")
		_, err = db.ExecContext(ctx, createLocalTableSQL)
		if err != nil {
			log.Printf("❌ Create local table error: %v", err)
			continue
		} else {
			fmt.Printf("✓ Local table %s.%s ready\n", chConfig.Database, tableName)
		}

		// Create Distributed table
		fmt.Println("0. Creating distributed table...")
		distTableName := tableName + "_dist"
		createDistTableSQL := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s.%s ON CLUSTER '%s' AS %s.%s
			ENGINE = Distributed('%s', '%s', '%s', rand())
		`, chConfig.Database, distTableName, "{cluster}", chConfig.Database, tableName, "{cluster}", chConfig.Database, tableName)
		print(createDistTableSQL)
		_, err = db.ExecContext(ctx, createDistTableSQL)
		if err != nil {
			log.Printf("❌ Create distributed table error: %v", err)
			continue
		} else {
			fmt.Printf("✓ Distributed table %s.%s ready\n", chConfig.Database, distTableName)
		}

		// 1. CREATE (Insert) Keys
		fmt.Println("1. Creating test keys...")
		testItems := make([]struct{ Key, Value string }, len(testKeys))

		for j, key := range testKeys {
			fullKey := addPrefix(baseKey + "_" + key)
			value := testValues[j]

			_, err = db.ExecContext(ctx, "INSERT INTO "+chConfig.Database+"."+tableName+" (key, value) VALUES (?, ?)", fullKey, value)
			if err != nil {
				log.Printf("❌ Insert key %s error: %v", fullKey, err)
				continue
			}

			testItems[j] = struct{ Key, Value string }{baseKey + "_" + key, value}
			fmt.Printf("✓ Key %s inserted with value: %s\n", baseKey+"_"+key, value)
		}

		// 2. READ (Get) Keys
		fmt.Println("2. Reading test keys...")
		for _, item := range testItems {
			if item.Key == "" {
				continue // Skip if insert failed
			}

			fullKey := addPrefix(item.Key)
			var retrievedValue string
			err = db.QueryRowContext(ctx, "SELECT value FROM "+chConfig.Database+"."+tableName+" WHERE key = ?", fullKey).Scan(&retrievedValue)
			if err != nil {
				log.Printf("❌ Get key %s error: %v", item.Key, err)
				continue
			}

			if retrievedValue == "" {
				log.Printf("❌ Key %s not found", item.Key)
				continue
			}

			fmt.Printf("✓ Key %s retrieved: %s\n", item.Key, retrievedValue)
		}

		// 3. UPDATE Key
		fmt.Println("3. Updating test key...")
		updateKey := baseKey + "_1"
		updatedValue := "Updated value for test key 1 - modified at " + time.Now().Format(time.RFC3339)

		fullUpdateKey := addPrefix(updateKey)
		updateSQL := fmt.Sprintf("ALTER TABLE %s.%s UPDATE value = ? WHERE key = ?", chConfig.Database, tableName)
		_, err = db.ExecContext(ctx, updateSQL, updatedValue, fullUpdateKey)
		if err != nil {
			log.Printf("❌ Update key %s error: %v", updateKey, err)
		} else {
			// Wait briefly for mutation, then verify
			time.Sleep(1 * time.Second)
			var updatedBytes string
			err = db.QueryRowContext(ctx, "SELECT value FROM "+chConfig.Database+"."+tableName+" WHERE key = ?", fullUpdateKey).Scan(&updatedBytes)
			if err == nil && updatedBytes != "" {
				fmt.Printf("✓ Key %s updated to: %s\n", updateKey, updatedBytes)
			} else {
				log.Printf("❌ Verification failed for updated key %s", updateKey)
			}
		}

		// 4. SCAN (Range Query) Test Keys
		fmt.Println("4. Scanning test keys...")
		startKey := addPrefix(baseKey + "_")
		endKey := addPrefix(baseKey + "_z")

		rows, err := db.QueryContext(ctx, "SELECT key, value FROM "+chConfig.Database+"."+tableName+" WHERE key >= ? AND key < ? LIMIT 100", startKey, endKey)
		if err != nil {
			log.Printf("❌ Scan error: %v", err)
		} else {
			count := 0
			for rows.Next() {
				var fullKey, value string
				if err := rows.Scan(&fullKey, &value); err != nil {
					break
				}
				cleanKey := removePrefix(fullKey)
				fmt.Printf("  - %s: %s\n", cleanKey, value)
				count++
			}
			fmt.Printf("✓ Scan found %d keys in range\n", count)
			rows.Close()
		}

		// 5. DELETE Key
		fmt.Println("5. Deleting test key...")
		deleteKey := baseKey + "_2"
		fullDeleteKey := addPrefix(deleteKey)

		// Verify exists before delete
		var exists int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+chConfig.Database+"."+tableName+" WHERE key = ?", fullDeleteKey).Scan(&exists)
		if err != nil || exists == 0 {
			log.Printf("❌ Key %s not found for deletion", deleteKey)
		} else {
			deleteSQL := fmt.Sprintf("ALTER TABLE %s.%s DELETE WHERE key = ?", chConfig.Database, tableName)
			_, err = db.ExecContext(ctx, deleteSQL, fullDeleteKey)
			if err != nil {
				log.Printf("❌ Delete key %s error: %v", deleteKey, err)
			} else {
				// Wait briefly for mutation, then verify
				time.Sleep(1 * time.Second)
				var existsAfter int
				err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+chConfig.Database+"."+tableName+" WHERE key = ?", fullDeleteKey).Scan(&existsAfter)
				if err == nil && existsAfter == 0 {
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
			cleanupSQL := fmt.Sprintf("ALTER TABLE %s.%s DELETE WHERE key = ?", chConfig.Database, tableName)
			_, err = db.ExecContext(ctx, cleanupSQL, fullCleanupKey)
			if err != nil {
				log.Printf("⚠️ Cleanup warning for %s: %v", item.Key, err)
			}
		}

		// Final verification - check one key still exists
		finalCheckKey := baseKey + "_3"
		var checkValue string
		err = db.QueryRowContext(ctx, "SELECT value FROM "+chConfig.Database+"."+tableName+" WHERE key = ?", addPrefix(finalCheckKey)).Scan(&checkValue)
		if err == nil && checkValue != "" {
			fmt.Printf("✓ Final verification: key %s still exists with value %s\n", finalCheckKey, checkValue)
		}

		fmt.Printf("✅ ClickHouse %d test completed\n", i+1)
	}
}
