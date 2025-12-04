package service

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"data-check-all/model"
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"strings"
	"time"
)

func TestTiDB(tidbs []model.TiDBConfig) {
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
			return nil, nil // Empty path means no cert
		}
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read cert file %s: %w", filePath, err)
		}
		return data, nil
	}

	getTLSConfig := func(config model.TiDBConfig) string {
		if !config.TLS {
			return "" // No TLS, return empty
		}
		log.Println("TLS enabled for TiDB connection")

		// Load CA cert
		caPEM, err := loadCertFromFile(config.SSLCACRT)
		if err != nil {
			log.Fatalf("Failed to load CA cert: %v", err)
		}
		if caPEM == nil {
			// For no CA, register insecure config
			tlsConfigName := "tidb-insecure"
			mysql.RegisterTLSConfig(tlsConfigName, &tls.Config{InsecureSkipVerify: true})
			return tlsConfigName
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caPEM)

		// Load client cert and key if provided
		tlsConfig := &tls.Config{
			RootCAs:            caCertPool,
			ServerName:         "",
			InsecureSkipVerify: true,
		}

		if config.SSLClientCRT != "" && config.SSLClientKey != "" {
			certPEM, err := loadCertFromFile(config.SSLClientCRT)
			if err != nil {
				log.Fatalf("Failed to load client cert: %v", err)
			}
			keyPEM, err := loadCertFromFile(config.SSLClientKey)
			if err != nil {
				log.Fatalf("Failed to load client key: %v", err)
			}
			cert, err := tls.X509KeyPair(certPEM, keyPEM)
			if err != nil {
				log.Fatalf("Failed to parse client cert/key: %v", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		// Register unique name per config (or use fixed name if single instance)
		tlsConfigName := fmt.Sprintf("tidb-tls-%s", config.Host) // Unique per host
		mysql.RegisterTLSConfig(tlsConfigName, tlsConfig)
		log.Printf("Registered TLS config: %s", tlsConfigName)
		return tlsConfigName
	}

	getClient := func(config model.TiDBConfig) (*sql.DB, error) {
		log.Printf("Connecting to TiDB at %s:%d (TLS: %v)", config.Host, config.Port, config.TLS)

		// Get or register TLS config
		tlsConfigName := getTLSConfig(config) // Now returns string

		// Build base DSN without TLS file paths
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&interpolateParams=true",
			config.Username, config.Password, config.Host, config.Port, config.Database)

		if config.TLS && tlsConfigName != "" {
			dsn += fmt.Sprintf("&tls=%s", tlsConfigName)
		} else if !config.TLS {
			// Non-TLS connection
			dsn += "&allowNativePasswords=true"
		}

		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return nil, fmt.Errorf("failed to open TiDB connection: %w", err)
		}

		// Configure connection pool (uncomment if needed)
		// db.SetMaxIdleConns(5)
		// db.SetMaxOpenConns(10)
		// db.SetConnMaxLifetime(time.Hour)

		// Test connection with Ping (more reliable for TLS handshake issues)
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second) // Add timeout
		defer cancel()
		err = db.PingContext(ctx)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("connection ping failed: %w", err)
		}

		// Get TiDB version for logging
		var version string
		err = db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
		if err != nil {
			log.Printf("Warning: Could not retrieve version: %v", err)
			version = "unknown"
		}

		log.Printf("✓ Connected to TiDB %s", version)
		return db, nil
	}

	// Your TiDB config

	for i, tidbConfig := range tidbs {
		fmt.Printf("\n=== Testing TiDB %d: %s:%d ===\n", i+1, tidbConfig.Host, tidbConfig.Port)

		// Set configuration for testing
		prefix = tidbConfig.Database + ":"
		tableName := tidbConfig.TableName
		if tableName == "" {
			tableName = "test"
		}

		db, err := getClient(tidbConfig)
		if err != nil {
			log.Printf("❌ Failed to create TiDB client: %v", err)
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

		// 0. CREATE TABLE IF NOT EXISTS
		fmt.Println("0. Creating table if not exists...")
		createTableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (`key` varchar(255) PRIMARY KEY, `value` TEXT)", tidbConfig.Database, tableName)
		fmt.Print(createTableSQL)
		_, err = db.ExecContext(ctx, createTableSQL)
		if err != nil {
			log.Printf("❌ Create table error: %v", err)
			log.Printf("SQL attempted: %s", createTableSQL)
			continue
		} else {
			fmt.Printf("✓ Table %s.%s ready\n", tidbConfig.Database, tableName)
		}

		// 1. CREATE (Insert) Keys
		fmt.Println("1. Creating test keys...")
		testItems := make([]struct{ Key, Value string }, len(testKeys))

		for j, key := range testKeys {
			fullKey := addPrefix(baseKey + "_" + key)
			value := testValues[j]

			_, err = db.ExecContext(ctx, "INSERT INTO "+tidbConfig.Database+"."+tableName+" (`key`, `value`) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)", fullKey, value)
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
			err = db.QueryRowContext(ctx, "SELECT value FROM "+tidbConfig.Database+"."+tableName+" WHERE `key` = ?", fullKey).Scan(&retrievedValue)
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
		_, err = db.ExecContext(ctx, "UPDATE "+tidbConfig.Database+"."+tableName+" SET `value` = ? WHERE `key` = ?", updatedValue, fullUpdateKey)
		if err != nil {
			log.Printf("❌ Update key %s error: %v", updateKey, err)
		} else {
			// Verify immediately (TiDB updates synchronously)
			var updatedValueStr string
			err = db.QueryRowContext(ctx, "SELECT value FROM "+tidbConfig.Database+"."+tableName+" WHERE `key` = ?", fullUpdateKey).Scan(&updatedValueStr)
			if err == nil && updatedValueStr != "" {
				fmt.Printf("✓ Key %s updated to: %s\n", updateKey, updatedValueStr)
			} else {
				log.Printf("❌ Verification failed for updated key %s", updateKey)
			}
		}

		// 4. SCAN (Range Query) Test Keys
		fmt.Println("4. Scanning test keys...")
		startKey := addPrefix(baseKey + "_")
		endKey := addPrefix(baseKey + "_z")

		rows, err := db.QueryContext(ctx, "SELECT `key`, `value` FROM "+tidbConfig.Database+"."+tableName+" WHERE `key` >= ? AND `key` < ? ORDER BY `key` LIMIT 100", startKey, endKey)
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

		// // 5. DELETE Key
		// fmt.Println("5. Deleting test key...")
		// deleteKey := baseKey + "_2"
		// fullDeleteKey := addPrefix(deleteKey)

		// // Verify exists before delete
		// var exists int
		// err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tidbConfig.Database+"."+tableName+" WHERE key = ?", fullDeleteKey).Scan(&exists)
		// if err != nil || exists == 0 {
		// 	log.Printf("❌ Key %s not found for deletion", deleteKey)
		// } else {
		// 	_, err = db.ExecContext(ctx, "DELETE FROM "+tidbConfig.Database+"."+tableName+" WHERE key = ?", fullDeleteKey)
		// 	if err != nil {
		// 		log.Printf("❌ Delete key %s error: %v", deleteKey, err)
		// 	} else {
		// 		// Verify immediately
		// 		var existsAfter int
		// 		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tidbConfig.Database+"."+tableName+" WHERE key = ?", fullDeleteKey).Scan(&existsAfter)
		// 		if err == nil && existsAfter == 0 {
		// 			fmt.Printf("✓ Key %s deleted successfully\n", deleteKey)
		// 		} else {
		// 			log.Printf("❌ Key %s still exists after deletion", deleteKey)
		// 		}
		// 	}
		// }

		// // 6. CLEANUP - Delete remaining test keys
		// fmt.Println("6. Cleaning up test keys...")
		// for _, item := range testItems {
		// 	if item.Key == "" {
		// 		continue
		// 	}
		// 	// Skip the key we're keeping for final verification (adjust if needed)
		// 	if strings.Contains(item.Key, "2") {
		// 		continue
		// 	}

		// 	fullCleanupKey := addPrefix(item.Key)
		// 	_, err = db.ExecContext(ctx, "DELETE FROM "+tidbConfig.Database+"."+tableName+" WHERE key = ?", fullCleanupKey)
		// 	if err != nil {
		// 		log.Printf("⚠️ Cleanup warning for %s: %v", item.Key, err)
		// 	}
		// }

		// Final verification - check one key still exists
		finalCheckKey := baseKey + "_3"
		var checkValue string
		err = db.QueryRowContext(ctx, "SELECT value FROM "+tidbConfig.Database+"."+tableName+" WHERE key = ?", addPrefix(finalCheckKey)).Scan(&checkValue)
		if err == nil && checkValue != "" {
			fmt.Printf("✓ Final verification: key %s still exists with value %s\n", finalCheckKey, checkValue)
		}

		fmt.Printf("✅ TiDB %d test completed\n", i+1)
	}
}
