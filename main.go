package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	_ "github.com/go-sql-driver/mysql"
	auth "golang.org/x/oauth2/google"
)

type InsertResult struct {
	Kind          string    `json:"kind"`
	TargetLink    string    `json:"targetLink"`
	Status        string    `json:"status"`
	User          string    `json:"user"`
	InsertTime    time.Time `json:"insertTime"`
	OperationType string    `json:"operationType"`
	OperationID   string    `json:"name"`
	TargetID      string    `json:"targetId"`
	SelfLink      string    `json:"selfLink"`
	TargetProject string    `json:"targetProject"`
	BackupContext struct {
		BackupID string `json:"backupId"`
		Kind     string `json:"kind"`
	} `json:"backupContext"`
}

type BackupItem struct {
	Kind            string    `json:"kind"`
	Status          string    `json:"status"`
	EnqueuedTime    time.Time `json:"enqueuedTime"`
	BackupID        string    `json:"id"`
	StartTime       time.Time `json:"startTime"`
	EndTime         time.Time `json:"endTime"`
	Type            string    `json:"type"`
	WindowStartTime time.Time `json:"windowStartTime"`
	Instance        string    `json:"instance"`
	SelfLink        string    `json:"selfLink"`
	Location        string    `json:"location"`
	BackupKind      string    `json:"backupKind"`
}
type BackupRunsList struct {
	Kind  string       `json:"kind"`
	Items []BackupItem `json:"items"`
}
type Operation struct {
	Status        string    `json:"status"`
	OperationID   string    `json:"name"`
	InsertTime    time.Time `json:"insertTime"`
	StartTime     time.Time `json:"startTime"`
	EndTime       time.Time `json:"endTime"`
	OperationType string    `json:"operationType"`
	TargetID      string    `json:"targetId"`
	BackupContext struct {
		BackupID string `json:"backupId"`
		Kind     string `json:"kind"`
	} `json:"backupContext,omitempty"`
	// Other fields
	// Kind          string    `json:"kind"`
	// TargetLink    string    `json:"targetLink"`
	// SelfLink      string    `json:"selfLink"`
	// TargetProject string    `json:"targetProject"`
	// User string `json:"user,omitempty"`
}
type OperationsList struct {
	Kind  string      `json:"kind"`
	Items []Operation `json:"items"`
}
type PostResponse struct {
	Kind          string    `json:"kind"`
	TargetLink    string    `json:"targetLink"`
	Status        string    `json:"status"`
	User          string    `json:"user"`
	InsertTime    time.Time `json:"insertTime"`
	OperationType string    `json:"operationType"`
	OperationID   string    `json:"name"`
	TargetID      string    `json:"targetId"`
	SelfLink      string    `json:"selfLink"`
	TargetProject string    `json:"targetProject"`
}

func getAuthToken() string {
	ctx := context.Background()
	scopes := []string{
		"https://www.googleapis.com/auth/cloud-platform",
	}
	credentials, err := auth.FindDefaultCredentials(ctx, scopes...)
	if err != nil {
		log.Fatal(err)
	}
	token, err := credentials.TokenSource.Token()
	if err != nil {
		log.Fatal(err)
	}

	return (fmt.Sprintf("Bearer %v", string(token.AccessToken)))
}

func InsertBackupRuns(project string, instance string) (InsertResult, error) {
	url := fmt.Sprintf("https://sqladmin.googleapis.com/v1/projects/%s/instances/%s/backupRuns", project, instance)
	method := "POST"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return InsertResult{}, err
	}

	req.Header.Add("Authorization", getAuthToken())

	res, err := client.Do(req)
	if err != nil {
		return InsertResult{}, err
	}
	defer res.Body.Close()

	var backupAction InsertResult
	err = json.NewDecoder(res.Body).Decode(&backupAction)
	if err != nil {
		return InsertResult{}, err
	}
	return backupAction, nil

}

func GetBackupState(project string, instance string, backupID string) (BackupItem, error) {
	url := fmt.Sprintf("https://sqladmin.googleapis.com/v1/projects/%s/instances/%s/backupRuns/%s", project, instance, backupID)
	method := "GET"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return BackupItem{}, err
	}

	req.Header.Add("Authorization", getAuthToken())

	res, err := client.Do(req)
	if err != nil {
		return BackupItem{}, err
	}
	defer res.Body.Close()

	var backupRun BackupItem
	err = json.NewDecoder(res.Body).Decode(&backupRun)
	if err != nil {
		return BackupItem{}, err
	}
	return backupRun, nil
}
func GetOperationState(project string, instance string, operationID string) (Operation, error) {
	url := fmt.Sprintf("https://sqladmin.googleapis.com/v1/projects/%s/operations/%s?instance=%s", project, operationID, instance)
	method := "GET"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return Operation{}, err
	}

	req.Header.Add("Authorization", getAuthToken())

	res, err := client.Do(req)
	if err != nil {
		return Operation{}, err
	}
	defer res.Body.Close()

	var operation Operation
	err = json.NewDecoder(res.Body).Decode(&operation)
	if err != nil {
		return Operation{}, err
	}

	return operation, nil
}
func LatestOperation(project string, instance string) (Operation, error) {
	url := fmt.Sprintf("https://sqladmin.googleapis.com/v1/projects/%s/operations?instance=%s", project, instance)
	method := "GET"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return Operation{}, err
	}

	req.Header.Add("Authorization", getAuthToken())

	res, err := client.Do(req)
	if err != nil {
		return Operation{}, err
	}
	defer res.Body.Close()

	var operationsList OperationsList
	err = json.NewDecoder(res.Body).Decode(&operationsList)
	if err != nil {
		return Operation{}, err
	}

	// Filter operations with OperationType as RESTORE_VOLUME
	var filteredOperations []Operation
	for _, operation := range operationsList.Items {
		if operation.OperationType == "RESTORE_VOLUME" {
			filteredOperations = append(filteredOperations, operation)
		}
	}

	// Sort operations based on InsertTime in descending order
	sort.Slice(filteredOperations, func(i, j int) bool {
		return filteredOperations[i].InsertTime.After(filteredOperations[j].InsertTime)
	})

	if len(filteredOperations) == 0 {
		return Operation{}, nil
	}
	return filteredOperations[0], nil
}
func LatestBackupRun(project string, instance string) (BackupItem, error) {
	url := fmt.Sprintf("https://sqladmin.googleapis.com/v1/projects/%s/instances/%s/backupRuns", project, instance)
	method := "GET"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return BackupItem{}, err
	}

	req.Header.Add("Authorization", getAuthToken())

	res, err := client.Do(req)
	if err != nil {
		return BackupItem{}, err
	}
	defer res.Body.Close()

	var backupRunsList BackupRunsList
	err = json.NewDecoder(res.Body).Decode(&backupRunsList)
	if err != nil {
		return BackupItem{}, err
	}

	if len(backupRunsList.Items) == 0 {
		return BackupItem{}, errors.New("No backup runs found")
	}

	sort.Slice(backupRunsList.Items, func(i, j int) bool {
		return backupRunsList.Items[i].StartTime.After(backupRunsList.Items[j].StartTime)
	})

	latestBackup := backupRunsList.Items[0]
	return latestBackup, nil
}
func RestoreBackup(project string, instance string, backupID string, sourceProject string, sourceInstance string) (PostResponse, error) {
	// Make HTTP POST request to second API
	url := fmt.Sprintf("https://sqladmin.googleapis.com/v1/projects/%s/instances/%s/restoreBackup", project, instance)
	method := "POST"
	requestData := map[string]interface{}{
		"restoreBackupContext": map[string]interface{}{
			"backupRunId": backupID,
			"project":     sourceProject,
			"instanceId":  sourceInstance,
		},
	}
	requestDataBytes, err := json.Marshal(requestData)
	if err != nil {
		return PostResponse{}, err
	}

	client := &http.Client{}
	req, err := http.NewRequest(method, url, bytes.NewBuffer(requestDataBytes))
	if err != nil {
		return PostResponse{}, err
	}
	req.Header.Add("Authorization", getAuthToken())
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return PostResponse{}, err
	}
	defer res.Body.Close()
	var response PostResponse
	err = json.NewDecoder(res.Body).Decode(&response)
	if err != nil {
		return PostResponse{}, err
	}
	// log.Printf("%+v\n", response)
	return response, nil
}
func ExecuteCloudSQLProxy(hostName string) (*os.Process, error) {
	log.SetFlags(0)
	// Execute the Cloud SQL Proxy command
	// Ref: https://cloud.google.com/sql/docs/mysql/connect-auth-proxy
	cmd := exec.Command("cloud-sql-proxy", hostName, "--unix-socket", "/cloudsql")
	// Create a pipe to capture stdout and stderr
	stdoutPipe, _ := cmd.StdoutPipe()

	// Start the command
	err := cmd.Start()
	if err != nil {
		return nil, err
	}

	// Set up a timer to break the loop after 5 seconds
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	// Wait for the Cloud SQL Proxy to start and check for success message
	reader := bufio.NewReader(stdoutPipe)
loop:
	for {
		select {
		case <-timer.C:
			log.Println("Command output:")
			io.Copy(os.Stdout, reader)
			log.Println("Timed out waiting for the Cloud SQL Proxy to start")
			// Send SIGINT signal to shut down the process
			cmd.Process.Signal(os.Interrupt)
			break loop
		default:
			line, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				return nil, err
			}
			if strings.Contains(line, "is ready for new connections!") {
				// cloud-sql-proxy should print "The proxy has started successfully and is ready for new connections!"
				break loop
			}
			if strings.Contains(line, "error") {
				log.Panicf(line)
				// Send SIGINT signal to shut down the process
				cmd.Process.Signal(os.Interrupt)
				break loop
			}
		}
	}

	return cmd.Process, nil
}
func GetDatabasePassword(project string, path string) (string, error) {
	ctx := context.Background()
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return "", err
	}
	defer client.Close()

	secretPath := fmt.Sprintf("projects/%s/secrets/%s/versions/latest", project, path)
	accessRequest := &secretmanagerpb.AccessSecretVersionRequest{
		Name: secretPath,
	}

	result, err := client.AccessSecretVersion(ctx, accessRequest)
	if err != nil {
		return "", err
	}

	return string(result.Payload.Data), nil
}
func CheckDatabaseConnection(user string, pass string, host string, database string) (*sql.DB, error) {
	log.SetFlags(0)
	// Set up the connection string.
	connString := fmt.Sprintf("%s:%s@unix(/cloudsql/%s)/%s", user, pass, host, database)
	// Open the connection.
	db, err := sql.Open("mysql", connString)
	if err != nil {
		return nil, err
	}

	// Check the connection.
	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}
func QuerySQL(db *sql.DB, queryCommands string) error {

	log.Println(queryCommands)
	// Start the timer.
	start := time.Now()

	// Query the database.
	rows, err := db.Query(queryCommands)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Get the column names.
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// Print the SQL query execution time.
	elapsed := time.Since(start)
	log.Printf("SQL query took %s\n", elapsed)

	// Print the column names.
	var header []string
	for _, col := range columns {
		header = append(header, col)
	}
	log.Println(strings.Join(header, " | "))

	// Loop through the rows and print the columns.
	for rows.Next() {
		// Create a slice of pointers to interface{} values to hold the column values.
		values := make([]interface{}, len(columns))
		for i := range columns {
			values[i] = new(interface{})
		}

		// Scan the row values into the values slice.
		err = rows.Scan(values...)
		if err != nil {
			return err
		}

		// Print the row values.
		var row []string
		for _, col := range values {
			switch v := *(col.(*interface{})); v.(type) {
			case []byte:
				row = append(row, string(v.([]byte)))
			case nil:
				row = append(row, "<nil>")
			default:
				row = append(row, fmt.Sprintf("%v", v))
			}
		}
		log.Println(strings.Join(row, " | "))
	}

	return nil
}
func main() {
	log.SetFlags(0)

	createBackup := flag.Bool("create-backup", false, "Set to true to create a backup, or leave it empty/false to get the latest backup.")
	sourceProject := flag.String("source-project", "", "the source project id to sync database from")
	sourceInstance := flag.String("source-instance", "", "the source database instance to sync database from")
	targetProject := flag.String("target-project", "", "the target project id to sync database")
	targetInstance := flag.String("target-instance", "", "the target database instance to sync database")
	targetRegion := flag.String("target-region", "", "the target project region")
	database := flag.String("database", "", "database name to modify")
	username := flag.String("username", "", "user name to connect to the database")
	passwordPath := flag.String("password-path", "", "the password path in Secret Manager")
	flag.Parse()

	if *targetProject == "" || *targetInstance == "" || *sourceProject == "" || *sourceInstance == "" {
		log.Fatal("Error: Parameter 'target-project', 'target-Database', 'source-project' and 'source-Database' are required")
	}

	var backupID string
	if *createBackup {
		backupAction, err := InsertBackupRuns(*sourceProject, *sourceInstance)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}
		log.Println("Backup ID:", backupAction.BackupContext.BackupID)
		log.Println("Backup State:", backupAction.Status)
		for {
			backupRun, err := GetBackupState(*sourceProject, *sourceInstance, backupAction.BackupContext.BackupID)
			if err != nil {
				log.Fatal(err)
				os.Exit(1)
			}
			if backupRun.Status == "SUCCESSFUL" {
				log.Println("Backup successful!")
				break
			} else if backupRun.Status == "ENQUEUED" || backupRun.Status == "RUNNING" || backupRun.Status == "PENDING" {
				log.Println("Backup state:", backupRun.Status)
				time.Sleep(30 * time.Second) // Wait 30 seconds before checking again
			} else {
				log.Panicln("Backup state:", backupRun.Status)
				os.Exit(1)
			}
		}
		backupID = backupAction.BackupContext.BackupID
	} else {
		latestBackupRun, err := LatestBackupRun(*sourceProject, *sourceInstance)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}
		log.Println("Latest Backup ID:", latestBackupRun.BackupID)
		backupID = latestBackupRun.BackupID
	}

	// Restore backup
	_, err := RestoreBackup(*targetProject, *targetInstance, backupID, *sourceProject, *sourceInstance)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	for {
		latestOperation, err := LatestOperation(*targetProject, *targetInstance)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}
		if latestOperation.Status == "DONE" {
			log.Println("Restore successful!")
			break
		} else if latestOperation.Status == "RUNNING" || latestOperation.Status == "PENDING" {
			log.Println("Restore state:", latestOperation.Status)
			time.Sleep(30 * time.Second) // Wait 30 seconds before checking again
		} else {
			log.Panicln("Restore state:", latestOperation.Status)
			os.Exit(1)
		}
	}

	if *username != "" && *passwordPath != "" && *database != "" {
		log.Println("Starting to modify database")
		// Get the database password from Secret Manager
		password, err := GetDatabasePassword(*targetProject, *passwordPath)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}

		host := fmt.Sprintf("%s:%s:%s", *targetProject, *targetRegion, *targetInstance)

		_, err = ExecuteCloudSQLProxy(host)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}

		db, err := CheckDatabaseConnection(*username, password, host, *database)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}

		var query string
		if *database == "massimo_db" {
			log.Println("Modify massimo database")
			query = "UPDATE `assets` SET `resource` = REPLACE(`resource`, 'source_project', 'target_project') WHERE `resource` LIKE '%source_project%';"
			query += "UPDATE `asset_storages` SET `bucket_name` = REPLACE(`bucket_name`, 'source_project', 'target_project') WHERE `bucket_name` LIKE '%source_project%';"
			query += "UPDATE `asset_images` SET `thumbnail_url` = REPLACE(`thumbnail_url`, 'source_project', 'target_project') WHERE `thumbnail_url` LIKE '%source_project%';"
			query += "SELECT * FROM asset_storages ORDER BY `id` LIMIT 300 OFFSET 0;"
			query = strings.Replace(query, "source_project", *sourceProject, -1)
			query = strings.Replace(query, "target_project", *targetProject, -1)
		} else if *database == "layout_service" {
			log.Println("Modify layout database")
			query = "UPDATE `layouts` SET `layout` = REPLACE(`layout`, 'source_project', 'target_project') WHERE `layout` LIKE '%source_project%';"
			query += "SELECT * FROM layouts ORDER BY `id` LIMIT 300 OFFSET 0;"
			query = strings.Replace(query, "source_project", *sourceProject, -1)
			query = strings.Replace(query, "target_project", *targetProject, -1)
		}

		// Split the SQL commands into individual statements.
		statements := strings.Split(query, ";")

		// Loop through the statements and execute each UPDATE query.
		for _, stmt := range statements {
			// Skip any empty statements.
			if strings.TrimSpace(stmt) == "" {
				continue
			}

			err = QuerySQL(db, stmt)
			if err != nil {
				log.Fatal(err)
				os.Exit(1)
			}
		}
	}
}
