package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

func initDatabase(db *sql.DB) {
	query := `CREATE TABLE IF NOT EXISTS tbl_migrations (
		mg_id INTEGER PRIMARY KEY,
		mg_sql_filename NVARCHAR(256),
		mg_date_applied TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`
	_, err := db.Exec(query)

	if err != nil {
		log.Fatal("Could not initialise migration file:\n", err)
	}

}

func retrieveMigrationPaths(schemaPath string) []os.DirEntry {

	if schemaPath == "" {
		schemaPath = "schema/"
	}

	migrations, err := os.ReadDir(schemaPath)

	if err != nil {
		log.Fatal("Error reading migration files: ", err)
	}

	fmt.Println("getting all migrations regardless of whether they have been applied")
	for i, m := range migrations {
		msg := fmt.Sprintf("migration %d was read from file (%s/%s)", i, schemaPath, m)
		fmt.Println(msg)
	}

	return migrations
}

func getUnAppliedMigrations(migrationFiles []os.DirEntry, db *sql.DB) []os.DirEntry {
	// query existing migrations
	existingMigrationRows, err := db.Query("SELECT mg_id, mg_sql_filename FROM tbl_migrations;")

	if err != nil {
		msg := "Error reading existing migrations from the database:\n"
		log.Fatal(msg, err)
	}

	defer existingMigrationRows.Close()

	filePaths := make([]string, 0)

	for existingMigrationRows.Next() {
		var filePath string
		var migrationID int
		if err := existingMigrationRows.Scan(&migrationID, &filePath); err != nil {
			msg := "error scanning single row"
			log.Fatal(msg, err)
		}
		info := fmt.Sprintf("migration %d was retrieved (%s)", migrationID, filePath)
		fmt.Println(info)
		filePaths = append(filePaths, filePath)
	}

	// create looup map to determine if migration has already been made
	allMigrationMap := make(map[string]bool)

	for _, file := range filePaths {
		allMigrationMap[file] = true
	}

	unappliedMigrationFiles := make([]os.DirEntry, 0)

	for _, migrationFile := range migrationFiles {
		if _, found := allMigrationMap[migrationFile.Name()]; !found {
			unappliedMigrationFiles = append(unappliedMigrationFiles, migrationFile)
		}
	}

	return unappliedMigrationFiles

}

func makeMigrations(schemaPath string, db *sql.DB) {

	migrationFiles := retrieveMigrationPaths(schemaPath)

	//add handler to create slice of migration files starting at first migration that has not been applied
	newMigrations := getUnAppliedMigrations(migrationFiles, db)

	appliedMigrations := make([]string, 0)

	for i, m := range newMigrations {
		fullPath := fmt.Sprintf("%s/%s", schemaPath, m.Name())
		schemaSql, readErr := os.ReadFile(fullPath)

		if readErr != nil {
			msg := fmt.Sprintf("could not read  %dth migration file (%s):\n", i, m.Name())
			log.Fatal(msg, readErr)
		}

		_, sqlErr := db.Exec(string(schemaSql))

		if sqlErr != nil {
			msg := fmt.Sprintf("could not apply migration (i:%d, name: %s): \n", i, m.Name())
			log.Fatal(msg, sqlErr)
		}

		_, migrationUpdateErr := db.Exec("INSERT INTO tbl_migrations (mg_sql_filename) values (?)", m.Name())

		if migrationUpdateErr != nil {
			msg := fmt.Sprintf("could not create a db record for migration entry %d (name=%s):\n", i, m.Name())
			log.Fatal(msg, migrationUpdateErr)
		}

		appliedMigrations = append(appliedMigrations, m.Name())

	}

	if nMig := len(appliedMigrations); nMig == 0 {
		fmt.Println("No migrations were applied")
	} else {
		msg := fmt.Sprintf("%d migrations were applied", nMig)
		fmt.Println(msg)
	}

}

func main() {

	schemaPath := "schema/"
	// schemaSql, _ := os.ReadFile(schemaPath)

	db, err := sql.Open("sqlite3", "db.sqlite3")

	if err != nil {
		log.Fatal("could not open database: ")
	}

	initDatabase(db)

	makeMigrations(schemaPath, db)

	//TODO: add some method of ensuring that migrations are applied in order

	//add 1-2 digits to migration filename
	//add validation to migration filename
	//add validation of applied migrations against filename
	//add migration squashing mechanism

}
