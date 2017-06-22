package main

func ClearMailjetEmailsPrint(message string) {
	JobPrint("clear-mailjet-emails", message)
}

func ClearMailjetEmails() {
	ClearMailjetEmailsPrint("Connecting to the database")

	db := GetDatabaseConnection()
	defer db.Close()

	ClearMailjetEmailsPrint("Clearing Mailjet emails")

	if _, err := db.Exec(`DELETE FROM mailjet_emails WHERE created_at < DATE_SUB(NOW(), INTERVAL 15 DAY)`); err != nil {
		panic(err)
	}

	ClearMailjetEmailsPrint("Done")
}
