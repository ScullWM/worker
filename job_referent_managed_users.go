package main

func ReferentManagedUsersPrint(message string) {
	JobPrint("referent-managed-users", message)
}

func ReferentManagedUsers() {
	ReferentManagedUsersPrint("Connecting to the database")

	db := GetDatabaseConnection()
	defer db.Close()

	// group_concat_max_len
	ReferentManagedUsersPrint("Setting group_concat_max_len")

	if _, err := db.Exec(`SET group_concat_max_len=15000`); err != nil {
		panic(err)
	}

	// Adherents
	ReferentManagedUsersPrint("Inserting adherents")

	if _, err := db.Exec(`
		INSERT INTO projection_referent_managed_users
		(status, type, original_id, email, postal_code, city, country, first_name, last_name, age, phone, committees, is_committee_member, is_committee_host, is_mail_subscriber, created_at)
			SELECT
				0,
				'adherent',
				a.id,
				a.email_address,
				a.address_postal_code,
				a.address_city_name,
				a.address_country,
				a.first_name,
				a.last_name,
				TIMESTAMPDIFF(YEAR, a.birthdate, CURDATE()) AS age,
				a.phone,
				(
					SELECT GROUP_CONCAT(c.name SEPARATOR '|')
					FROM committees_memberships cm
					LEFT JOIN committees c ON cm.committee_uuid = c.uuid
					WHERE cm.adherent_id = a.id
				),
				(
					SELECT COUNT(cm.id) > 0
					FROM committees_memberships cm
					LEFT JOIN committees c ON cm.committee_uuid = c.uuid
					WHERE cm.adherent_id = a.id AND c.status = 'APPROVED'
				),
				(
					SELECT COUNT(cm.id) > 0
					FROM committees_memberships cm
					LEFT JOIN committees c ON cm.committee_uuid = c.uuid
					WHERE cm.adherent_id = a.id AND c.status = 'APPROVED' AND (cm.privilege = 'SUPERVISOR' OR cm.privilege = 'HOST')
				),
				a.referents_emails_subscription,
				a.registered_at
			FROM adherents a
	`); err != nil {
		panic(err)
	}

	// Newsletter
	ReferentManagedUsersPrint("Inserting newsletter subscriptions")

	if _, err := db.Exec(`
		INSERT INTO projection_referent_managed_users
		(status, type, original_id, email, postal_code, city, country, first_name, last_name, age, phone, committees, is_committee_member, is_committee_host, is_mail_subscriber, created_at)
			SELECT
				0,
				'newsletter',
				n.id,
				n.email,
				n.postal_code,
				NULL,
				NULL,
				NULL,
				NULL,
				NULL,
				NULL,
				'',
				0,
				0,
				1,
				n.created_at
			FROM newsletter_subscriptions n
			WHERE LENGTH(n.postal_code) = 5
	`); err != nil {
		panic(err)
	}

	// Switching data source
	ReferentManagedUsersPrint("Switching front-end data source")

	if _, err := db.Exec(`UPDATE projection_referent_managed_users SET status = status + 1`); err != nil {
		panic(err)
	}

	// Removing expired data
	ReferentManagedUsersPrint("Removing expired data")

	if _, err := db.Exec(`DELETE FROM projection_referent_managed_users WHERE status >= 2`); err != nil {
		panic(err)
	}

	ReferentManagedUsersPrint("Done")
}
