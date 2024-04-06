package model

type Database struct {
	DatabaseName string `json:"database_name"`
	DBHost       string `json:"db_host"`
	DBPort       string `json:"db_port"`
	DBUsername   string `json:"db_username"`
	DBPassword   string `json:"db_password"`
}
