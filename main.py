import mysql.connector as database
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class DatabaseService:
    def __init__(self, host, user, password, db_name):
        self.db_name = db_name
        self.host = host
        self.user = user
        self.password = password
        self.db = None
        self.cursor = None

    def connect_db(self):
        try:
            self.db = database.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            self.cursor = self.db.cursor()
            logging.info("Database connection established.")
        except database.Error as err:
            logging.error(f"Error connecting to MySQL: {err}")
            exit(1)

    def create_db(self):
        try:
            logging.info(f'Creating database with the name: {self.db_name}')
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
            self.db.commit()
        except database.Error as err:
            logging.error(f"Error creating database: {err}")

    def select_db(self):
        try:
            logging.info(f"Selecting database: {self.db_name}")
            self.cursor.execute(f"USE {self.db_name}")
        except database.Error as err:
            logging.error(f"Error selecting database: {err}")

    def create_table(self, table_name, table_sql):
        try:
            self.cursor.execute(table_sql)
            logging.info(f"Created Table {table_name}")
        except database.Error as err:
            logging.error(f'Database error creating table {table_name}, {err}')

    def create_tables(self):
        tables = {
            "luchthaven": """
                CREATE TABLE IF NOT EXISTS luchthaven (
                    id INT PRIMARY KEY NOT NULL,
                    naam VARCHAR(255) UNIQUE NOT NULL,
                    stad VARCHAR(255) NOT NULL,
                    land VARCHAR(255) NOT NULL,
                    iata_code CHAR(3) UNIQUE NOT NULL
                );
            """,
            "vliegveld": """
                CREATE TABLE IF NOT EXISTS vliegveld (
                    id INT PRIMARY KEY NOT NULL,
                    naam VARCHAR(255) UNIQUE NOT NULL
                );
            """,
            "maatschappij": """
                CREATE TABLE IF NOT EXISTS maatschappij (
                    id INT PRIMARY KEY NOT NULL,
                    naam VARCHAR(255) UNIQUE NOT NULL
                );
            """,
            "terminal": """
                CREATE TABLE IF NOT EXISTS terminal (
                    id INT PRIMARY KEY NOT NULL,
                    naam VARCHAR(50) NOT NULL,
                    luchthaven_id INT,
                    FOREIGN KEY (luchthaven_id) REFERENCES luchthaven(id) 
                    ON UPDATE CASCADE ON DELETE CASCADE
                );
            """,
            "gate": """
                CREATE TABLE IF NOT EXISTS gate (
                    id INT PRIMARY KEY NOT NULL,
                    naam VARCHAR(255) UNIQUE NOT NULL,
                    terminal_id INT,
                    FOREIGN KEY (terminal_id) REFERENCES terminal(id) 
                    ON UPDATE CASCADE ON DELETE SET NULL
                );
            """,
            "reiziger": """
                CREATE TABLE IF NOT EXISTS reiziger (
                    id INT PRIMARY KEY NOT NULL,
                    voornaam VARCHAR(255) NOT NULL,
                    achternaam VARCHAR(255) NOT NULL
                );
            """,
            "baggage": """
                CREATE TABLE IF NOT EXISTS baggage (
                    id INT PRIMARY KEY NOT NULL,
                    gate_id INT,
                    reiziger_id INT,
                    FOREIGN KEY (gate_id) REFERENCES gate(id) 
                    ON UPDATE CASCADE ON DELETE SET NULL,
                    FOREIGN KEY (reiziger_id) REFERENCES reiziger(id) 
                    ON UPDATE CASCADE ON DELETE CASCADE
                );
            """,
            "vliegtuig": """
                CREATE TABLE IF NOT EXISTS vliegtuig (
                    id INT PRIMARY KEY NOT NULL,
                    model VARCHAR(255) NOT NULL,
                    capaciteit INT NOT NULL,
                    maatschappij_id INT,
                    FOREIGN KEY (maatschappij_id) REFERENCES maatschappij(id) 
                    ON UPDATE CASCADE ON DELETE CASCADE
                );
            """,
            "vlucht": """
                CREATE TABLE IF NOT EXISTS vlucht (
                    id INT PRIMARY KEY NOT NULL,
                    van VARCHAR(255) NOT NULL,
                    naar VARCHAR(255) NOT NULL,
                    gate_id INT,
                    vliegveld_id INT,
                    vliegtuig_id INT,
                    vertrek_luchthaven_id INT,
                    aankomst_luchthaven_id INT,
                    FOREIGN KEY (gate_id) REFERENCES gate(id) 
                    ON UPDATE CASCADE ON DELETE SET NULL,
                    FOREIGN KEY (vliegtuig_id) REFERENCES vliegtuig(id) 
                    ON UPDATE CASCADE ON DELETE SET NULL,
                    FOREIGN KEY (vliegveld_id) REFERENCES vliegveld(id) 
                    ON UPDATE CASCADE ON DELETE SET NULL,
                    FOREIGN KEY (vertrek_luchthaven_id) REFERENCES luchthaven(id) 
                    ON UPDATE CASCADE ON DELETE SET NULL,
                    FOREIGN KEY (aankomst_luchthaven_id) REFERENCES luchthaven(id) 
                    ON UPDATE CASCADE ON DELETE SET NULL
                );
            """,
            "vliegticket": """
                CREATE TABLE IF NOT EXISTS vliegticket (
                    id INT PRIMARY KEY NOT NULL,
                    vlucht_id INT,
                    reiziger_id INT,
                    baggage_id INT,
                    FOREIGN KEY (vlucht_id) REFERENCES vlucht(id) 
                    ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (reiziger_id) REFERENCES reiziger(id) 
                    ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (baggage_id) REFERENCES baggage(id) 
                    ON UPDATE CASCADE ON DELETE SET NULL
                );
            """,
            "crew": """
                CREATE TABLE IF NOT EXISTS crew (
                    id INT PRIMARY KEY NOT NULL,
                    voornaam VARCHAR(255) NOT NULL,
                    achternaam VARCHAR(255) NOT NULL,
                    functie VARCHAR(100) NOT NULL,
                    maatschappij_id INT,
                    FOREIGN KEY (maatschappij_id) REFERENCES maatschappij(id) 
                    ON UPDATE CASCADE ON DELETE SET NULL
                );
            """,
            "vliegtuig_crew": """
                CREATE TABLE IF NOT EXISTS vliegtuig_crew (
                    vliegtuig_id INT NOT NULL,
                    crew_id INT NOT NULL,
                    PRIMARY KEY (vliegtuig_id, crew_id),
                    FOREIGN KEY (vliegtuig_id) REFERENCES vliegtuig(id) 
                    ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (crew_id) REFERENCES crew(id) 
                    ON UPDATE CASCADE ON DELETE CASCADE
                );
            """,
            "vluchtschema": """
                CREATE TABLE IF NOT EXISTS vluchtschema (
                    id INT PRIMARY KEY NOT NULL,
                    vlucht_id INT,
                    vertrektijd TIMESTAMP NOT NULL,
                    aankomsttijd TIMESTAMP NOT NULL,
                    frequentie VARCHAR(50),
                    FOREIGN KEY (vlucht_id) REFERENCES vlucht(id) 
                    ON UPDATE CASCADE ON DELETE CASCADE
                );
            """
        }

        for table_name, table_sql in tables.items():
            self.create_table(table_name, table_sql)

        logging.info("All tables created successfully.")

    def insert_rows(self, sql, val):
        try:
            self.cursor.execute(sql, val)
            self.db.commit()
            logging.info(f"{sql} - records inserted")
        except database.Error as err:
            logging.error(f'Error inserting rows: {sql}, {err}')

    def insert_data(self, data):
        try:
            for table_name, rows in data.items():
                for row in rows:
                    placeholders = ', '.join(['%s'] * len(row))
                    query = f"INSERT INTO {table_name} VALUES ({placeholders})"
                    logging.info(f"Executing query: {query} with values {row}")
                    self.insert_rows(query, row)
        except database.Error as err:
            logging.error(f"Error insert_data: {err}")

    def display_tables(self):
        try:
            self.cursor.execute("SHOW TABLES")
            tables = self.cursor.fetchall()  
            logging.info("Tables in the selected database:")
            for table in tables:
                logging.info(table[0])  
        except database.Error as err:
            logging.error(f"Error fetching tables: {err}")

    def delete_db(self):
        try:
            logging.info(f'Deleting database with the name: {self.db_name}')
            self.cursor.execute(f"DROP DATABASE IF EXISTS {self.db_name}")
            self.db.commit()
            logging.info(f"Database {self.db_name} deleted successfully.")
        except database.Error as err:
            logging.error(f"Error deleting database: {err}")

    def close_connection(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.db:
                self.db.close()
            logging.info("Database connection closed.")
        except database.Error as err:
            logging.error(f"Error closing the connection: {err}")

def main():
    db_service = DatabaseService("127.0.0.1", "root", "password", "iprodamod_db")

    db_service.connect_db()
    db_service.delete_db()
    db_service.create_db()
    db_service.select_db()

    insert_values = {
        "luchthaven": [
            (1, "Schiphol", "Amsterdam", "Nederland", "AMS"),
            (2, "Heathrow", "London", "UK", "LHR"),
            (3, "Charles de Gaulle", "Paris", "France", "CDG"),
            (4, "Frankfurt", "Frankfurt", "Germany", "FRA"),
            (5, "JFK", "New York", "USA", "JFK"),
            (6, "Changi", "Singapore", "Singapore", "SIN"),
            (7, "Narita", "Tokyo", "Japan", "NRT"),
            (8, "Dubai International", "Dubai", "UAE", "DXB"),
            (9, "Los Angeles", "Los Angeles", "USA", "LAX"),
            (10, "Sydney", "Sydney", "Australia", "SYD")
        ],
        "vliegveld": [
            (1, "Schiphol International"),
            (2, "Heathrow Airport"),
            (3, "Charles de Gaulle Airport"),
            (4, "Frankfurt Airport"),
            (5, "JFK Airport"),
            (6, "Changi Airport"),
            (7, "Narita Airport"),
            (8, "Dubai International Airport"),
            (9, "Los Angeles International Airport"),
            (10, "Sydney Airport")
        ],
        "maatschappij": [
            (1, "KLM"),
            (2, "British Airways"),
            (3, "Air France"),
            (4, "Lufthansa"),
            (5, "Delta Airlines"),
            (6, "Singapore Airlines"),
            (7, "Japan Airlines"),
            (8, "Emirates"),
            (9, "American Airlines"),
            (10, "Qantas")
        ],
        "vliegtuig": [
            (1, "Boeing 747", 400, 1),
            (2, "Airbus A380", 500, 2),
            (3, "Boeing 777", 350, 3),
            (4, "Airbus A350", 300, 4),
            (5, "Boeing 737", 200, 5),
            (6, "Airbus A320", 180, 6),
            (7, "Boeing 787", 250, 7),
            (8, "Airbus A330", 290, 8),
            (9, "Boeing 767", 220, 9),
            (10, "Airbus A340", 270, 10)
        ],
        "terminal": [
            (1, "Terminal 1", 1),
            (2, "Terminal 2", 2),
            (3, "Terminal 3", 3),
            (4, "Terminal 4", 4),
            (5, "Terminal 5", 5),
            (6, "Terminal 6", 6),
            (7, "Terminal 7", 7),
            (8, "Terminal 8", 8),
            (9, "Terminal 9", 9),
            (10, "Terminal 10", 10)
        ],
        "reiziger": [
            (1, "Jan", "Jansen"),
            (2, "Piet", "Pietersen"),
            (3, "Klaas", "Klaassen"),
            (4, "Henk", "Henksen"),
            (5, "Marie", "Marietje"),
            (6, "Anna", "Annasen"),
            (7, "John", "Johnson"),
            (8, "Jane", "Doe"),
            (9, "Alice", "Wonderland"),
            (10, "Bob", "Builder")
        ],
        "crew": [
            (1, "Pieter", "Peters", "Piloot", 1),
            (2, "John", "Smith", "Co-Piloot", 2),
            (3, "Alice", "Johnson", "Stewardess", 3),
            (4, "Bob", "Brown", "Steward", 4),
            (5, "Charlie", "Davis", "Technicus", 5),
            (6, "David", "Evans", "Piloot", 6),
            (7, "Eve", "Foster", "Co-Piloot", 7),
            (8, "Frank", "Green", "Steward", 8),
            (9, "Grace", "Harris", "Stewardess", 9),
            (10, "Hank", "Ivy", "Technicus", 10)
        ],
        "gate": [
            (1, "Gate A1", 1),
            (2, "Gate B2", 2),
            (3, "Gate C3", 3),
            (4, "Gate D4", 4),
            (5, "Gate E5", 5),
            (6, "Gate F6", 6),
            (7, "Gate G7", 7),
            (8, "Gate H8", 8),
            (9, "Gate I9", 9),
            (10, "Gate J10", 10)
        ],
        "baggage": [
            (1, 1, 1),
            (2, 2, 2),
            (3, 3, 3),
            (4, 4, 4),
            (5, 5, 5),
            (6, 6, 6),
            (7, 7, 7),
            (8, 8, 8),
            (9, 9, 9),
            (10, 10, 10)
        ],
        "vlucht": [
            (1, "Amsterdam", "New York", 1, 1, 1, 1, 1),
            (2, "London", "Paris", 2, 2, 2, 2, 2),
            (3, "Paris", "Frankfurt", 3, 3, 3, 3, 3),
            (4, "Frankfurt", "Singapore", 4, 4, 4, 4, 4),
            (5, "New York", "Tokyo", 5, 5, 5, 5, 5),
            (6, "Singapore", "Dubai", 6, 6, 6, 6, 6),
            (7, "Tokyo", "Los Angeles", 7, 7, 7, 7, 7),
            (8, "Dubai", "Sydney", 8, 8, 8, 8, 8),
            (9, "Los Angeles", "Amsterdam", 9, 9, 9, 9, 9),
            (10, "Sydney", "London", 10, 10, 10, 10, 10)
        ],
        "vliegticket": [
            (1, 1, 1, 1),
            (2, 2, 2, 2),
            (3, 3, 3, 3),
            (4, 4, 4, 4),
            (5, 5, 5, 5),
            (6, 6, 6, 6),
            (7, 7, 7, 7),
            (8, 8, 8, 8),
            (9, 9, 9, 9),
            (10, 10, 10, 10)
        ],
        "vluchtschema": [
            (1, 1, "2024-10-16 10:00:00", "2024-10-16 14:00:00", "Dagelijks"),
            (2, 2, "2024-10-17 11:00:00", "2024-10-17 15:00:00", "Dagelijks"),
            (3, 3, "2024-10-18 12:00:00", "2024-10-18 16:00:00", "Dagelijks"),
            (4, 4, "2024-10-19 13:00:00", "2024-10-19 17:00:00", "Dagelijks"),
            (5, 5, "2024-10-20 14:00:00", "2024-10-20 18:00:00", "Dagelijks"),
            (6, 6, "2024-10-21 15:00:00", "2024-10-21 19:00:00", "Dagelijks"),
            (7, 7, "2024-10-22 16:00:00", "2024-10-22 20:00:00", "Dagelijks"),
            (8, 8, "2024-10-23 17:00:00", "2024-10-23 21:00:00", "Dagelijks"),
            (9, 9, "2024-10-24 18:00:00", "2024-10-24 22:00:00", "Dagelijks"),
            (10, 10, "2024-10-25 19:00:00", "2024-10-25 23:00:00", "Dagelijks")
        ]
    }

    db_service.create_tables()
    db_service.insert_data(insert_values)

    db_service.display_tables()

    db_service.close_connection()

if __name__ == "__main__":
    main()
