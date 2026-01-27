# Bi-Solutions Data Integration Project

This project demonstrates a data integration pipeline, transforming raw (bronze) data into a cleaned (silver) layer, and then into a structured (gold) star schema within a PostgreSQL database.

## Project Structure

*   `bronze/`: Raw, untransformed data (CSV, JSON).
*   `silver/`: Cleaned and processed data (CSV).
*   `gold/`: SQL scripts for star schema definition.
*   `get_demographics_csv.py`: Script to fetch demographic data.
*   `process_to_silver.py`: Script to transform bronze data to silver.
*   `docker-compose.yml`: Docker Compose file to run the PostgreSQL database.
*   `create_gold_tables.py`: Script to create the star schema tables in PostgreSQL.
*   `silver_to_gold.py`: Script to load data from the silver layer into the gold star schema.

## Setup

### 1. Python Environment

It's highly recommended to use a Python virtual environment.

```bash
# Create a virtual environment (if you haven't already)
python -m venv venv

# Activate the virtual environment
# On Windows:
.\venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install required Python packages
pip install pandas psycopg2-binary sqlalchemy
```

### 2. Bronze Layer Data Acquisition

Place any provided raw data files into the `bronze/` directory as appropriate.
To fetch demographic data from an API:

```bash
python get_demographics_csv.py
```

### 3. Silver Layer Transformation

This step processes the raw data from the `bronze/` layer, cleans it, and stores it in the `silver/` directory.

```bash
python process_to_silver.py
```

### 4. Gold Layer - PostgreSQL Database Setup (Star Schema)

This layer involves setting up a PostgreSQL database using Docker and defining a star schema.

#### a. Start PostgreSQL Database with Docker Compose

Ensure Docker is running on your system.
The `docker-compose.yml` file defines a PostgreSQL service.

```bash
# From the project root directory, start the database in detached mode
docker-compose up -d
```

You can verify the container is running with `docker ps`.

#### b. Create Star Schema Tables

The `create_gold_tables.py` script will connect to the running PostgreSQL database and create the dimension and fact tables as defined in the SQL scripts within the `gold/` directory.

```bash
python create_gold_tables.py
```

The database connection details are:
*   **Host**: `localhost`
*   **Port**: `5432`
*   **Database**: `gold_db`
*   **Username**: `gold_user`
*   **Password**: `gold_password`

You can use tools like `pgAdmin` or `DBeaver` to connect to and inspect the database.

#### c. Populate Gold Tables

The `silver_to_gold.py` script loads the processed data from the `silver/` directory into the newly created star schema tables in the PostgreSQL database. This script handles data transformation, mapping business keys to surrogate keys, and populating dimension and fact tables.

```bash
python silver_to_gold.py
```