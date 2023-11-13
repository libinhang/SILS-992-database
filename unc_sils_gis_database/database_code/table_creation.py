import psycopg2


db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': '123456',
    'host': 'localhost',
    'port': '5432'
}

drop_tables_commands = [
    "DROP TABLE IF EXISTS pollutant_emissions;",
    "DROP TABLE IF EXISTS toxic_release_inventory;",
    "DROP TABLE IF EXISTS biodiversity;",
    "DROP TABLE IF EXISTS climate;",
    "DROP TABLE IF EXISTS air_quality;",
    "DROP TABLE IF EXISTS counties;"
]

create_tables_commands = [
    """
    CREATE TABLE IF NOT EXISTS counties (
        county_code INT PRIMARY KEY,
        county_name VARCHAR(255),
        latitude DECIMAL(10, 8),
        longitude DECIMAL(11, 8),
        state_code VARCHAR(2)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS air_quality (
        site_num VARCHAR(255),
        latitude DECIMAL(9, 6),
        longitude DECIMAL(9, 6),
        datum VARCHAR(10),
        parameter_name VARCHAR(255),
        sample_duration VARCHAR(255),
        pollutant_standard VARCHAR(255),
        date_local DATE,
        units_of_measure VARCHAR(255),
        event_type VARCHAR(50),
        observation_count INT,
        observation_percent DECIMAL(5, 2),
        arithmetic_mean DECIMAL(10, 3),
        max_value DECIMAL(10, 3),
        max_hour INT,
        aqi INT,
        local_site_name VARCHAR(255),
        address TEXT,
        county VARCHAR(255),
        city_name VARCHAR(255),
        geoid VARCHAR(5),
        PRIMARY KEY (site_num, date_local, parameter_name)
    )
    """,
        """
    CREATE TABLE IF NOT EXISTS climate (
        climate_id SERIAL PRIMARY KEY,
        facility_id VARCHAR(11),
        year INTEGER,
        month INTEGER,
        day INTEGER,
        element VARCHAR(255),
        value DECIMAL(10, 2),
        mflag VARCHAR(31),
        qflag VARCHAR(31),
        sflag VARCHAR(31),
        county_code INT,
        FOREIGN KEY (county_code) REFERENCES counties(county_code)
    )
    """,
        """
    CREATE TABLE IF NOT EXISTS biodiversity (
        gbif_id BIGINT PRIMARY KEY,
        dataset_key VARCHAR(255),
        occurrence_id VARCHAR(255),
        kingdom VARCHAR(255),
        phylum VARCHAR(255),
        class VARCHAR(255),
        orders VARCHAR(255),
        family VARCHAR(255),
        genus VARCHAR(255),
        species VARCHAR(255),
        infraspecific_epithet VARCHAR(255),
        taxon_rank VARCHAR(255),
        scientific_name VARCHAR(255),
        verbatim_scientific_name VARCHAR(255),
        locality VARCHAR(255),
        occurrence_status VARCHAR(255),
        individual_count INT,
        publishing_org_key VARCHAR(255),
        decimal_latitude DECIMAL(10, 8),
        decimal_longitude DECIMAL(11, 8),
        coordinate_uncertainty_in_meters INT,
        event_date DATE,
        taxon_key BIGINT,
        species_key BIGINT,
        basis_of_record VARCHAR(255),
        collection_code VARCHAR(255),
        catalog_number VARCHAR(255),
        license VARCHAR(255),
        last_interpreted TIMESTAMP,
        media_type VARCHAR(255),
        issue VARCHAR(255),
        county_code INT,
        FOREIGN KEY (county_code) REFERENCES counties(county_code)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS toxic_release_inventory (
        tri_facility_id VARCHAR(255),
        chem_name VARCHAR(1000),
        latitude DECIMAL(10, 8),
        longitude DECIMAL(11, 8),
        reporting_year INTEGER, 
        elemental_metal_included VARCHAR(255),
        rel_est_amt DECIMAL(20, 2),
        carcinogen VARCHAR(255),
        pbt_ind VARCHAR(255),
        pfas_ind VARCHAR(255),
        county_code INT,
        discharge_to VARCHAR(255),
        PRIMARY KEY (tri_facility_id, chem_name,reporting_year),
        FOREIGN KEY (county_code) REFERENCES counties(county_code)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pollutant_emissions (
        site_name VARCHAR(255),
        county_code INT,
        pollutant VARCHAR(255),
        pollutant_type VARCHAR(255),
        emissions_tons DECIMAL(18, 10),
        latitude DECIMAL(10, 8),
        longitude DECIMAL(11, 8),
        facility_type VARCHAR(255),
        naics VARCHAR(255),
        PRIMARY KEY (site_name, pollutant),
        FOREIGN KEY (county_code) REFERENCES counties(county_code)
    )
"""
]

# Connect to the PostgreSQL database and create tables
try:
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cursor:
            for command in drop_tables_commands:
                cursor.execute(command)
            for command in create_tables_commands:
                cursor.execute(command)
            conn.commit()  
    print("Tables dropped and created successfully")
except psycopg2.Error as e:
    print(f"An error occurred: {e}")