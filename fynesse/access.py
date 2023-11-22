import pandas

from .config import *
import pymysql
from pymysql import Connection
import yaml
import pandas as pd


"""These are the types of import we might expect in this file
import httplib2
import oauth2
import tables
import mongodb
import sqlite"""

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with 
outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond 
the legal side also think about the ethical issues around this data. """


def data() -> pandas.DataFrame:
    """Read the data from the web or local file, load to database, do joining by SQL, and return structured format
    such as a data frame.
    Called after connected to the mariadb URL"""

    # Load database configurations
    db_config = config
    with open("credentials.yaml") as f:
        credentials = yaml.safe_load(f)
        username = credentials["username"]
        password = credentials["password"]
    url = db_config["data_url"]
    port = db_config["port"]

    # Create a connection
    conn = create_connection(
        user=username,
        password=password,
        host=url,
        database="property_prices",
        port=port,
    )

    # Create database `property_prices`
    create_database_property_prices(conn)

    # Create table `pp_data`, specify schema and primary key
    setup_pp_data(conn)

    # Load csv data to table `pp_data`
    pp_filenames = [f"pp-{year}.csv" for year in range(2018, 2023)]
    for filename in pp_filenames:
        if os.path.isfile(filename):
            print(f"Loading {filename} to pp_data...")
            upload_csv_to_table(conn, filename, "pp_data")
            print("Load done")
        else:
            print(f">>>File {filename} doesn't exist, skipped.")

    # Look into first 5 rows
    rows = select_top(conn, "pp_data", 5)
    print(">>> First 5 rows of `pp_data`:")
    print_res(rows)
    # Check number of rows == 28'258,161
    # >>>>> May take half an hour to run!!! <<<<<
    # res = count_number_of_rows(conn, "pp_data")
    # print_res(res)

    # Create table `postcode_data`, specify schema and primary key
    setup_postcode_data(conn)

    # Load csv to database `postcode_data` table
    postcode_filename = "open_postcode_geo.csv"
    print(f"Loading {postcode_filename} to postcode_data...")
    upload_csv_to_table(conn, postcode_filename, "postcode_data")
    print("Load done")

    # Look into first 5 rows
    rows = select_top(conn, "postcode_data", 5)
    print(">>> First 5 rows of `postcode_data`:")
    print_res(rows)

    # Print table names
    rows = get_tables(conn)
    print(">>> Table names:")
    print_res(rows)

    # Index postcode_data by postcode
    index_postcode_data(conn)

    # Join pp_data and postcode_data on postcode
    rows = join_pp_pc(conn)
    for i in range(5):
        print(rows[i])

    column_names = [
        "price",
        "date_of_transfer",
        "postcode",
        "property_type",
        "new_build_flag",
        "tenure_type",
        "locality",
        "town_city",
        "district",
        "county",
        "country",
        "latitude",
        "longitude",
    ]
    df = pd.DataFrame(rows, columns=column_names)
    return df


def pandas_join_pp_pc() -> None:
    """Read data from CSV, do the joining with pandas, and store the result df to CSV"""
    pp_columns = [
        "transaction_unique_identifier",
        "price",
        "date_of_transfer",
        "postcode",
        "property_type",
        "new_build_flag",
        "tenure_type",
        "primary_addressable_object_name",
        "secondary_addressable_object_name",
        "street",
        "locality",
        "town_city",
        "district",
        "county",
        "ppd_category_type",
        "record_status",
        "db_id",
    ]
    pp_data = pd.DataFrame(columns=pp_columns)
    for i in range(2018, 2023):
        print(f"Reading pp-{i}.csv")
        df = pd.read_csv(f"pp-{i}.csv", names=pp_columns)
        pp_data = pd.concat([pp_data, df])
    print(pp_data.iloc[range(5)])

    pc_columns = [
        "postcode",
        "status",
        "usertype",
        "easting",
        "northing",
        "positional_quality_indicator",
        "country",
        "latitude",
        "longitude",
        "postcode_no_space",
        "postcode_fixed_width_seven",
        "postcode_fixed_width_eight",
        "postcode_area",
        "postcode_district",
        "postcode_sector",
        "outcode",
        "incode",
        "db_id",
    ]
    print("Reading open_postcode_geo.csv")
    postcode_data = pd.read_csv("open_postcode_geo.csv", names=pc_columns)
    print(postcode_data.iloc[range(5)])

    pcd_columns = [
        "price",
        "date_of_transfer",
        "postcode",
        "property_type",
        "new_build_flag",
        "tenure_type",
        "locality",
        "town_city",
        "district",
        "county",
        "country",
        "latitude",
        "longitude",
    ]
    print("Joining...")
    prices_coordinates_data = pd.merge(
        pp_data, postcode_data, how="left", on="postcode"
    )[pcd_columns]
    print(prices_coordinates_data.iloc[range(5)])

    print("Writing to prices_coordinates_data.csv")
    prices_coordinates_data.to_csv("prices_coordinates_data.csv")

    print("Done")
    return


def print_res(rows: tuple) -> None:
    for r in rows:
        print(r)


def create_connection(user, password, host, database, port=3306) -> Connection:
    """Create a database connection to the MariaDB database
        specified by the host url and database name.
    :param user: username
    :param password: password
    :param host: host url
    :param database: database
    :param port: port number
    :return: Connection object or None
    """
    conn = None
    try:
        conn = pymysql.connect(
            user=user,
            passwd=password,
            host=host,
            port=port,
            local_infile=1,
            db=database,
        )
    except Exception as e:
        print(f"Error connecting to the MariaDB Server: {e}")
    return conn


def create_database_property_prices(conn: Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
        SET time_zone = "+00:00";
        
        CREATE DATABASE IF NOT EXISTS `property_prices` DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;
        
        USE `property_prices`;
    """
    )


def setup_pp_data(conn: Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        USE `property_prices`;
    
        --
        -- Table structure for table `pp_data`
        --
        DROP TABLE IF EXISTS `pp_data`;
        CREATE TABLE IF NOT EXISTS `pp_data` (
          `transaction_unique_identifier` tinytext COLLATE utf8_bin NOT NULL,
          `price` int(10) unsigned NOT NULL,
          `date_of_transfer` date NOT NULL,
          `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
          `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
          `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
          `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
          `primary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
          `secondary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
          `street` tinytext COLLATE utf8_bin NOT NULL,
          `locality` tinytext COLLATE utf8_bin NOT NULL,
          `town_city` tinytext COLLATE utf8_bin NOT NULL,
          `district` tinytext COLLATE utf8_bin NOT NULL,
          `county` tinytext COLLATE utf8_bin NOT NULL,
          `ppd_category_type` varchar(2) COLLATE utf8_bin NOT NULL,
          `record_status` varchar(2) COLLATE utf8_bin NOT NULL,
          `db_id` bigint(20) unsigned NOT NULL
        ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1 ;

        --
        -- Primary key for table `pp_data`
        --
        ALTER TABLE `pp_data`
        ADD PRIMARY KEY (`db_id`);
        
        ALTER TABLE `pp_data`
        MODIFY db_id bigint(20) unsigned NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=1;
    """
    )
    return


def setup_postcode_data(conn: Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        USE `property_prices`;

        --
        -- Table structure for table `postcode_data`
        --
        DROP TABLE IF EXISTS `postcode_data`;
        CREATE TABLE IF NOT EXISTS `postcode_data` (
          `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
          `status` enum('live','terminated') NOT NULL,
          `usertype` enum('small', 'large') NOT NULL,
          `easting` int unsigned,
          `northing` int unsigned,
          `positional_quality_indicator` int NOT NULL,
          `country` enum('England', 'Wales', 'Scotland', 'Northern Ireland', 'Channel Islands', 'Isle of Man') NOT NULL,
          `latitude` decimal(11,8) NOT NULL,
          `longitude` decimal(10,8) NOT NULL,
          `postcode_no_space` tinytext COLLATE utf8_bin NOT NULL,
          `postcode_fixed_width_seven` varchar(7) COLLATE utf8_bin NOT NULL,
          `postcode_fixed_width_eight` varchar(8) COLLATE utf8_bin NOT NULL,
          `postcode_area` varchar(2) COLLATE utf8_bin NOT NULL,
          `postcode_district` varchar(4) COLLATE utf8_bin NOT NULL,
          `postcode_sector` varchar(6) COLLATE utf8_bin NOT NULL,
          `outcode` varchar(4) COLLATE utf8_bin NOT NULL,
          `incode` varchar(3)  COLLATE utf8_bin NOT NULL,
          `db_id` bigint(20) unsigned NOT NULL
        ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
        
        --
        -- Primary key for table `postcode_data`
        --
        ALTER TABLE `postcode_data`
        ADD PRIMARY KEY (`db_id`);
        
        ALTER TABLE `postcode_data`
        MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;
    """
    )
    return


def setup_prices_coordinates_data(conn: Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        USE `property_prices`;
        --
        -- Table structure for table `prices_coordinates_data`
        --
        DROP TABLE IF EXISTS `prices_coordinates_data`;
        CREATE TABLE IF NOT EXISTS `prices_coordinates_data` (
          `price` int(10) unsigned NOT NULL,
          `date_of_transfer` date NOT NULL,
          `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
          `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
          `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
          `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
          `locality` tinytext COLLATE utf8_bin NOT NULL,
          `town_city` tinytext COLLATE utf8_bin NOT NULL,
          `district` tinytext COLLATE utf8_bin NOT NULL,
          `county` tinytext COLLATE utf8_bin NOT NULL,
          `country` enum('England', 'Wales', 'Scotland', 'Northern Ireland', 'Channel Islands', 'Isle of Man') NOT NULL,
          `latitude` decimal(11,8) NOT NULL,
          `longitude` decimal(10,8) NOT NULL,
          `db_id` bigint(20) unsigned NOT NULL
        ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1 ;

        --
        -- Primary key for table `prices_coordinates_data`
        --
        ALTER TABLE `prices_coordinates_data`
        ADD PRIMARY KEY (`db_id`);
        
        ALTER TABLE `prices_coordinates_data`
        MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;
    """
    )
    return


def get_tables(conn: Connection) -> tuple:
    cur = conn.cursor()
    cur.execute(
        """
        USE property_prices;
        SHOW TABLES;
    """
    )
    rows = cur.fetchall()
    return rows


def select_top(conn: Connection, table: str, n: int) -> tuple:
    """
    Query n first rows of the table
    :param conn: the Connection object
    :param table: The table to query
    :param n: Number of rows to query
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table} LIMIT {n} ;")

    rows = cur.fetchall()
    return rows


def upload_csv_to_table(
    conn: Connection, filename: str, table_name: str = "pp_data"
) -> None:
    """
    Upload csv file in Google Colab to table_name in database
    :param conn: the Connection object
    :param filename: the csv filename to be uploaded
    :param table_name: The table to load to
    """
    sql_command = f"""
        LOAD DATA LOCAL INFILE '{filename}' INTO TABLE `{table_name}`
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED by '"'
        LINES STARTING BY '' TERMINATED BY '\n';
    """
    cur = conn.cursor()
    cur.execute(sql_command)


def count_number_of_rows(conn: Connection, table: str) -> tuple:
    """
    Query number of rows of the table
    :param conn: the Connection object
    :param table: The table to query
    """
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(db_id) as rows_num FROM {table};")

    rows = cur.fetchall()
    return rows


def index_postcode_data(conn: Connection) -> None:
    cur = conn.cursor()
    cur.execute("""CREATE INDEX PCIndex ON postcode_data (postcode);""")
    return


def join_pp_pc(conn: Connection) -> tuple:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT pp.price, pp.date_of_transfer, pp.postcode, pp.property_type, 
        pp.new_build_flag, pp.tenure_type, pp.locality, pp.town_city, pp.district, 
        pp.county, pc.country, pc.latitude, pc.longitude
        FROM pp_data AS pp
        LEFT JOIN postcode_data AS pc
        ON pc.postcode=pp.postcode ;
    """
    )
    rows = cur.fetchall()
    return rows
