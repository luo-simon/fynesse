from .config import *

import pymysql
import requests
import os
import zipfile
import pandas as pd
import osmnx as ox

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """


class Database:
    def __init__(self, username, password, url, port=3306):
        self.username = username
        self.password = password
        self.url = url
        self.port = port
        self.conn = self.connect()

    def connect(self):
        """
        Create a database connection to the MariaDB server specified by the host url.
        """
        conn = None
        try:
            conn = pymysql.connect(
                user=self.username,
                passwd=self.password,
                host=self.url,
                port=self.port,
                local_infile=1,
                client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS,
            )
            print(f"Successfully connected to server.")
        except Exception as e:
            print(f"Error connecting to the MariaDB Server: {e}")
        return conn

    def list_existing_databases(self):
        return self.execute("SHOW DATABASES")

    def use_database(self, db_name):
        self.execute(f"USE {db_name};")

    def execute(self, sql, verbose=False):
        cur = self.conn.cursor()
        if verbose:
            print(f"Execute: {sql}")
        cur.execute(sql)
        return cur.fetchall()

    def execute_to_df(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [i[0] for i in cur.description]
        return pd.DataFrame(rows, columns=cols)

    def get_processlist(self):
        return self.execute_to_df("SHOW FULL PROCESSLIST")

    def kill_process(self, process_num):
        self.execute(f"KILL {process_num}")

    def create_database(self, db_name="property_prices"):
        self.execute(
            f"""
SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";
CREATE DATABASE IF NOT EXISTS `{db_name}` DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;
USE `{db_name}`;               
"""
        )

    def show_indexes(self, table_name):
        return self.execute_to_df(f"SHOW INDEXES FROM {table_name};")

    def create_index(self, table_name, columns, index_name=None):
        """
        columns: list of column name strings
        """
        if index_name:
            self.execute(
                f"CREATE INDEX {index_name} ON `{table_name}` ({','.join(columns)});",
                True,
            )
        else:
            self.execute(
                f"CREATE INDEX {table_name}_{'_'.join(columns)}_index ON `{table_name}` ({','.join(columns)});",
                True,
            )

    def create_table(self, table_name, create_table_cmd, csv_files, index_columns=[]):
        if input(
            f"Are you sure you want to (re)create table {table_name}? This will overwrite any existing tables with the same name and may take a long time."
        ).lower() not in ["y", "yes"]:
            print("Did not create table.")
            return

        sql = f"""
DROP TABLE IF EXISTS `{table_name}`; {create_table_cmd}
ALTER TABLE `{table_name}`
ADD PRIMARY KEY (`db_id`);

ALTER TABLE `{table_name}`
MODIFY db_id bigint(20) unsigned NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=1;
"""
        self.execute(sql)

        for file in csv_files:
            self.upload_file(table=table_name, file_name=file)

        if len(index_columns) > 0:
            self.create_index(table_name, index_columns)

    def create_pp_data(self):
        create_table_cmd = """
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
) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1;
"""
        csv_files = self.get_pp_data()
        self.create_table(
            table_name="pp_data",
            create_table_cmd=create_table_cmd,
            csv_files=csv_files,
            index_columns=["postcode", "date_of_transfer"],
        )

    def create_postcode_data(self):
        create_table_cmd = """
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
"""

        csv_files = self.get_postcode_data()

        self.create_table(
            table_name="postcode_data",
            create_table_cmd=create_table_cmd,
            csv_files=csv_files,
            index_columns=["postcode", "latitude", "longitude"],
        )

    def create_prices_coordinates_data(self):
        create_table_cmd = """
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
"""
        for year in range(1995, 2023):
            filepath = f"data/prices_coordinates_data_{year}.csv"
            if not os.path.exists(filepath):
                print(f"Joining table for rows in {year}.")
                df = self.execute_to_df(
                    f"""
                SELECT
                price, date_of_transfer, pp_data.postcode, property_type, new_build_flag, tenure_type, locality, town_city, district, county, country, latitude, longitude
                FROM pp_data
                INNER JOIN postcode_data
                ON pp_data.postcode = postcode_data.postcode          
                WHERE 
                    date_of_transfer >= '{year}-01-01' AND
                    date_of_transfer < '{year+1}-01-01'
                """
                )
                df.to_csv(filepath, header=False, index=False)
                print(f"Joined successfully, stored to: {filepath}")
            else:
                print(f"{filepath} exists. Skipping.")

        self.create_table(
            table_name="prices_coordinates_data",
            create_table_cmd=create_table_cmd,
            csv_files=[],
        )

        for year in range(1995, 2023):
            self.upload_file(
                "prices_coordinates_data", f"data/prices_coordinates_data_{year}.csv"
            )

        self.create_index(
            "prices_coordinates_data",
            ["date_of_transfer", "latitude", "longitude"],
            "pcd_date_lat_long_index",
        )

    def get_columns(self, table):
        cols = self.execute(f"SHOW COLUMNS FROM {table}")
        col_names = [c[0] for c in cols]
        return col_names

    def rand_sample(self, table, n=10, seed=None):
        rows = []
        m = self.execute(f"SELECT MAX(db_id) FROM {table}")[0][0]
        return self.execute_to_df(
            f"""
            SELECT DISTINCT *
            FROM {table} as t1
            JOIN (
                SELECT ROUND(RAND({seed if seed else ''}) * {m}) as id
                FROM {table}
                LIMIT {int(n * 1.1)} 
            ) AS t2
            ON
            t1.db_id = t2.id
            LIMIT {n}
            """
        )

    def select_top(self, table, n):
        """
        Query n first rows of the table
        :param conn: the Connection object
        :param table: The table to query
        :param n: Number of rows to query
        """
        return self.execute(f"SELECT * FROM {table} LIMIT {n};")

    def head(self, table, n=5):
        rows = self.select_top(table, n)
        for r in rows:
            print(r)

    def upload_file(self, table, file_name):
        """
        Upload a file to the table
        :param conn: the Connection object
        :param table: The name of the table to upload to
        :param file_name: name of file to upload
        """
        print(f"Uploading {file_name} to {table}")
        cur = self.conn.cursor()
        sql = f"""
LOAD DATA LOCAL INFILE '{file_name}'
INTO TABLE `{table}`
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED by '"'
LINES STARTING BY '' TERMINATED BY '\n';
"""
        cur.execute(sql)
        print(f"Data loaded successfully into table `{table}` from '{file_name}'.")

    def get_file_from_url(self, file_path, url, verbose=False):
        if not os.path.exists(file_path):
            with open(file_path, "wb") as out_file:
                print(f"Downloading file {file_path} from {url}")
                content = requests.get(url, stream=True).content
                out_file.write(content)
        else:
            if verbose:
                print(f"File {file_path} exists. Skipping download.")

    def get_pp_data(self):
        files = []
        # Download parts from 1995 to 2022
        for year in range(1995, 2023):
            for part in [1, 2]:
                file_name = f"pp-{year}-part{part}.csv"
                file_path = f"data/{file_name}"
                url = f"http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/{file_name}"
                self.get_file_from_url(file_path, url)
                files.append(file_path)
        return files

    def get_postcode_data(self):
        self.get_file_from_url(
            file_path="data/open_postcode_geo.csv.zip",
            url="https://www.getthedata.com/downloads/open_postcode_geo.csv.zip",
        )

        if not os.path.exists("data/open_postcode_geo.csv"):
            with zipfile.ZipFile("postcode_data.zip", "r") as zip_ref:
                zip_ref.extractall("./")

        return ["data/open_postcode_geo.csv"]
