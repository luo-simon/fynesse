from .config import *

"""These are the types of import we might expect in this file
import httplib2
import oauth2
import tables
import mongodb
import sqlite"""

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """

def data():
    """Read the data from the web or local file, returning structured format such as a data frame"""
    raise NotImplementedError

from .config import *

"""These are the types of import we might expect in this file
import httplib2
import oauth2
import tables
import mongodb
import sqlite"""
import pymysql

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """

def data():
    """Read the data from the web or local file, returning structured format such as a data frame"""
    raise NotImplementedError

# Download each 'data part' from the gov.uk site.
# Approx. 5 minutes to download the whole dataset.
import requests
import os

class Database():
    def __init__(self, username, password, url, port=3306):
       self.username = username
       self.password = password
       self.url = url
       self.port = port
       self.conn = self.connect(user=username, password=password, host=url, database=None, port=port)

    def connect(user, password, host, database, port):
        """
        Create a database connection to the MariaDB database specified by the host url and database name.
        :param user: username
        :param password: password
        :param host: host url
        :param database: database
        :param port: port number
        :return: Connection object or None
        """
        conn = None
        try:
            conn = pymysql.connect(user=user,
                                passwd=password,
                                host=host,
                                port=port,
                                local_infile=1,
                                db=database
                                )
        except Exception as e:
            print(f"Error connecting to the MariaDB Server: {e}")
        return conn

   
def get_file_from_url(file_name, url):
    if not os.path.exists(file_name):
      with open(file_name, 'wb') as out_file:
        print(f"Downloading file {file_name} from {url}")
        content = requests.get(url, stream=True).content
        out_file.write(content)
    else:
      print(f"File {file_name} exists. Skipping download.")
    
# Download parts from 1995 to 2022
for year in range(1995, 2023):
  for part in [1, 2]:
    file_name = f"pp-{year}-part{part}.csv"
    url = f"http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/{file_name}"
    get_file_from_url(file_name, url)



