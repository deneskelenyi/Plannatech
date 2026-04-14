import argparse
import sys

import mysql.connector

import config as app_config


ap = argparse.ArgumentParser()
ap.add_argument("-test", "--test", required=False, help="Send to test database: 0 or 1, default is 0")
ap.add_argument("-op", "--op", required=False, help="Send to test database: 0 or 1, default is 0")
args = vars(ap.parse_args())
isTest = 0
if args["test"] is not None:
    try:
        isTest = int(args["test"])
    except Exception:
        print("Error -test parameter must be 0 or 1")
        sys.exit()
if isTest not in [0, 1]:
    print("Error -test parameter must be 0 or 1")
    sys.exit()


def reconnect(i):
    try:
        if i == 1:
            mydb = app_config.create_mysql_pool("plannatech_pool", pool_size=5, test_mode=(isTest == 1))
        else:
            mydb = app_config.create_mysql_connection(test_mode=(isTest == 1))
    except mysql.connector.Error as error:
        print("parameterized query failed {}".format(error))
        print("Mysql connect failed")
        sys.exit(2)
    return mydb


mydb = reconnect(1)
mydb2 = reconnect(2)
