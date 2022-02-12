""" Data Mart Dumper

This program takes in a formatted EON extract of SBD's Redshift Table DDLs and generates crt_ models that can be placed
in the dbt project. The purpose of crt_ models is to simply create the tables in the warehouse so that they exist when
converting the Redshift stored procedures.

Note: Any table that already exists won't be create and any table that is created by a dbt model will be overwritten.
Note: Convert the .sql file to a .txt file by changing the extension before running the script.

Future Updates:
    - Handle quote formatting (possibly , followed by "")
    - Handle IDENTITY data types
    - Create Constraint formatting
"""

import re
import os

WRITE_LOCATION = os.getcwd()
TXT_FILE_LOCATION = 'dw_tables.txt'


# Change this to where you want the files to be dumped, otherise they will be dumped in the current working directory
# WRITE_LOCATION =

def main() -> None:
    """ Generates a collection of crt_<table_name>.sql files that can be executed in the dbt project"""

    # Read the contents of the file.
    with open(TXT_FILE_LOCATION, "r") as f:
        ddl_statements = f.read()
        assert ddl_statements, "File has no contents"

    i = 1
    pattern = re.compile(r'col(?!\d)')
    while len(re.findall(pattern, ddl_statements)):
        ddl_statements = re.sub(pattern, f'col{i}', ddl_statements, 1)
        i += 1
        if i == 48: i = 1

    print(ddl_statements)

    os.chdir(WRITE_LOCATION)
    file = open(f'sp_sbd_dm_trans_monthly_insert.sql', 'w')
    file.write(ddl_statements)
    file.close()
    return


if __name__ == '__main__':
    for i in range(1,48):
        print(f'col{i} AS ,')
