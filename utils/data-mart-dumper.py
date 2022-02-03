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
TXT_FILE_LOCATION = 'global_pl_stage_tables.txt'

# Change this to where you want the files to be dumped, otherise they will be dumped in the current working directory
# WRITE_LOCATION = r''

def main() -> None:
    """ Generates a collection of crt_<table_name>.sql files that can be executed in the dbt project"""

    # Read the contents of the file.
    with open(TXT_FILE_LOCATION, "r") as f:
        ddl_statements = f.read()
        assert ddl_statements, "File has no contents"

    # Format the contents of the file
    ddl_statements = re.sub(r'--.*', '', ddl_statements)    # Comment Remover
    ddl_statements = re.sub(r'ENCODE (lzo|az64|raw|bytedict|delta|delta32K|mostly8|mostly16|mostly32|runlength|text255|text32K|zstd)',
                            '', ddl_statements, flags=re.IGNORECASE)  # Redshift Encode Replacement
    ddl_statements = re.sub(r'DISTSTYLE (\w|\t|\n|\(|\)|\s|,)*;\nALTER TABLE .*;', '', ddl_statements) # DISTTYLE Replacement

    # Extract the CREATE TABLE IF NOT EXISTS <table_name>.
    create_table_statements = re.findall(r"CREATE TABLE IF NOT EXISTS.*", ddl_statements)

    # Extract the <table_name> without the source system prefix.
    table_names = [table_name.split('CREATE TABLE IF NOT EXISTS')[1][1:] for table_name in create_table_statements]
    table_names = [table_name.replace('.', '_') for table_name in table_names]

    # Extract the definition of the table.
    table_definitions = re.split(r"CREATE TABLE IF NOT EXISTS .*", ddl_statements)[1:]

    assert len(create_table_statements) == len(table_definitions) == len(table_names), \
        "Lists are not the same length"


    # Use the extracts to format the file contents and write to the file.
    for create, definition, file_name in zip(create_table_statements, table_definitions, table_names):
        table_definition = f'{create}{definition}'.strip()

        file_body = '{{% set table_metadata = {{ \n\t ' \
                    '"table_definition":" \n\t\t' \
                    '{} \n\t' \
                    '"\n' \
                    '}}%}} \n\n' \
                    '{{{{ config(materialized = "ephemeral") }}}}\n' \
                    '{{% do run_query(table_metadata.table_definition) %}}'.format(table_definition)

        os.chdir(WRITE_LOCATION)
        file = open(f'crt_{file_name}.sql', 'w')
        file.write(file_body)
        file.close()

    return


if __name__ == '__main__':
    main()