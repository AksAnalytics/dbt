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
TXT_FILE_LOCATION = 'dw_procedures.txt'

# Change this to where you want the files to be dumped, otherise they will be dumped in the current working directory
# WRITE_LOCATION = r'/Users/williamdst/Documents/EON-Files/SBD-DBT-DEMO/dbt-data-marting-framework/umm/models/onboarding/stage'

def main() -> None:
    """ Generates a collection of crt_<table_name>.sql files that can be executed in the dbt project"""

    # Read the contents of the file.
    with open(TXT_FILE_LOCATION, "r") as f:
        ddl_statements = f.read()
        assert ddl_statements, "File has no contents"

    sources_yml = sorted(set(re.findall(r'\w*\..*\_current', ddl_statements)))
    sources_yml_no_current = [source.replace('_current', '') for source in sources_yml]
    schema_names = sorted(set([table_name.split('.')[0] for table_name in sources_yml_no_current if re.match(r"\w*\.\w*", table_name)]))
    print(schema_names)

    # BREAK THE PROCEDURES INTO DIFFEERENT FILES
    pattern = r"\n\$\$\n;"
    start_extract = 0
    i = 0
    for match in re.finditer(pattern, ddl_statements):
        procedure_definition = ddl_statements[start_extract: match.end()]
        procedure_name = re.findall(r'CREATE OR REPLACE PROCEDURE (\w*\.\w*)', procedure_definition)[0]
        procedure_name = re.sub(r'\.', r'__', procedure_name)

        raw_sources_list = list(set(re.findall(r'\w*\..*\_current', procedure_definition)))
        no_current_sources = [raw.replace('_current', '') for raw in raw_sources_list]

        # DIRECTLY REPLACE THE SOURCES
        schema_table_split = [no_current.split('.') for no_current in no_current_sources]
        dbt_sources_list = [f"{{{{ source('{split[0]}', '{split[1]}') }}}}" for split in schema_table_split]
        assert len(raw_sources_list) == len(dbt_sources_list), "Lists are not the same length"
        for raw, dbt in zip(raw_sources_list, dbt_sources_list):
            procedure_definition = procedure_definition.replace(raw, dbt)

        """ # IF YOU DON'T WANT THE SOURCING
        assert len(raw_sources_list) == len(no_current_sources), "Lists are not the same length"
        for raw, no_source in zip(raw_sources_list, no_current_sources):
            procedure_definition = procedure_definition.replace(raw, no_source)
        """

        """
        os.chdir(WRITE_LOCATION)
        file = open(f'proc_{procedure_name.strip()}.sql', 'w')
        file.write(procedure_definition)
        file.close()
        """

        start_extract = match.end()+1

    return

if __name__ == '__main__':
    main()