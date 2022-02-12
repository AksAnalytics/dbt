""" EON_DATA_MART_DUMPER

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

import sys
import logging
applicationName='EON_DATA_MART_DUMPER.py'
# create logger
logger = logging.getLogger(applicationName)
logging.basicConfig(level=logging.DEBUG)
logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

import re
import os
import argparse
import time


WRITE_LOCATION = os.getcwd()
TXT_FILE_LOCATION = 'global_pl_stage_tables.txt'

# Change this to where you want the files to be dumped, otherwise they will be dumped in the current working directory
# WRITE_LOCATION = r''

def dump_crt_models(crt_source_file, crt_model_output_location=None):
    """ Generates a collection of crt_<table_name>.sql files that can be executed in the dbt project"""
    processStageName="Generating collection of CRT models with source file set as %s and output location set as %s"%(crt_source_file, crt_model_output_location)
    logger.info("%s start    %s" %(time.strftime("%Y%m%dT%T"),processStageName))
    try:
        # Read the contents of the file.
        processStageName="Extract the CREATE TABLE IF NOT EXISTS <table_name>."
        logger.info("%s start    %s" %(time.strftime("%Y%m%dT%T"),processStageName))
        with open(crt_source_file, "r") as f:
            ddl_statements = f.read()
            # logger.info("Just read the file contents: \n%s" %(ddl_statements))
            assert ddl_statements, "File has no contents"
        logger.info("%s complete %s" %(time.strftime("%Y%m%dT%T"),processStageName))
        

        # Format the contents of the file
        processStageName="Format the contents of the file"
        logger.info("%s start    %s" %(time.strftime("%Y%m%dT%T"),processStageName))
        ddl_statements = re.sub(r'--.*', '', ddl_statements)    # Comment Remover
        ddl_statements = re.sub(r'ENCODE (lzo|az64|raw|bytedict|delta|delta32K|mostly8|mostly16|mostly32|runlength|text255|text32K|zstd)',
                            '', ddl_statements, flags=re.IGNORECASE)  # Redshift Encode Replacement
        ddl_statements = re.sub(r'DISTSTYLE (\w|\t|\n|\(|\)| |\.)*;', '', ddl_statements) # DISTTYLE Replacement
        ddl_statements = re.sub(r'ALTER TABLE (\w|\t|\n|\(|\)|\s|,|\.)*;', '', ddl_statements) # ALTER TABLE Replacement
        logger.info("%s complete %s" %(time.strftime("%Y%m%dT%T"),processStageName))

        # Extract the CREATE TABLE IF NOT EXISTS <table_name> or CREATE TABLE <table_name>
        processStageName="Extract the CREATE TABLE IF NOT EXISTS <table_name> or CREATE TABLE <table_name>"
        logger.info("%s start    %s" %(time.strftime("%Y%m%dT%T"),processStageName))
        create_table_statements = re.findall(r"CREATE TABLE (?:\w|\d|\.| )*", ddl_statements)
        logger.debug("Print create table stmnt: \n%s"%(create_table_statements))
        logger.info("%s complete %s" %(time.strftime("%Y%m%dT%T"),processStageName))
       

        # Extract the <table_name> without the source system prefix.
        processStageName="Extract the <table_name> without the source system prefix."
        logger.info("%s start    %s" %(time.strftime("%Y%m%dT%T"),processStageName))
        table_group = [table.split('CREATE TABLE ') for table in create_table_statements]
        table_names = [table_name[1] for table_name in table_group]
        table_names = [table_name.replace('.', '_').replace('IF NOT EXISTS ', '') for table_name in table_names]
        logger.debug("Print table names: \n%s"%(table_names))
        logger.info("%s complete %s" %(time.strftime("%Y%m%dT%T"),processStageName))
        

        # Extract the compete CREATE TABLE [IF NOT EXISTS] <table_name> (<table definition>)
        processStageName="Extract the compete CREATE TABLE [IF NOT EXISTS] <table_name> (<table definition>)"
        logger.info("%s start    %s" %(time.strftime("%Y%m%dT%T"),processStageName))
        complete_extract = re.findall(r'CREATE TABLE (?:\w|\s|\.|\n|\(|,|(?<=\d)\)|\"|(?<=\w)\))+\)', ddl_statements)
        complete_extract = [table_ddl.replace('"', '') for table_ddl in complete_extract]   # Remove quotes around column names
        assert len(complete_extract) == len(table_names), "Lists are not the same length"
        logger.info("%s complete %s" %(time.strftime("%Y%m%dT%T"),processStageName))
        
        # Use the extracts to format the file contents and write to the file.
        processStageName="Use the extracts to format the file contents and write to the file."
        logger.info("%s start    %s" %(time.strftime("%Y%m%dT%T"),processStageName))
        for table_ddl, table_name in zip(complete_extract, table_names):
            file_body = '{{% set table_metadata = {{ \n\t ' \
                        '"table_definition":" \n\t\t' \
                        '{} \n\t' \
                        '"\n' \
                        '}}%}} \n\n' \
                        '{{{{ config(materialized = "ephemeral") }}}}\n' \
                        '{{% do run_query(table_metadata.table_definition) %}}'.format(table_ddl)

            ## check if the crt_model_output_location is provided from user/calling program. otherwise, use default which is Current working directory
            if crt_model_output_location is None:
                crt_model_output_location = os.getcwd()
                logger.info("Using default output directory as  %s"%(crt_model_output_location))
            
            os.chdir(crt_model_output_location)
            file = open(f'crt_{table_name}.sql', 'w')
            file.write(file_body)
            file.close()
            logger.info("%s complete %s" %(time.strftime("%Y%m%dT%T"),processStageName))
    except Exception as error_message:
        errorMessage="%s Error during %s stage while calling the EON_DATA_MART_DUMPER.dump_crt_models(%s) function with rootCause: %s " % (time.strftime("%Y%m%dT%T"), processStageName,crt_source_file,str(error_message))
        print(
            'Exception occured with message: {0}'.format(error_message)) ## log Message to console incase logger is not available on system
        logger.error(errorMessage) ## log the formatted error message
        raise Exception(errorMessage)  ## throw the exception with appropriate message to caller program/method
    else: 
        return

    

# Print an Error message on program entry if incorrect number of arguments is entered
def printArgsErrorMessage():
        args = sys.argv[1:]
        result = ''

        for arg in args:
                result += " " + arg
        print("Argument list: " + result)

###### EON_DATA_MART_DUMPER - Program driver/ main
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Not enough arguments yo. Arguments provided: " + str(len(sys.argv)) + ". Exiting.")
        printArgsErrorMessage()
        exit(1)
    try:
        parser = argparse.ArgumentParser(prog='EON_DATA_MART_DUMPER',description='EON Collective Data Mart Dumper. copyright © 2022 EON Collective')
        parser.add_argument('--version', action='version', version='%(prog)s 1.1-EON-ALPHA')
        parser.add_argument('crt_source_file', help='crt_source file with original create table statements - original dumps')
        parser.add_argument('--output-dir', dest='output_dir', action='store', nargs=1, help='crt models/onboarding files output location. This is an optional field. otherwise program will use current working directory.')
        args = parser.parse_args()

        crt_source_file=args.crt_source_file

        if args.output_dir is not None:
            output_dir=args.output_dir[0]
        else:
            output_dir=args.output_dir
        logger.info( "%s: Starting to process dump file from %s with output location set as %s " % (time.strftime("%Y%m%dT%T"), crt_source_file, output_dir))

        dump_crt_models(crt_source_file, crt_model_output_location=output_dir)
    except Exception as e:
        errorMessage="%s Error during EON_DATA_MART_DUMPER process with rootCause: %s " % (time.strftime("%Y%m%dT%T"), str(e))
        logger.error(errorMessage) ## log the formatted error message
        raise Exception(errorMessage)  ## throw the exception with appropriate message to caller program/method
    finally:
        processStageName="Exiting program "
        logger.info( "%s: Complete - %s " % (time.strftime("%Y%m%dT%T"), processStageName))

## ------- FOR EON COllective INTERNAL USE ONLY: Prior written consent for use is required for AFFILIATES, LICENSORS, SUPPLIERS OR SUBCONTRACTORS. powered by ADEPT. copyright © 2022 EON Collective. --------------- ##