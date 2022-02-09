# EON_DATA_MART_DUMPER.py #

This README would normally document whatever steps are necessary to get your application up and running.

### What is this repository for? ###

* Quick summary

Document the Data Mart Dumper Accelerator

* Version
* [Learn Markdown](https://bitbucket.org/tutorials/markdowndemo)

### How do I get set up? ###

* Summary of set up
* Configuration

The application takes two arguments. One mandatory and one optional one

`crt_source_file` (required) - Determines the file to be used to generate the crt models
`--output-dir` (optional) - Determines the output location for the crt model file

## How to run the program

Running with default output directory = `cwd`

```
python3 eon_data_mart_dumper.py dw_tables.txt
```

Running with user provided output directory

```
python3 eon_data_mart_dumper.py dw_tables.txt --output-dir /User/Shared/out/
```


* Dependencies
* Database configuration
* How to run tests
* Deployment instructions

### Contribution guidelines ###

* Writing tests
* Code review
* Other guidelines

### Who do I talk to? ###

* Repo owner or admin
* Other community or team contact