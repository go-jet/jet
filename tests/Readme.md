
# Integration tests

This folder contains integration tests intended to test jet generator, statements and query result mapping with a running database.

## How to run tests?

Before we can run tests, we need to set up and initialize test databases.
To simplify the process there is a Makefile with a list of helper commands.
```shell
# We first need to checkout testdata from separate repository into git submodule,
# then download docker image for each of the databases listed in docker-compose.yaml file, and 
# finally run and initialize databases with downloaded test data.
# Note that on the first run this command might take a couple of minutes.
make setup

# When databases are ready, we can generate sql builder and model types for each of the test databases
make jet-gen-all
```

Then we can run the tests the usual way:
```shell
go test -v ./...
```
