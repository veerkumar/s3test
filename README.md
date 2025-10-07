# Datadobi S3 Test Suite

This repository contains a suite of S3 client tests in the form of JUnit tests.
These tests are used by [Datadobi](https://www.datadobi.com) to test edge case behaviour of S3 server implementations.

## Specifying Target S3 Servers

The test suite is expected to be run against an empty bucket on an S3 server.
The target S3 bucket can be specified using an `http`, `https`, or `s3profile` URI.

`http` and `https` URIs are expected to follow the pattern `http[s]://[<access_key_id>:<secret_access_key>@]<endpoint>[:<port>][/<bucket_name>]`.

`s3profile` URIs are of the form `s3profile://<profile_name>[/<bucket_name>]`.
The profile name refers to a profile that is read from the AWS CLI configuration files location in the `.aws` directory in your home directory.

The target URI can be passed to the tests either via the command line (see below) or by setting it as the value of the `S3TEST_URI` environment variable.

If the `bucket_name` is omitted from the target URI, each test will create and destroy a new bucket for each test case.
This slows down test execution, but improves test isolation.

## Running Tests

### Test Runner

This project contains a simple wrapper test runner in the `com.datadobi.s3test.RunTests` class.
This can be launched either from an IDE or by using `gradlew run`.

Individual test cases can be explictly included or excluded using the `-i`/`--include` and `-e`/`--exclude` command line arguments.

### Running from an IDE

Since each test is a JUnit test case, tests can be easily executed from your IDE of choice.
Instructions on how to run JUnit tests from IDEs is out of scope for this README.