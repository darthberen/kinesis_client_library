/*
Package kcl provides a kinesis client library (KCL) implementation for Amazon Web Services.
The primary goals of this package are to provide a performant Go KCL while keeping the library
idomatic (and readable) and easy to use.

The AWS SDK is configured based on the AWS SDK's default configuration:
http://godoc.org/github.com/awslabs/aws-sdk-go/aws#pkg-constants.  This means
that AWS credentials are loaded using the credentials.NewChainCredentials method.
The quickest way to get started using this package is to set the following
environmental variables: Access Key ID: AWS_ACCESS_KEY_ID or AWS_ACCESS_KEY,
Secret Access Key: AWS_SECRET_ACCESS_KEY or AWS_SECRET_KEY, and region: AWS_REGION.

Please log and issues or feature requests here: https://github.com/suicidejack/kinesis_client_library/issues.
*/
package kcl
