// Copyright 2022 Blues Inc.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"bytes"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Upload stats to S3
func s3UploadStats(filename string, contents []byte) (err error) {

	var sess *session.Session
	sess, err = session.NewSession(
		&aws.Config{
			Region: aws.String(Config.AWSRegion),
			Credentials: credentials.NewStaticCredentials(
				Config.AWSAccessKeyID,
				Config.AWSAccessKey,
				"",
			),
		})
	if err != nil {
		return
	}

	uploader := s3manager.NewUploader(sess)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(Config.AWSBucket),
		ACL:    aws.String("public-read"),
		Key:    aws.String(filename),
		Body:   bytes.NewReader(contents),
	})

	return
}
