package s3io

import (
	"context"
	"errors"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (cl *client) Exists(key string) (bool, error) {

	_, err := cl.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: cl.bucket,
		Key:    aws.String(key),
	})
	if err == nil {
		return true, nil
	}

	var responseError *awshttp.ResponseError
	if errors.As(err, &responseError) && responseError.ResponseError.HTTPStatusCode() == http.StatusNotFound {
		return false, nil
	}

	return false, err
}
