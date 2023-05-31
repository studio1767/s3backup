package s3io

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (cl *client) LatestMatching(prefix string) (string, int64, error) {

	loi := s3.ListObjectsV2Input{
		Bucket: cl.bucket,
		Prefix: aws.String(prefix),
	}

	for {
		resp, err := cl.client.ListObjectsV2(context.Background(), &loi)
		if err != nil {
			return "", 0, err
		}
		if resp.IsTruncated == true {
			loi.ContinuationToken = resp.NextContinuationToken
			continue
		}

		num := len(resp.Contents)
		if num == 0 {
			break
		}
		object := resp.Contents[num-1]

		return aws.ToString(object.Key), object.Size, nil
	}

	return "", 0, &ErrNoMatch{
		msg: fmt.Sprintf("No objects found with prefix: %s", prefix),
	}
}
