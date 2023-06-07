package s3io

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"strings"

	"filippo.io/age"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var downloadable = map[string]bool{
	"":                                 true,
	string(types.StorageClassStandard): true,
	string(types.StorageClassReducedRedundancy): true,
	string(types.StorageClassStandardIa):        true,
	string(types.StorageClassOnezoneIa):         true,
}

func (cl *client) checkDownloadable(key string) error {
	hoo, err := cl.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: cl.bucket,
		Key:    aws.String(key),
	})
	if err != nil {
		var nosuchkey *types.NoSuchKey
		if errors.As(err, &nosuchkey) {
			return &ErrNoSuchObject{
				key: key,
			}
		}
		return err
	}

	sclass := string(hoo.StorageClass)
	if downloadable[sclass] {
		return nil
	}

	return &ErrNotDownloadable{
		key:          key,
		storageClass: sclass,
	}
}

func (cl *client) Download(key string, sink io.Writer) (int64, error) {

	// verify we can download the object
	err := cl.checkDownloadable(key)
	if err != nil {
		return 0, err
	}

	// use the simple GetObject method as we won't have a io.WriterAt interface
	//   to use the manager/paraller downloader
	resp, err := cl.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: cl.bucket,
		Key:    aws.String(key),
	})
	if err != nil {
		var nosuchkey *types.NoSuchKey
		if errors.As(err, &nosuchkey) {
			return 0, &ErrNoSuchObject{
				key: key,
			}
		}
		return 0, err
	}
	defer resp.Body.Close()

	var reader io.Reader = resp.Body

	// check the meta data to see if decompressing/decryption is needed
	compressed := false
	encrypted := false
	passkey := ""

	meta := resp.Metadata
	for k, v := range meta {
		if "s3bu-compress" == strings.ToLower(k) {
			compressed = true
		}
		if "s3bu-encrypt" == strings.ToLower(k) {
			encrypted = true
		}
		if "s3bu-scrypt-id" == strings.ToLower(k) {
			passkey = v
		}
	}

	// decrypt first
	if encrypted && passkey == "" {
		if len(cl.identities) == 0 {
			return 0, &ErrIdentitiesNotFound{}
		}

		dreader, err := age.Decrypt(reader, cl.identities...)
		if err != nil {
			return 0, err
		}

		reader = dreader
	}

	if len(passkey) > 0 {
		// get the passphrase to decrypt with
		passphrase, ok := cl.passphrases[passkey]
		if ok == false {
			return 0, &ErrPassphraseNotFound{
				operation: "download",
			}
		}

		identity, err := age.NewScryptIdentity(passphrase)
		if err != nil {
			return 0, err
		}
		dreader, err := age.Decrypt(reader, identity)
		if err != nil {
			return 0, err
		}

		reader = dreader
	}

	// then decompress
	if compressed {
		gzreader, err := gzip.NewReader(reader)
		if err != nil {
			return 0, err
		}
		defer gzreader.Close()

		reader = gzreader
	}

	nbytes, err := io.Copy(sink, reader)
	if err != nil {
		return 0, err
	}

	return nbytes, nil
}
