package s3io

import (
	"compress/gzip"
	"context"
	"io"

	"filippo.io/age"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (cl *client) Upload(key string, source io.Reader) (int64, error) {

	compress := false
	encrypt := false
	scrypt := false

	return cl.upload(key, source, compress, encrypt, scrypt)
}

func (cl *client) UploadCompressed(key string, source io.Reader) (int64, error) {

	compress := true
	encrypt := false
	scrypt := false

	return cl.upload(key, source, compress, encrypt, scrypt)
}

func (cl *client) UploadEncrypted(key string, source io.Reader, compress bool) (int64, error) {

	encrypt := true
	scrypt := false

	return cl.upload(key, source, compress, encrypt, scrypt)
}

func (cl *client) UploadPassphrase(key string, source io.Reader, compress bool) (int64, error) {

	if len(cl.passkeys) == 0 {
		return 0, &ErrPassphraseNotFound{
			operation: "upload",
		}
	}

	encrypt := false
	scrypt := true

	return cl.upload(key, source, compress, encrypt, scrypt)
}

func (cl *client) upload(key string, source io.Reader, compress, encrypt, scrypt bool) (int64, error) {

	// create the map for metadata
	mdata := make(map[string]string)

	// insert the compressor - it's a writer but we need a reader
	//   so use an io.Pipe with goroutine
	if compress {
		mdata["s3bu-compress"] = "gzip"
		mdata["s3bu-compress-version"] = "001"

		reader, writer := io.Pipe()
		defer reader.Close()

		go func(writer *io.PipeWriter, source io.Reader) {
			gzwriter := gzip.NewWriter(writer)

			_, err := io.Copy(gzwriter, source)

			gzwriter.Close()
			if err != nil {
				writer.CloseWithError(err)
			} else {
				writer.Close()
			}

		}(writer, source)

		source = reader
	}

	// insert the encrypter
	if encrypt && !scrypt {
		mdata["s3bu-encrypt"] = "age"
		mdata["s3bu-encrypt-version"] = "001"

		reader, writer := io.Pipe()
		defer reader.Close()

		go func(writer *io.PipeWriter, source io.Reader) {
			ewriter, err := age.Encrypt(writer, cl.recipients...)
			if err != nil {
				writer.CloseWithError(err)
				return
			}

			_, err = io.Copy(ewriter, source)

			ewriter.Close()
			if err != nil {
				writer.CloseWithError(err)
			} else {
				writer.Close()
			}

		}(writer, source)

		source = reader
	}

	// insert passphrase encryption
	if scrypt {
		passkey := cl.passkeys[len(cl.passkeys)-1]
		passphrase := cl.passphrases[passkey]

		recipient, err := age.NewScryptRecipient(passphrase)
		if err != nil {
			return 0, err
		}

		mdata["s3bu-scrypt"] = "age"
		mdata["s3bu-scrypt-version"] = "001"
		mdata["s3bu-scrypt-id"] = passkey

		reader, writer := io.Pipe()
		defer reader.Close()

		go func(writer *io.PipeWriter, source io.Reader, recipient age.Recipient) {
			ewriter, err := age.Encrypt(writer, recipient)
			if err != nil {
				writer.CloseWithError(err)
				return
			}

			_, err = io.Copy(ewriter, source)

			ewriter.Close()
			if err != nil {
				writer.CloseWithError(err)
			} else {
				writer.Close()
			}

		}(writer, source, recipient)

		source = reader
	}

	// count how many bytes actually get uploaded after compression
	//   and encryption
	counter := NewReadCounter(source)
	defer counter.Close()

	// can't use the simple PutObject method because don't know the ContentLength
	// in advance so use an Uploader...

	ctx := context.Background()

	uploader := manager.NewUploader(cl.client)

	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:   cl.bucket,
		Key:      aws.String(key),
		Body:     counter,
		Metadata: mdata,
	})

	return counter.TotalBytes(), err
}
