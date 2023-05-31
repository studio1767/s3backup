package s3io

import (
	"bufio"
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

	// the compressors and encryptors need a writer but the aws uploader needs a reader. use
	// a pipe to create both and split the upload into two phases:
	//   phase 1: compression and encryption of the file using the pipe's writer
	//   phase 2: uploading using the pipe's reader
	// phase 2 runs in a separate goroutine
	reader, writer := io.Pipe()

	// use this context to cancel the uploading if there's an error in phase1
	ctx, cancel := context.WithCancel(context.Background())

	// channel to share the metadata generated in phase 1 with the uploader
	// in phase2
	mchan := make(chan map[string]string)
	defer close(mchan)

	// channel to get any error back from phase2
	echan := make(chan error)
	defer close(echan)

	// run phase2 in separate goroutine
	go cl.uploadPhase2(ctx, echan, mchan, reader, key)

	// create a counter at the bottom of the stack to measure how much
	//   data is actually uploaded
	counter := NewWriteCounter(writer)
	defer counter.Close()

	_, err := cl.uploadPhase1(mchan, counter, source, compress, encrypt, scrypt)
	if err != nil {
		// signal phase2 to finish with cancel
		cancel()

		// close the writer - need to do this even if the context has been
		//   cancelled, otherwise the aws uploader blocks...
		writer.Close()

		// ignore any error from phase2
		<-echan

		return 0, err
	}

	// signal phase2 to finish by closing the writer
	writer.Close()

	// return bytes actually uploaded and any error. the channel read
	//   will block until phase2 finishes.
	return counter.TotalBytes(), <-echan
}

func (cl *client) uploadPhase1(mchan chan<- map[string]string, writer io.WriteCloser, source io.Reader, compress, encrypt, scrypt bool) (int64, error) {
	// create the map for metadata
	mdata := make(map[string]string)

	// insert the encrypter first - so it will be called
	//   after the compressor
	if encrypt && !scrypt {
		mdata["s3bu-encrypt"] = "age"
		mdata["s3bu-encrypt-version"] = "001"

		// encrypt writes data into the unlerlying writer and can block ...
		//   add a bufio writer in there so there's some space to write to
		bwriter := bufio.NewWriter(writer)
		defer bwriter.Flush()

		ewriter, err := age.Encrypt(bwriter, cl.recipients...)
		if err != nil {
			return 0, err
		}
		defer ewriter.Close()

		writer = ewriter
	}

	if scrypt {
		passkey := cl.passkeys[len(cl.passkeys)-1]

		mdata["s3bu-scrypt"] = "age"
		mdata["s3bu-scrypt-version"] = "001"
		mdata["s3bu-scrypt-id"] = passkey

		passphrase := cl.passphrases[passkey]

		recipient, err := age.NewScryptRecipient(passphrase)
		if err != nil {
			return 0, err
		}

		// encrypt writes data into the unlerlying writer and can block ...
		//   add a bufio writer in there so there's some space to write to
		bwriter := bufio.NewWriter(writer)
		defer bwriter.Flush()

		ewriter, err := age.Encrypt(bwriter, recipient)
		if err != nil {
			return 0, err
		}
		defer ewriter.Close()

		writer = ewriter
	}

	// insert the compressor
	if compress {
		mdata["s3bu-compress"] = "gzip"
		mdata["s3bu-compress-version"] = "001"

		gzwriter := gzip.NewWriter(writer)
		defer gzwriter.Close()

		writer = gzwriter
	}

	mchan <- mdata

	return io.Copy(writer, source)
}

func (cl *client) uploadPhase2(ctx context.Context, rchan chan<- error, mchan <-chan map[string]string, reader io.ReadCloser, key string) {
	// close the reader when done
	defer reader.Close()

	// wait for the meta data to arrive
	var mdata map[string]string
	for mdata == nil {
		select {
		case mdata = <-mchan:
		case <-ctx.Done():
			rchan <- nil
			return
		}
	}

	// can't use the simple PutObject method because don't know the ContentLength
	// in advance so use an Uploader...
	uploader := manager.NewUploader(cl.client)
	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:   cl.bucket,
		Key:      aws.String(key),
		Body:     reader,
		Metadata: mdata,
	})

	rchan <- err
}
