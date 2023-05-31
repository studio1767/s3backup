package s3io

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"

	"filippo.io/age"
	"gopkg.in/yaml.v3"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Client interface {
	Exists(key string) (bool, error)
	LatestMatching(prefix string) (string, int64, error)

	Upload(key string, source io.Reader) (int64, error)
	UploadCompressed(key string, source io.Reader) (int64, error)
	UploadEncrypted(key string, source io.Reader, compress bool) (int64, error)
	UploadPassphrase(key string, source io.Reader, compress bool) (int64, error)

	HasIdentities() bool

	Download(key string, sink io.Writer) (int64, error)
}

type client struct {
	client      *s3.Client
	bucket      *string
	recipients  []age.Recipient
	identities  []age.Identity
	passkeys    []string
	passphrases map[string]string
}

func NewClient(profile, bucket string, identities_file, secrets_file string) (Client, error) {

	// load the profile
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithSharedConfigProfile(profile))
	if err != nil {
		return nil, err
	}

	// create the client
	s3client := s3.NewFromConfig(cfg)

	// load the various encryption key
	recipients, err := loadRecipients(s3client, bucket)
	if err != nil {
		return nil, err
	}
	identities, err := loadIdentities(identities_file)
	if err != nil {
		return nil, err
	}
	passkeys, passphrases, err := loadSecrets(secrets_file)
	if err != nil {
		return nil, err
	}

	// create the client
	cl := client{
		client:      s3client,
		bucket:      aws.String(bucket),
		recipients:  recipients,
		identities:  identities,
		passkeys:    passkeys,
		passphrases: passphrases,
	}

	return &cl, nil
}

func (cl *client) HasIdentities() bool {
	return len(cl.identities) > 0
}

func loadRecipients(cl *s3.Client, bucket string) ([]age.Recipient, error) {

	resp, err := cl.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("repo/recipients.txt"),
	})
	if err != nil {
		var nosuchkey *types.NoSuchKey
		if errors.As(err, &nosuchkey) {
			return nil, &ErrNoRecipientsFile{}
		}
		return nil, err
	}
	defer resp.Body.Close()

	return age.ParseRecipients(resp.Body)
}

func loadIdentities(identities_file string) ([]age.Identity, error) {
	// set the default path for 'default'
	if identities_file == "default" {
		u, err := user.Current()
		if err != nil {
			return nil, err
		}
		identities_file = filepath.Join(u.HomeDir, ".s3bu", "identities.txt")
	}

	// check the file permissions
	info, err := os.Stat(identities_file)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	perms := info.Mode()
	if perms&0077 != 0 {
		return nil, &ErrPermissionsTooOpen{
			msg: fmt.Sprintf("Permissions on identities file are too open: %#o", perms),
		}
	}

	// load the identities
	f, err := os.Open(identities_file)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	return age.ParseIdentities(f)
}

func loadSecrets(secrets_file string) ([]string, map[string]string, error) {
	// set the default path
	if secrets_file == "default" {
		u, err := user.Current()
		if err != nil {
			return nil, nil, err
		}
		secrets_file = filepath.Join(u.HomeDir, ".s3bu", "secrets.yml")
	}

	// check the file permissions
	info, err := os.Stat(secrets_file)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, &ErrNoSecretsFile{}
		}
		return nil, nil, err
	}
	perms := info.Mode()
	if perms&0077 != 0 {
		return nil, nil, &ErrPermissionsTooOpen{
			msg: fmt.Sprintf("Permissions on secrets file are too open: %#o", perms),
		}
	}

	// load the file
	data, err := os.ReadFile(secrets_file)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, &ErrNoSecretsFile{}
		}
		return nil, nil, err
	}

	type Data struct {
		Id         string
		Passphrase string
	}
	var raw []Data

	err = yaml.Unmarshal(data, &raw)
	if err != nil {
		return nil, nil, err
	}

	if len(raw) == 0 {
		return nil, nil, &ErrNoSecretsFound{
			file: secrets_file,
		}
	}

	passphrases := make(map[string]string)
	var passkeys []string

	for _, entry := range raw {
		passkeys = append(passkeys, entry.Id)
		passphrases[entry.Id] = entry.Passphrase
	}

	return passkeys, passphrases, nil
}
