package s3io

import (
	"fmt"
)

type ErrPermissionsTooOpen struct {
	msg string
}

func (e *ErrPermissionsTooOpen) Error() string {
	return e.msg
}

type ErrNoRecipientsFile struct{}

func (e *ErrNoRecipientsFile) Error() string {
	return "recipients file not found: expected in bucket at 'repo/recipients.txt'"
}

type ErrNoSecretsFile struct{}

func (e *ErrNoSecretsFile) Error() string {
	return "secrets file not found: default is '~/.s3bu/secrets.yml'"
}

type ErrNoSecretsFound struct {
	file string
}

func (e *ErrNoSecretsFound) Error() string {
	return fmt.Sprintf("no secrets found in '%s'", e.file)
}

type ErrPassphraseNotFound struct {
	operation string
}

func (e *ErrPassphraseNotFound) Error() string {
	return fmt.Sprintf("unable to %s: passphrase not found", e.operation)
}

type ErrIdentitiesNotFound struct{}

func (e *ErrIdentitiesNotFound) Error() string {
	return fmt.Sprintf("unable to decrypt: no identities available")
}

type ErrNoSuchObject struct {
	key string
}

func (e *ErrNoSuchObject) Error() string {
	return fmt.Sprintf("no such object in bucket: %s", e.key)
}

type ErrNoMatch struct {
	msg string
}

func (e *ErrNoMatch) Error() string {
	return e.msg
}

type ErrNotDownloadable struct {
	key          string
	storageClass string
}

func (e *ErrNotDownloadable) Error() string {
	return fmt.Sprintf("object is not downloadable: storage class is %s", e.storageClass)
}
