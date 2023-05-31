package manifest

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/studio1767/s3backup/internal/s3io"
)

type ErrNoSuchManifest struct {
	msg string
}

func (e *ErrNoSuchManifest) Error() string {
	return e.msg
}

func Download(client s3io.Client, jobname, label string) (*os.File, string, error) {
	// the prefix path
	prefix := fmt.Sprintf("manifests/%s/%s/", jobname, label)

	// get the key for the latest job configuration
	mkey, _, err := client.LatestMatching(prefix)
	if err != nil {
		var nomatch *s3io.ErrNoMatch
		if errors.As(err, &nomatch) {
			return nil, "", &ErrNoSuchManifest{
				msg: fmt.Sprintf("No manifest for job and label: %s:%s", jobname, label),
			}
		}
		return nil, "", err
	}

	f, err := DownloadWithKey(client, mkey)

	return f, mkey, err
}

func DownloadWithKey(client s3io.Client, mkey string) (*os.File, error) {
	// the name of the manifest file to save to. if the file is compressed on s3, it will
	//   automatically be decompressed on download so remove and '.gz' suffix.
	mtokens := strings.Split(mkey, "/")
	mname := strings.TrimSuffix(mtokens[len(mtokens)-1], ".gz")

	// download the job to a tempfile
	tmpfile := filepath.Join(os.TempDir(), mname)
	f, err := os.Create(tmpfile)
	if err != nil {
		return nil, err
	}

	_, err = client.Download(mkey, f)
	if err != nil {
		f.Close()
		os.Remove(tmpfile)
		return nil, err
	}

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		f.Close()
		os.Remove(tmpfile)
		return nil, err
	}

	return f, nil
}

func Upload(client s3io.Client, source io.Reader, jobname, label string) (string, error) {
	// create the manifest key
	now := time.Now()
	stamp := now.Format("2006-01-02")
	seconds := (((now.Hour() * 60) + now.Minute()) * 60) + now.Second()

	mkey := fmt.Sprintf("manifests/%s/%s/%s-%s-%s-%05d.csv.gz", jobname, label, jobname, label, stamp, seconds)

	_, err := client.UploadPassphrase(mkey, source, true)

	return mkey, err
}
