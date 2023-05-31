package job

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"

	yaml "gopkg.in/yaml.v3"

	"github.com/studio1767/s3backup/internal/s3io"
)

type ErrNoSuchJob struct {
	msg string
}

func (e *ErrNoSuchJob) Error() string {
	return e.msg
}

type Job struct {
	Name string

	Sources []struct {
		Path  string
		Label string
	}

	IncludeTopDirs []string `yaml:"include_top_dirs"`
	ExcludeTopDirs []string `yaml:"exclude_top_dirs"`

	IncludeExtensions []string `yaml:"include_extensions"`
	ExcludeExtensions []string `yaml:"exclude_extensions"`

	SkipDirs     []string `yaml:"skip_dirs"`
	SkipDirItems []string `yaml:"skip_dir_items"`
}

func Download(client s3io.Client, jobname string) (*Job, string, error) {
	// the prefix path
	prefix := fmt.Sprintf("jobs/%s/", jobname)

	// get the key for the latest job configuration
	jobkey, _, err := client.LatestMatching(prefix)
	if err != nil {
		var nomatch *s3io.ErrNoMatch
		if errors.As(err, &nomatch) {
			return nil, "", &ErrNoSuchJob{
				msg: fmt.Sprintf("No such job: %s", jobname),
			}
		}
		return nil, "", err
	}

	// download the job into a buffer
	data := bytes.NewBuffer(nil)

	_, err = client.Download(jobkey, data)
	if err != nil {
		return nil, jobkey, err
	}

	// unmarshal the data
	var job Job
	err = yaml.Unmarshal(data.Bytes(), &job)
	if err != nil {
		return nil, jobkey, err
	}

	job.Name = jobname

	return &job, jobkey, nil
}

func Upload(client s3io.Client, source io.Reader, jobname string) (string, error) {
	// the prefix path
	prefix := fmt.Sprintf("jobs/%s/", jobname)

	// get the key for the latest job configuration
	jobkey, _, err := client.LatestMatching(prefix)
	if err != nil {
		var nomatch *s3io.ErrNoMatch
		if errors.As(err, &nomatch) == false {
			return "", err
		}

		// if this is the first time uploading, fake the jobkey
		jobkey = fmt.Sprintf("jobs/%s/%s-000.yml", jobname, jobname)
	}

	// get the id of the last key
	re := regexp.MustCompile(fmt.Sprintf("^(.*/%s-)(\\d+)(.*)", jobname))

	matches := re.FindStringSubmatch(jobkey)
	if matches == nil || len(matches) != 4 {
		return "", nil
	}

	id, err := strconv.Atoi(matches[2])
	if err != nil {
		return "", err
	}

	key := fmt.Sprintf("%s%03d%s", matches[1], id+1, matches[3])

	// upload to the key
	_, err = client.UploadPassphrase(key, source, true)

	return key, err
}
