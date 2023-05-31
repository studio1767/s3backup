# Backup and Restore to AWS S3

This project creates tools to backup local data to an AWS S3 bucket, restore the backups (fully or partially based on a regex),
and some helper tools to upload job configurations and download files with transparent decryption.

## Key Features

* client side ecryption of data and metadata
* client managed keys
* support for multiple backup sources per job
* support for multiple backup jobs per repository
* deduplication of all files across all jobs in the repository

## Setup

Setting up the environment with all the keys, and users and permissions is reasonably involved. Instead
of documenting all the steps, and no doubt getting things wrong, there is a sibling repository to this 
one at [s3backup-infrastructure](https://github.com/studio1767/s3backup-infrastructure) that uses terraform to 
generate a working system that's ready to use.

It creates the s3 bucket, generates IAM users with correct permissions, creates the encryption keys and finally, generates
template job configurations for each backup job. Once this is done, you only need to update the job configuration
files to match your backup needs, upload them, and you have a working system.

The following sections describe how different parts of the system work.

## Bucket Structure

There are four key prefixes used in the bucket as described in the table below.

|   Prefix   | Description                                              |
|------------|----------------------------------------------------------|
| repo/      | used for the age recipients key for encrypting data      |
| jobs/      | all job configurations                                   |
| manifests/ | uploaded manifests for each backup                       |
| data/      | the backed up data stored under a content-hash hierarchy |

The `repo/` prefix currently has a single object with the key `repo/recipients.txt`. This holds
the recipients key for the age encryption algorithm and is required to be present. In the default
permissions setup, backup and restore users only have read access to this key.

The `jobs/` prefix is where all the job configurations are stored. 

The key format is:

    jobs/<jobname>/<jobname>-<version>.yml

An example of what this looks like is this:

    2023-01-29 17:03:53        756 jobs/nimbus/nimbus-001.yml
    2023-01-30 09:38:15        777 jobs/nimbus/nimbus-002.yml
    2023-03-27 13:34:11        951 jobs/nimbus/nimbus-003.yml
    2023-03-28 14:53:29        771 jobs/nimbus/nimbus-004.yml

The backup application downloads the most recent job configuration and uses the contents to drive
the backup process.

If jobs are uploaded using the `s3jobupload` tools, they will be encrypted before uploading to the bucket.

The `manifests/` prefix is where the backup tool uploads the backup manifests to. This file is a simple csv file
that lists all the files processed by the backup and their metadata. It is used on the next backup to generate the
diff between what's on the disk and what's already uploaded. 

The format of the keys is:

    manifests/<jobname>/<labelname>/<jobname>-<labelname>-<timestamp>

An example of what this looks like is:

    2023-02-28 13:10:58    1794480 manifests/nimbus/nimbus/nimbus-nimbus-2023-02-28-47431.csv.gz
    2023-03-16 11:21:28    1794600 manifests/nimbus/nimbus/nimbus-nimbus-2023-03-16-40837.csv.gz
    2023-03-28 15:06:35    1800139 manifests/nimbus/nimbus/nimbus-nimbus-2023-03-28-54390.csv.gz
    2023-04-21 20:14:37    1800140 manifests/nimbus/nimbus/nimbus-nimbus-2023-04-21-72872.csv.gz
    2023-05-03 13:48:40    1799336 manifests/nimbus/nimbus/nimbus-nimbus-2023-05-03-49716.csv.gz

The `data/` prefix holds the actual backed up file data. It is stored in keys named after the content
hash of the file. The manifest file maps the backed up file name to it's content hash for
restoring. 

The format of the keys is:

    data/<first-4-characters-of-hash>/<full-hash>

An example of this is:

    2023-04-30 11:17:44     869607 data/3c34/3c346f7689103d92df482fda90300dd8c66da0a883a9be02ab0eba41d04c39d5
    2023-05-31 11:37:09    3976570 data/3c34/3c349ec0413b44aad9b95bc3d7155f559896fbf6b80857ca7bfa25ad05fb3a9e
    2023-02-28 09:39:13       5015 data/3c36/3c36d6326384e558d9e74143b12208d882431bb475a583c276468d3b85563db0
    2023-05-10 10:03:05     243932 data/3c37/3c373028c9e73fb01278a94fd4f7095ed471272d115baaf43733bdef4c1031ea
    2023-01-31 17:29:32       5492 data/3c37/3c37ec4d217101090d4eea8d9fdf3b94cf674ca52676c39522aed250f5bbb3d3
    2023-05-10 09:50:07        399 data/3c38/3c38f7a454656cba1f35bffbb2bcc99796fdc3cacb1ed3d9e80941edc1ef9188
    2023-01-04 14:08:24        823 data/3c39/3c39093bdfcf762ac5df336a8a04defc03dfc84d10b94b2826d740ac9b506026
    2023-03-10 05:26:18     737528 data/3c39/3c390a4f7dd4733633da106e1282bf03ba5e23197b6826f72d58371ec5d2d786
    2023-04-28 14:58:55     393046 data/3c39/3c390b0a4d6ccdea0329f5d0a0b565c67b6d0d26e889fa58ed2affe7aa0e2fe0

## Encryption

A very important thing to keep in mind here is that the encryption and decryption all happens on the client side
using client managed keys. If the client loses the keys, then the backups will no longer be accessible.
So, keep them somewhere safe!

The metadata (job configurations and manifest files) and data are encrypted automatically during the backup.
As mentioned previously, this happens on the client side, using client managed keys. Both use [age](https://github.com/FiloSottile/age)
for ecnryption.

The backup data is encrypted using the recipients file in the bucket under the key `repo/recipients.txt`. This 
is a hard requirement - the backup won't run if the file isn't there.

To restore, you need the matching identities file, accessible on your local disk. Do not upload the 
identities file to the bucket.

Using asymmetric encryption for the data means that the public key can be made accessible in the bucket for all
backup users to access, but only people you provide the identities key to can restore.

The metadata is encrypted using passphrases. These are unique to each user. This way users can only download
and decrypt their own job and manifest files.

The keys for the metadata encryption/decryption are stored by default in:

    ~/.s3bu/secrets.yml

It is also a hard requirement that this file be present and have valid contents.

The file generated by the infrastructure project creates passphrases of the same form that age itself will generate.
The contents of the secrets file is like this:

    - id: Ol4uX0S47CRmk2ZR
      passphrase: often-shove-area-age-frog-brick-lift-snake-city-joy
    - id: jhU3wC1wQ5sFakiN
      passphrase: call-mention-enjoy-roast-woman-clinic-excite-buyer-toad-meadow
    - id: 0S5F4YM84UPJEhBG
      passphrase: gift-range-soon-sand-wolf-salmon-uphold-captain-material-champion
    - id: PtV7YohWQhtz1KTZ
      passphrase: absurd-fresh-jeans-weapon-chaos-door-dutch-tell-trick-vanish

When encrypting, the last entry in the file is used. To retire keys, simply add a new passphrase 
to the file. Leave the older entries as they will be needed to decrypt older jobs and manifests.

The `id` is added to the uploaded file's metadata and is used to locate the correct passphrase for
transparent decryption.

## Job Configuration File

The job configuration file is a simple yaml file. A very simple configuration looks like this:

    ---
    # the directory to scan for inputs
    sources:
    - path: /home/me
      label: home
    - path: /home/me/Projects
      label: projects
      
    # list of file extensions to exclude
    exclude_extensions:
    - .DS_Store
    - .tfstate
    - .tfstate.backup
    
    # list of file extensions to include
    # include_extensions:
    # - .go
    # - .txt
    
    # top level directories to exclude
    exclude_top_dirs:
    - Applications
    - Downloads
    - .Trash
    
    # top level directories to include
    # include_top_dirs:
    # - Projects
    
    # directories we skip
    skip_dirs:
    - 3rdparty
    - pyenv
    
    # directories we skip ... if they have one of these (file or directory)
    skip_dir_items:
    - .nobackup
    - .git

The `sources` key lists the local source directories to backup. The `path` is the physical path and the `label` a logical 
name. If the physical mount point changes, you can update the path and keep the label the same and the backups will continue 
as normal. Manifest files are keyed using both the jobname and label as defined in here.

For the exclude/include options, include is evaluated first, then exclude: if an extension or directory is in both lists, it
will be excluded. I rarely use the 'include_' variant - if it isn't present, it includes everything.

The `*_extensions` options list file extensions to consider. 

The `*_top_dirs` are only evaluated for directories at the top level of the backup source.

The key `skip_dirs` lists directories to skip at all levels of the backup.

The `skip_dir_items` will skip directories if there is a file or directory with the name of one of the items in the
directory.

## Usage

### Update Job Configuration

To update a job configuration, first download the latest version of the job file to edit. 

    s3jobdownload -p <my-aws-profile> <bucket-name> <job-name>

This will automatically find the most recent configuration for the named job and download it. The
infrastructure project automatically creates initial job configurations from a default so there 
will always be a job there to edit.

Edit the configuration using a text editor and then upload:

    s3jobupload -p <my-aws-profile> <bucket-name> <job-name> <path-to-job-file>

This will rename the file to be numbered as the next number in sequence for the job configurations
and it will become the active configuration.

If this is the first time you have updated the configuration, the job will be stored at a key matching this:

    jobs/<jobname>/<jobname>-001.yml

### Running Backups

To run the backup, run this command:

    s3backup -p <my-aws-profile> <backup-bucket-name> <myjobname>

The backup procedure is something like this:

* download the latest job configuration (decrypting as necessary)
* download the latest manifest file (decrypting as necessary)
* iterate over the contents of the manifest file and the filesystem together
* compare the file names and metadata of each to determine if a file is new or modified
* if the file appears to be new or modified, generate the hash of its content
* check the bucket for this hash; if it isn't there, upload it
* keep looping until both the manifest and the filesystem iterators are drained
* as a final step, upload the new manifest file

The manifest that is generated is a full manifest of what is on the disk. In this way,
we only ever do incremental uploads/backups, but we always have a full manifest.

### Restoring Content

As mentioned in the encryption section, restoring uses the identities for decrypting the data. The default location 
for the identities file is:

    ~/.s3bu/identities.txt

If it's there, the tool will find it and use it. If it's somewhere else, the location can be overridden on the command line.

To restore you will also need the passphrases used to encrypt the manifest file. The infrastructure project generates 
individual secrets files with passphrases for each user, and one containing all passphrases intended for use by an
administrator.

Once all this is in place, run the restore with a command like this:

    s3restore -p <my-aws-profile> <backup-bucket-name> <manifest-key> <restore-root> [<pattern>]

The pattern is optional and is a regular expression used to match the file name. It defaults to '.*' to 
restore everything in the manifest.

As an example of a selective restore, to restore everything in a manifest under a directory called 
'Projects/s3backup', you would run a command like this:

    s3restore -p myprofilename backups.example.com manifests/test/local/test-local-2023-05-29-51748.csv.gz local '^Projects/s3backup/'

This would restore the files into the a directory called 'local' and preserve the full path to the file under this
new location.

By default, restore will not restore into a directory that isn't empty. To force it to do this, use the `-f` flag.

If is running in force mode, it won't overwrite any files that are already in the files system. To change this behaviour, 
use the `-o` flag.

The restore operation is like this:

* download the specified manifest file (decrypting as necessary)
* loop through each entry in the manifest
* compare the filename to the pattern, and if it matches, download it (decrypting as necessary)
* set the permissions on the file to match those recorded in the manifest

### Manual Downloading

There is a utility that will manually download any file you specify with a valid key and decrypt as
it goes. To use it:

    s3download -p myprofile mybucket full-key-to-file restore-directory

By default, it won't overwrite a file that already exists; use the '-o' flag to change that.

