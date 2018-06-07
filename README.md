# idseq-cli


## Install

- Requires python2
- `pip install git+https://github.com/chanzuckerberg/idseq-cli.git`

## Submit a sample
1. Make sure AWS CLI is installed and configured
1. Make sure you have login credentials for idseq website. You will need your login email and an authentication token
1. idseq currently accepts fastq or fasta files, single or paired, gzipped or not. Make sure you have the files on your local machine or on S3.
1. Submit your sample as follows. Get more detailed instructions by running `idseq --help`

```
idseq -t <authentication_token> -p <project_name> -s <sample_name> -u idseq.net -e <email> --r1 <file1.fastq.gz> --r2 <file2.fastq.gz>

```

Here is a realistic example to upload a file to your local instance of idseq-web (localhost:3000):

```
idseq -t idseq1234 -p 'Awesome Project' -s 'Nice Sample' -u http://localhost:3000 -e fake@example.com --r1 s3://czbiohub-infectious-disease/RR004/RR004_water_2_S23/RR004_water_2_S23_R1_001.fastq.gz --r2 s3://czbiohub-infectious-disease/RR004/RR004_water_2_S23/RR004_water_2_S23_R2_001.fastq.gz

```

Another example:

```
idseq -p Water -s 'no host test' -u http://localhost:3000 -e xyz@chanzuckerberg.com -t abcxyz --r1 s3://czbiohub-infectious-disease/host_pre_subtracted/cMAL/Sample_cMAL_MP3304_CSF/retained-NR.retained-NT.filter.unmapped.cdhit.MP3304-23_S26_L002.fasta --preload s3://czbiohub-infectious-disease/host_pre_subtracted/cMAL/Sample_cMAL_MP3304_CSF --host_id 5 --job_queue idseq_alpha_stg1

```


