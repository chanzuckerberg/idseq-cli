# idseq-cli


## Install

`pip install git+https://github.com/chanzuckerberg/idseq-cli.git`

## Submit a sample
1. First, make sure you have login credentials for idseq website. You will need your login email and an authentication token
1. Only gzipped paired fastq files are accepted for idseq at the moment. Make sure you have the files on your local machine or on S3.
1. Submit your sample as follows. Get more detailed instructions by running `idseq --help`

```
idseq -t <authentication_token> -p <project_name> -s <sample_name> -u dev.idseq.net -e <email> --r1 <file1.fastq.gz> --r2 <file2.fastq.gz>

```

Here is a realistic example:

```
idseq -t idseq1234 -p 'Awesome Project' -s 'Nice Sample' -u localhost:3000 -e fake@example.com --r1 s3://czbiohub-infectious-disease/RR004/RR004_water_2_S23/RR004_water_2_S23_R1_001.fastq.gz --r2 s3://czbiohub-infectious-disease/RR004/RR004_water_2_S23/RR004_water_2_S23_R2_001.fastq.gz

```
