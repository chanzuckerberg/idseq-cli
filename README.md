# [IDseq](https://idseq.net/) &middot; [![GitHub license](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://github.com/chanzuckerberg/idseq-web/blob/master/LICENSE) [![Build Status](https://travis-ci.org/chanzuckerberg/idseq-cli.svg?branch=master)](https://travis-ci.org/chanzuckerberg/idseq-cli)

This project is stable and still maintained, but not actively under development.

![logo](https://assets.idseq.net/assets/Logo_Black.png)

#### Infectious Disease Sequencing Platform
IDseq is an unbiased global software platform that helps scientists identify pathogens in metagenomic sequencing data.

- **Discover** - Identify the pathogen landscape
- **Detect** - Monitor and review potential outbreaks
- **Decipher** - Find potential infecting organisms in large datasets

A collaborative open project of [Chan Zuckerberg Initiative](https://www.chanzuckerberg.com/) and [Chan Zuckerberg Biohub](https://czbiohub.org).

Check out our repositories:
- [idseq-web](https://github.com/chanzuckerberg/idseq-web) - Frontend portal
- [idseq-workflows](https://github.com/chanzuckerberg/idseq-workflows) - Bioinformatics workflows
- [idseq-cli](https://github.com/chanzuckerberg/idseq-cli) - Command line upload interface (here)
- [idseq-bench](https://github.com/chanzuckerberg/idseq-bench) - Pipeline benchmarking tools


## Getting Started/Usage Instructions
- See live instructions and view your user token at https://idseq.net/cli_user_instructions

### (1) Install and configure the Amazon Web Services Command Line Interface (AWS CLI):

For macOS users: We recommend trying the Homebrew package manager to install `awscli`. You can install by running these commands:

`/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"`

`brew install awscli`

- Otherwise follow the AWS installation instructions here: https://docs.aws.amazon.com/cli/latest/userguide/installing.html

- Verify it works by running `aws help`, which should display usage instructions. You do not need to set up AWS credentials unless you're using the bulk upload mode.

### (2) Install the IDseq CLI:

`pip install git+https://github.com/chanzuckerberg/idseq-cli.git --upgrade`
- Tips: Make sure you have Python installed already. Try running pip --version or python --version.

- Try running with pip2 or pip3 depending on your configuration. Try sudo pip if you run into permissions errors. You can use this same command in the future to update the CLI if needed.

### (3) Upload a single sample:

`idseq -e YOUR_EMAIL -t YOUR_TOKEN -p'Your Project Name' -s 'Your Sample Name' \
--r1 your_sample_R1.fastq.gz --r2 your_sample_R2.fastq.gz`

- Replace YOUR_EMAIL with your IDseq email and YOUR_TOKEN with your upload token.
- Supported file types: .fastq/.fq/.fasta/.fa or .fastq.gz/.fq.gz/.fasta.gz/.fa.gz

- You can create a new project as part of this command. Provide the name of a project that doesn't exist yet and the command will create it for you.

- You will be prompted to upload metadata in a CSV file with your samples. This is also where you will specify the Host Genome.
  - Instructions: https://idseq.net/metadata/instructions
  - Metadata dictionary and supported host genomes: https://idseq.net/metadata/dictionary
  - Metadata CSV template: https://idseq.net/metadata/metadata_template_csv

- Your authentication token for uploading is the token after -t. Keep this private like a password!

- Tips: Avoid copying commands into programs like TextEdit because it may change "straight quotes" into “smart quotes” (“ ‘ ’ ”) which will not be parsed correctly in your terminal.

- The '\' symbol means to continue on the next line in the terminal. If you use this in your command, make sure it is not followed by a space before the line break.

- New to using a command line? You will need to use cd and ls to navigate to the folder on your computer containing the source files you want to upload. [Guide here](https://www.git-tower.com/learn/git/ebook/en/command-line/appendix/command-line-101).

### (Optional) Run the program in interactive mode:

Having trouble? Just run idseq without any parameters and the program will guide you through the process.

### (Optional) Upload samples in bulk mode by specifying a folder:

`idseq -e YOUR_EMAIL -t YOUR_TOKEN -p'Your Project Name' \
--bulk /path/to/your/folder`

Edit the command in this text box and copy-and-paste:

`idseq -e YOUR_EMAIL -t YOUR_TOKEN -p 'Your Project Name' --bulk .`
- The '.' refers to the current folder in your terminal. The program will try to auto-detect files in the folder.

## Troubleshooting

### `OverflowError: cannot fit 'int' into an index-sized integer.`

Your computer might be limited on RAM. Try reducing your file chunk size by adding `--uploadchunksize 250` to your command. This splits your file into smaller pieces before uploading each one to IDseq.


## Contributing

This project is not currently accepting contributions at this time.

This project adheres to the Contributor Covenant [code of conduct](https://www.contributor-covenant.org/). By participating, you are expected to uphold this code. Please report unacceptable behavior to opensource@chanzuckerberg.com.

## Reporting Security Issues

Please note: If you believe you have found a security issue, please responsibly disclose by contacting us at security@chanzuckerberg.com.

See [SECURITY.md](SECURITY.md) for more information.

