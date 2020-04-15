# type: ignore
import os

import idseq.cli

from argparse import Namespace
from urllib.parse import unquote_plus

# use .env file if supported
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def lambda_handler(event, context):
    print("Log stream name:", context.log_stream_name)
    print("Log group name:", context.log_group_name)
    print("Request ID:", context.aws_request_id)

    try:
        bucket, r1_key, r2_key, sample_name = _extract_values(event)
    except ValueError as e:
        print(e)
        return

    idseq.cli.main(Namespace(
        email=os.getenv('IDSEQ_EMAIL'),
        token=os.getenv('IDSEQ_TOKEN'),
        project=os.getenv('IDSEQ_PROJECT'),

        sample_name=sample_name,
        metadata=_metadata(sample_name),
        r1=idseq.uploader.build_path(bucket, r1_key),
        r2=idseq.uploader.build_path(bucket, r2_key),

        bulk=None,
        skip_geosearch=True,
        accept_all=True,
        use_taxon_whitelist=False,
        # for testing
        do_not_process=False,

        url='https://idseq.net',
        # uploadchunksize is ignored for s3
        uploadchunksize=idseq.cli.uploader.DEFAULT_MAX_PART_SIZE_IN_MB,
    ))
    # In event invocation type (asynchronous execution), the return value is
    # discarded.
    return


def _extract_values(event):
    """
    >>> _extract_values(_test_event())
    ('czb', 'prefix/RR1_R1_001.fastq.gz', 'prefix/RR1_R2_001.fastq.gz', 'RR1')

    >>> _extract_values({})
    Traceback (most recent call last):
    ...
    ValueError: No event input

    >>> _extract_values({'Records': [{'eventName': 'ObjectDeleted'}]})
    Traceback (most recent call last):
    ...
    ValueError: Skipping non-create event: ObjectDeleted
    """
    if not event:
        raise ValueError('No event input')

    # Only one PUT event per invocation. See:
    # https://stackoverflow.com/questions/53842445/is-it-possible-for-s3-to-send-multiple-records
    assert len(event['Records']) == 1
    record = event['Records'][0]

    if not record['eventName'].startswith('ObjectCreated:'):
        raise ValueError(
            'Skipping non-create event: {}'.format(record['eventName']))

    bucket, key = _get_bucket_key(record)

    r1_key, r2_key, sample_name = _get_names(key)

    return bucket, r1_key, r2_key, sample_name


def _get_bucket_key(record):
    bucket = record['s3']['bucket']['name']
    key = unquote_plus(record['s3']['object']['key'])
    return bucket, key


def _get_names(key):
    """
    Here we assume that R1 and R2 files are both available when the R2 file is available.
    This assumption is consistent with all previous files in czb bucket examined.

    >>> _get_names('s3://czb/RR1_R2_001.fastq.gz')
    ('s3://czb/RR1_R1_001.fastq.gz', 's3://czb/RR1_R2_001.fastq.gz', 'RR1')

    >>> _get_names('s3://czb/test.txt')
    Traceback (most recent call last):
    ...
    ValueError: Skipping non fastq.gz file: s3://czb/test.txt

    >>> _get_names('s3://czb/Undetermined_S0_R1_001.fastq.gz')
    Traceback (most recent call last):
    ...
    ValueError: Skipping non-R2 file: s3://czb/Undetermined_S0_R1_001.fastq.gz

    >>> _get_names('s3://czb/Undetermined_S0_R2_001.fastq.gz')
    Traceback (most recent call last):
    ...
    ValueError: Skipping non Rapid Response (RR) file: s3://czb/Undetermined_S0_R2_001.fastq.gz
    """
    if not key.endswith('.fastq.gz'):
        raise ValueError('Skipping non fastq.gz file: {}'.format(key))

    r1_suffix = '_R1_001.fastq.gz'
    r2_suffix = '_R2_001.fastq.gz'
    if not key.endswith(r2_suffix):
        raise ValueError('Skipping non-R2 file: {}'.format(key))
    r1_key = key.replace(r2_suffix, r1_suffix)
    r2_key = key

    sample_name = key.split('/')[-1].split(r2_suffix)[0]
    if not sample_name.startswith('RR'):
        raise ValueError(
            'Skipping non Rapid Response (RR) file: {}'.format(key))

    return r1_key, r2_key, sample_name


def _metadata(sample_name):
    return {
        'sample_name': sample_name,
        'host_genome': 'Human',
        'sample_type': 'Unknown',
        'nucleotide_type': 'RNA',
        'collection_date': '2020-03',
        'Collection Location': 'California, USA',
        'Water Control': 'Yes' if '_hela' in sample_name or '_wa' in sample_name else 'No',
        # custom metadata
        'Uploaded By': os.getenv('AWS_LAMBDA_FUNCTION_NAME', 'idseq-cli'),
    }


def _test_event(eventName="ObjectCreated:Put"):
    return {
        "Records": [
            {
                "eventName": eventName,
                "s3": {
                    "bucket": {
                        "name": "czb",
                    },
                    "object": {
                        "key": "prefix/RR1_R2_001.fastq.gz",
                    }
                }
            }
        ]
    }


def _test_context():
    """
    See https://docs.aws.amazon.com/lambda/latest/dg/python-context.html
    """
    return Namespace(
        log_stream_name='test_log_stream_name',
        log_group_name='test_log_group_name',
        aws_request_id='test_aws_request_id',
    )


if __name__ == '__main__':
    # print(lambda_handler(_test_event(), _test_context()))
    import doctest
    doctest.testmod(optionflags=doctest.FAIL_FAST | doctest.ELLIPSIS)
