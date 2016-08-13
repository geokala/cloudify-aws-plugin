from cloudify_aws.s3 import connection

from botocore.exceptions import ClientError
from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError

import string


def validate_bucket_name(bucket_name):
    # http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
    invalid_bucket_message = (
        'Bucket names must be between 3 and 63 characters in length, '
        'inclusive. '
        'Bucket names containing dots will be rejected as these will render '
        'virtual hosting of these buckets unusable with SSL. '
        'Bucket names can contain lower case letters, numbers, and hyphens. '
        'Bucket names must start and end with a lower case letter or number. '
        'Bucket {name} did not meet these requirements.'
    )

    valid_start_and_end = string.lowercase + string.digits
    valid_characters = string.lowercase + string.digits + '-'

    if (
        3 <= len(bucket_name) <= 63 and
        all(char in valid_characters for char in bucket_name) and
        bucket_name[0] in valid_start_and_end and
        bucket_name[-1] in valid_start_and_end
    ):
        return True
    else:
        raise NonRecoverableError(
            invalid_bucket_message.format(name=bucket_name),
        )


def _get_bucket_url(ctx):
    bucket_name = ctx.node.properties['name']
    s3_client = connection.S3ConnectionClient().client()

    bucket_region = s3_client.head_bucket(
        Bucket=bucket_name,
    )['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']

    try:
        s3_client.get_bucket_website(Bucket=bucket_name)
        web_bucket = True
    except ClientError as err:
        if 'NoSuchWebsiteConfiguration' in str(err):
            web_bucket = False
        else:
            raise

    if web_bucket:
        url_structure = 'http://{bucket}.s3-website-{region}.amazonaws.com/'
    else:
        url_structure = 'https://s3.amazonaws.com/{bucket}/'

    return url_structure.format(
        bucket=bucket_name,
        region=bucket_region,
    )


@operation
def create(ctx):
    ctx.instance.runtime_properties['created'] = (
        ctx.instance.runtime_properties.get('created', False)
    )

    bucket_name = ctx.node.properties['name']

    should_exist = (
        ctx.node.properties['use_existing_resource'] or
        ctx.instance.runtime_properties['created']
    )

    # It's possible for bucket names to not match our validation if they were
    # pre-existing, for a variety of reasons (including changed rules on s3).
    # We therefore only validate if we're not using an existing bucket.
    # Note: It's possible that the existing_buckets check can fail with an
    # invalid bucket name, but presumably then this was going to fail anyway.
    if not should_exist:
        validate_bucket_name(ctx.node.properties['name'])

    s3_client = connection.S3ConnectionClient().client()
    existing_buckets = s3_client.list_buckets()['Buckets']
    existing_buckets = [bucket['Name'] for bucket in existing_buckets]
    if should_exist:
        if bucket_name in existing_buckets:
            ctx.instance.runtime_properties['url'] = _get_bucket_url(ctx)
            return True
        else:
            raise NonRecoverableError(
                'Attempt to use existing bucket {bucket} failed, as no '
                'bucket by that name exists.'.format(bucket=bucket_name)
            )
    else:
        if bucket_name in existing_buckets:
            raise NonRecoverableError(
                'Bucket {bucket} already exists, but use_existing_resource '
                'is not set to true.'.format(bucket=bucket_name)
            )

    try:
        s3_client.create_bucket(
            Bucket=bucket_name,
            ACL=ctx.node.properties['permissions'],
        )
        ctx.instance.runtime_properties['created'] = True
    except ClientError as err:
        raise NonRecoverableError(
            'Bucket creation failed: {}'.format(str(err))
        )

    # See if we should configure this as a website
    index = ctx.node.properties['website_index_page']
    error = ctx.node.properties['website_error_page']
    if (index, error) == ('', ''):
        # Neither the index nor the error page were defined, this bucket is
        # not intended to be a website
        pass
    elif '' in (index, error):
        raise NonRecoverableError(
            'For the bucket to be configured as a website, both '
            'website_index_page and website_error_page must be set.'
        )
    else:
        if '/' in index:
            raise NonRecoverableError(
                'S3 bucket website default page must not contain a /'
            )
        s3_client.put_bucket_website(
            Bucket=bucket_name,
            WebsiteConfiguration={
                'ErrorDocument': {
                    'Key': error,
                },
                'IndexDocument': {
                    'Suffix': index,
                },
            },
        )

    ctx.instance.runtime_properties['url'] = _get_bucket_url(ctx)


@operation
def delete(ctx):
    if ctx.node.properties['use_existing_resource']:
        return True

    bucket_name = ctx.node.properties['name']

    if not ctx.instance.runtime_properties.get('created', False):
        raise NonRecoverableError(
            'Bucket {bucket} creation failed, so it will not be '
            'deleted.'.format(bucket=bucket_name)
        )

    s3_client = connection.S3ConnectionClient().client()

    try:
        s3_client.delete_bucket(Bucket=bucket_name)
    except ClientError as err:
        raise NonRecoverableError(
            'Bucket deletion failed: {}'.format(err.message)
        )
