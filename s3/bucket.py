from boto.exception import S3CreateError, S3PermissionsError, S3ResponseError
from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError

from s3 import connection


@operation
def create(ctx):
    ctx.instance.runtime_properties['created'] = False

    s3_client = connection.S3ConnectionClient().client()
    bucket_name = ctx.node.properties['name']

    existing_buckets = s3_client.get_all_buckets()
    existing_buckets = [bucket.name for bucket in existing_buckets]
    if ctx.node.properties['use_existing_resource']:
        if bucket_name in existing_buckets:
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
        bucket = s3_client.create_bucket(bucket_name)
        ctx.instance.runtime_properties['created'] = True
    except S3CreateError as err:
        raise NonRecoverableError(
            'Bucket creation failed: {}'.format(err.msg)
        )

    if ctx.node.properties['public']:
        bucket.make_public()

    if ctx.node.properties['website_default_page'] != '':
        website_default_page = ctx.node.properties['website_default_page']
        if '/' in website_default_page:
            raise NonRecoverableError(
                'S3 bucket website default page must not contain a /'
            )
        else:
            bucket.configure_website(suffix=website_default_page)

    ctx.instance.runtime_properties['url'] = (
        'http://' + bucket.get_website_endpoint()
    )


@operation
def delete(ctx):
    s3_client = connection.S3ConnectionClient().client()
    bucket_name = ctx.node.properties['name']

    if ctx.node.properties['use_existing_resource']:
        return True

    if not ctx.instance.runtime_properties.get('created', False):
        raise NonRecoverableError(
            'Bucket {bucket} creation failed, so it will not be '
            'deleted.'.format(bucket=bucket_name)
        )

    try:
        bucket = s3_client.get_bucket(bucket_name)
        bucket.delete()
    except (S3PermissionsError, S3ResponseError) as err:
        raise NonRecoverableError(
            'Bucket deletion failed: {}'.format(err.message)
        )
