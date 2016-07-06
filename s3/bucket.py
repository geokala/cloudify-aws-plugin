from boto.exception import S3CreateError, S3PermissionsError
from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError

from s3 import connection


@operation
def create(ctx):
    s3_client = connection.S3ConnectionClient().client()
    bucket_name = ctx.node.properties['name']
    try:
        bucket = s3_client.create_bucket(bucket_name)
    except S3CreateError as err:
        raise NonRecoverableError(
            'Bucket creation failed: {}'.format(err.msg)
        )

    if ctx.node.properties['public']:
        bucket.make_public()


@operation
def delete(ctx):
    s3_client = connection.S3ConnectionClient().client()
    bucket_name = ctx.node.properties['name']
    try:
        bucket = s3_client.bucket.Bucket(name=bucket_name)
        bucket.delete()
    except S3PermissionsError as err:
        # TODO: I'm guessing about this being the only error we should handle
        # in this way
        raise NonRecoverableError(
            'Bucket deletion failed: {}'.format(err.msg)
        )
