from boto.exception import S3CreateError, S3PermissionsError
from boto import s3
from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError

from s3 import connection


def _find_bucket(ctx):
    relationships = ctx.instance.relationships

    if len(relationships) == 0:
        raise NonRecoverableError(
            'S3 objects must be contained in S3 buckets using a '
            'cloudify.aws.relationships.s3_object_contained_in_bucket '
            'relationship.'
        )

    bucket_name = None
    for relationship in relationships:
        if relationship.type == 's3_object_contained_in_bucket':
            bucket_name = relationship.target.node.properties['name']

    if bucket_name is None:
        raise NonRecoverableError(
            'Could not get containing bucket name.'
        )

    return bucket_name


@operation
def create(ctx):
    _find_bucket(ctx)

    s3_client = connection.S3ConnectionClient().client()
    bucket = s3_client.bucket.Bucket(name=bucket_name)

    # Create key
    key = s3.key.Key(bucket)
    key.key = ctx.node.properties['name']

    if ctx.node.properties['load_contents_from_file']:
        contents = ctx.download_resource(contents)
        key.set_contents_from_filename(contents)
    else:
        key.set_contents_from_string(ctx.node.properties['contents'])


@operation
def delete(ctx):
    _find_bucket(ctx)

    s3_client = connection.S3ConnectionClient().client()
    bucket = s3_client.bucket.Bucket(name=bucket_name)

    bucket.delete_key(ctx.node.properties['name'])
