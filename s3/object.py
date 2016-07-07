from boto.exception import S3ResponseError
from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError

from s3 import connection


def _get_bucket(ctx):
    relationships = ctx.instance.relationships

    if len(relationships) == 0:
        raise NonRecoverableError(
            'S3 objects must be contained in S3 buckets using a '
            'cloudify.aws.relationships.s3_object_contained_in_bucket '
            'relationship.'
        )

    bucket_name = None
    target = 'cloudify.aws.relationships.s3_object_contained_in_bucket'
    for relationship in relationships:
        if relationship.type == target:
            bucket_name = relationship.target.node.properties['name']

    if bucket_name is None:
        raise NonRecoverableError(
            'Could not get containing bucket name from related node.'
        )

    s3_client = connection.S3ConnectionClient().client()
    try:
        bucket = s3_client.get_bucket(bucket_name)
    except S3ResponseError as err:
        raise NonRecoverableError(
            'Could not access bucket {bucket}: {error} '.format(
                bucket=bucket_name,
                error=err.message,
            )
        )

    return bucket


def _bucket_is_public(bucket):
    for grant in bucket.list_grants():
        if (
            grant.uri == 'http://acs.amazonaws.com/groups/global/AllUsers' and
            grant.permission == 'READ'
        ):
            return True
    return False


@operation
def create(ctx):
    ctx.instance.runtime_properties['created'] = False

    bucket = _get_bucket(ctx)

    keys = bucket.get_all_keys()
    keys = [key.name for key in keys]
    if ctx.node.properties['name'] in keys:
        raise NonRecoverableError(
            'Cannot create key {name} in bucket {bucket} as a key by this '
            'name already exists in the bucket.'.format(
                name=ctx.node.properties['name'],
                bucket=bucket.name,
            )
        )

    # Create key
    key = bucket.new_key(ctx.node.properties['name'])
    key.content_type = ctx.node.properties['content_type']

    if ctx.node.properties['load_contents_from_file']:
        contents = ctx.download_resource(ctx.node.properties['contents'])
        key.set_contents_from_filename(contents)
    else:
        key.set_contents_from_string(ctx.node.properties['contents'])

    if _bucket_is_public(bucket):
        key.make_public()

    ctx.instance.runtime_properties['created'] = True


@operation
def delete(ctx):
    bucket = _get_bucket(ctx)
    if ctx.instance.runtime_properties.get('created', False):
        bucket.delete_key(ctx.node.properties['name'])
    else:
        raise NonRecoverableError(
            'Creation of key {name} in bucket {bucket} failed, so it will not '
            'be deleted.'.format(
                name=ctx.node.properties['name'],
                bucket=bucket.name,
            )
        )
