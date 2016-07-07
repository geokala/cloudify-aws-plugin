from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError

from s3 import connection


def _find_bucket_details(ctx):
    relationships = ctx.instance.relationships

    if len(relationships) == 0:
        raise NonRecoverableError(
            'S3 objects must be contained in S3 buckets using a '
            'cloudify.aws.relationships.s3_object_contained_in_bucket '
            'relationship.'
        )

    bucket_details = None
    target = 'cloudify.aws.relationships.s3_object_contained_in_bucket'
    for relationship in relationships:
        if relationship.type == target:
            bucket_details = relationship.target.node.properties

    if bucket_details is None:
        raise NonRecoverableError(
            'Could not get containing bucket.'
        )

    return bucket_details


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
    bucket_details = _find_bucket_details(ctx)

    s3_client = connection.S3ConnectionClient().client()
    bucket = s3_client.get_bucket(bucket_details['name'])

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


@operation
def delete(ctx):
    bucket_details = _find_bucket_details(ctx)

    s3_client = connection.S3ConnectionClient().client()
    bucket = s3_client.get_bucket(bucket_details['name'])

    bucket.delete_key(ctx.node.properties['name'])
