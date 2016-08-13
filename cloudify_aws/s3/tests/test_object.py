########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.

import testtools
from mock import patch, Mock

from cloudify.exceptions import NonRecoverableError
from botocore.exceptions import ClientError

from cloudify_aws.s3 import object


class TestObject(testtools.TestCase):

    def setUp(self):
        super(TestObject, self).setUp()

    def make_node_context(self,
                          name,
                          contents='',
                          load_contents_from_file=True,
                          content_type='application/octet-stream',
                          permissions=None,
                          runtime_properties=None):
        ctx = Mock()
        ctx.node.properties = {
            'name': name,
            'contents': contents,
            'load_contents_from_file': load_contents_from_file,
            'content_type': content_type,
        }
        if permissions is not None:
            ctx.node.properties['permissions'] = permissions

        if runtime_properties is None:
            runtime_properties = {}
        ctx.instance.runtime_properties = runtime_properties

        return ctx

    def configure_mock_connection(self,
                                  mock_conn):
        mock_client = Mock()

        mock_sess = Mock()
        mock_sess.client.return_value = mock_client
        mock_conn.S3ConnectionClient.return_value = mock_sess

        return mock_conn

    def make_bucket_relationship(self, bucket_name):
        relationship = Mock()
        relationship.type = (
            'cloudify.aws.relationships.s3_object_contained_in_bucket'
        )
        relationship.target.node.properties = {
            'name': bucket_name,
        }
        return relationship

    def test_get_bucket_details_fails_with_no_relationships(self):
        ctx = Mock()
        ctx.instance.relationships = []

        self.assertRaises(
            NonRecoverableError,
            object._get_bucket_details,
            ctx,
        )

    def test_get_bucket_details_fails_wrong_relationship(self):
        ctx = Mock()
        relationship = Mock()
        relationship.type = 'notright'
        ctx.instance.relationships = [relationship]

        self.assertRaises(
            NonRecoverableError,
            object._get_bucket_details,
            ctx,
        )

    @patch('cloudify_aws.s3.object.connection')
    def test_get_bucket_details(self, mock_conn):
        mock_conn = self.configure_mock_connection(
            mock_conn,
        )
        mock_client = (
            mock_conn.S3ConnectionClient.return_value.client.return_value
        )
        mock_client.get_bucket_acl.return_value = {
            'expected': 'yes',
            'ResponseMetadata': 'stuff',
        }

        bucket_name = 'testbucket'
        ctx = Mock()
        ctx.instance.relationships = [
            self.make_bucket_relationship(bucket_name=bucket_name)
        ]

        result_name, result_perms = object._get_bucket_details(ctx)

        self.assertEqual(result_name, bucket_name)
        self.assertEqual(result_perms, {'expected': 'yes'})
        mock_client.get_bucket_acl.assert_called_once_with(
            Bucket=bucket_name,
        )

    @patch('cloudify_aws.s3.object._get_bucket_details')
    @patch('cloudify_aws.s3.object.connection')
    def test_create_existing_fails(self, mock_conn, mock_details):
        mock_conn = self.configure_mock_connection(
            mock_conn,
        )
        mock_client = (
            mock_conn.S3ConnectionClient.return_value.client.return_value
        )

        bucket_name = 'goodbucket'
        bucket_permissions = {'bad_people': 'not_allowed'}
        mock_details.return_value = (
            bucket_name,
            bucket_permissions,
        )

        object_name = 'myobject'

        ctx = self.make_node_context(name=object_name)

        mock_client.list_objects.return_value = {
            'Contents': [
                {'Key': object_name},
            ],
        }

        self.assertRaises(
            NonRecoverableError,
            object.create,
            ctx,
        )
        self.assertFalse(ctx.instance.runtime_properties['created'])

    @patch('cloudify_aws.s3.object._get_bucket_details')
    @patch('cloudify_aws.s3.object.connection')
    def test_create_first_object_in_bucket(self, mock_conn, mock_details):
        mock_conn = self.configure_mock_connection(
            mock_conn,
        )
        mock_client = (
            mock_conn.S3ConnectionClient.return_value.client.return_value
        )

        bucket_name = 'goodbucket'
        bucket_permissions = {'bad_people': 'not_allowed'}
        mock_details.return_value = (
            bucket_name,
            bucket_permissions,
        )

        object_name = 'myobject'

        ctx = self.make_node_context(
            name=object_name,
            load_contents_from_file=False,
        )

        mock_client.list_objects.return_value = {}

        object.create(ctx)
        self.assertTrue(ctx.instance.runtime_properties['created'])

    @patch('cloudify_aws.s3.object._get_bucket_details')
    @patch('cloudify_aws.s3.object.connection')
    def test_create_with_permissions_preset(self, mock_conn, mock_details):
        mock_conn = self.configure_mock_connection(
            mock_conn,
        )
        mock_client = (
            mock_conn.S3ConnectionClient.return_value.client.return_value
        )

        bucket_name = 'goodbucket'
        bucket_permissions = {'bad_people': 'not_allowed'}
        mock_details.return_value = (
            bucket_name,
            bucket_permissions,
        )

        object_name = 'second'
        permissions = 'public'
        contents = ''
        content_type = 'type'

        ctx = self.make_node_context(
            name=object_name,
            load_contents_from_file=False,
            permissions=permissions,
            content_type=content_type,
            contents=contents,
        )

        mock_client.list_objects.return_value = {}

        object.create(ctx)
        mock_client.put_object.assert_called_once_with(
            Body=contents,
            Bucket=bucket_name,
            Key=object_name,
            ContentType=content_type,
            ACL=permissions,
        )
        self.assertTrue(ctx.instance.runtime_properties['created'])

    @patch('cloudify_aws.s3.object._get_bucket_details')
    @patch('cloudify_aws.s3.object.connection')
    def test_create_inherit_bucket_permissions(self, mock_conn, mock_details):
        mock_conn = self.configure_mock_connection(
            mock_conn,
        )
        mock_client = (
            mock_conn.S3ConnectionClient.return_value.client.return_value
        )

        bucket_name = 'goodbucket'
        bucket_permissions = {'bad_people': 'not_allowed'}
        mock_details.return_value = (
            bucket_name,
            bucket_permissions,
        )

        object_name = 'second'
        contents = ''
        content_type = 'type'

        ctx = self.make_node_context(
            name=object_name,
            load_contents_from_file=False,
            content_type=content_type,
            contents=contents,
        )

        mock_client.list_objects.return_value = {}

        object.create(ctx)
        mock_client.put_object.assert_called_once_with(
            Body=contents,
            Bucket=bucket_name,
            Key=object_name,
            ContentType=content_type,
        )
        mock_client.put_object_acl.assert_called_once_with(
            Bucket=bucket_name,
            Key=object_name,
            AccessControlPolicy=bucket_permissions,
        )
        self.assertTrue(ctx.instance.runtime_properties['created'])

    @patch('__builtin__.open')
    @patch('cloudify_aws.s3.object._get_bucket_details')
    @patch('cloudify_aws.s3.object.connection')
    def test_create_from_file(self, mock_conn, mock_details, mock_open):
        mock_conn = self.configure_mock_connection(
            mock_conn,
        )
        mock_client = (
            mock_conn.S3ConnectionClient.return_value.client.return_value
        )

        bucket_name = 'goodbucket'
        bucket_permissions = {'bad_people': 'not_allowed'}
        mock_details.return_value = (
            bucket_name,
            bucket_permissions,
        )

        object_name = 'second'
        filename = 'myfile'
        content_type = 'test'

        ctx = self.make_node_context(
            name=object_name,
            load_contents_from_file=True,
            content_type=content_type,
            contents=filename,
        )

        mock_client.list_objects.return_value = {}

        object.create(ctx)
        mock_client.put_object.assert_called_once_with(
            Body=mock_open.return_value,
            Bucket=bucket_name,
            Key=object_name,
            ContentType=content_type,
        )
        mock_client.put_object_acl.assert_called_once_with(
            Bucket=bucket_name,
            Key=object_name,
            AccessControlPolicy=bucket_permissions,
        )
        # Make sure we called open with the right file
        mock_open.assert_called_once_with(
            ctx.download_resource(filename)
        )
        # Make sure we closed the file
        mock_open.return_value.close.assert_called_once_with()
        self.assertTrue(ctx.instance.runtime_properties['created'])

    @patch('__builtin__.open')
    @patch('cloudify_aws.s3.object._get_bucket_details')
    @patch('cloudify_aws.s3.object.connection')
    def test_create_from_string(self, mock_conn, mock_details, mock_open):
        mock_conn = self.configure_mock_connection(
            mock_conn,
        )
        mock_client = (
            mock_conn.S3ConnectionClient.return_value.client.return_value
        )

        bucket_name = 'goodbucket'
        bucket_permissions = {'bad_people': 'not_allowed'}
        mock_details.return_value = (
            bucket_name,
            bucket_permissions,
        )

        object_name = 'second'
        contents = 'wonderful object in s3'
        content_type = 'test'

        ctx = self.make_node_context(
            name=object_name,
            load_contents_from_file=False,
            content_type=content_type,
            contents=contents,
        )

        mock_client.list_objects.return_value = {}

        object.create(ctx)
        mock_client.put_object.assert_called_once_with(
            Body=contents,
            Bucket=bucket_name,
            Key=object_name,
            ContentType=content_type,
        )
        self.assertEqual(mock_open.call_count, 0)
        self.assertTrue(ctx.instance.runtime_properties['created'])

    @patch('cloudify_aws.s3.object._get_bucket_details')
    @patch('cloudify_aws.s3.object.connection')
    def test_delete_not_attempted_when_not_created(self,
                                                   mock_conn,
                                                   mock_details):
        object_name = 'test'
        ctx = self.make_node_context(
            name=object_name,
            runtime_properties={'created': False},
        )

        bucket_name = 'goodbucket'
        bucket_permissions = {'bad_people': 'not_allowed'}
        mock_details.return_value = (
            bucket_name,
            bucket_permissions,
        )

        correct_error = False
        try:
            object.delete(ctx)
        except NonRecoverableError as err:
            correct_error = True
            self.assertIn(
                'failed, so it will not be deleted',
                str(err),
            )

        self.assertTrue(correct_error)

    @patch('cloudify_aws.s3.object._get_bucket_details')
    @patch('cloudify_aws.s3.object.connection')
    def test_delete_not_attempted_when_created_not_set(self,
                                                       mock_conn,
                                                       mock_details):
        object_name = 'test'
        ctx = self.make_node_context(
            name=object_name,
            runtime_properties={},
        )

        bucket_name = 'goodbucket'
        bucket_permissions = {'bad_people': 'not_allowed'}
        mock_details.return_value = (
            bucket_name,
            bucket_permissions,
        )

        correct_error = False
        try:
            object.delete(ctx)
        except NonRecoverableError as err:
            correct_error = True
            self.assertIn(
                'failed, so it will not be deleted',
                str(err),
            )

        self.assertTrue(correct_error)

    @patch('cloudify_aws.s3.object._get_bucket_details')
    @patch('cloudify_aws.s3.object.connection')
    def test_delete_key(self,
                        mock_conn,
                        mock_details):
        mock_conn = self.configure_mock_connection(
            mock_conn,
        )
        mock_client = (
            mock_conn.S3ConnectionClient.return_value.client.return_value
        )
        object_name = 'test'
        ctx = self.make_node_context(
            name=object_name,
            runtime_properties={'created': True},
        )

        bucket_name = 'goodbucket'
        bucket_permissions = {'bad_people': 'not_allowed'}
        mock_details.return_value = (
            bucket_name,
            bucket_permissions,
        )

        object.delete(ctx)

        mock_client.delete_object.assert_called_once_with(
            Bucket=bucket_name,
            Key=object_name,
        )

    @patch('cloudify_aws.s3.object._get_bucket_details')
    @patch('cloudify_aws.s3.object.connection')
    def test_delete_key_client_error(self,
                                     mock_conn,
                                     mock_details):
        mock_conn = self.configure_mock_connection(
            mock_conn,
        )
        mock_client = (
            mock_conn.S3ConnectionClient.return_value.client.return_value
        )
        mock_client.delete_object.side_effect = ClientError(
            {'Error': {'Message': 'ItAllWentWrong'}},
            'test_failure',
        )

        object_name = 'test'
        ctx = self.make_node_context(
            name=object_name,
            runtime_properties={'created': True},
        )

        bucket_name = 'goodbucket'
        bucket_permissions = {'bad_people': 'not_allowed'}
        mock_details.return_value = (
            bucket_name,
            bucket_permissions,
        )

        self.assertRaises(
            NonRecoverableError,
            object.delete,
            ctx,
        )

        mock_client.delete_object.assert_called_once_with(
            Bucket=bucket_name,
            Key=object_name,
        )
