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

from cloudify_aws.s3 import bucket


class TestBucket(testtools.TestCase):

    def setUp(self):
        super(TestBucket, self).setUp()

    def make_node_context(self,
                          bucket_name='mybucket',
                          use_existing_resource=False,
                          permissions='public',
                          website_index_page='',
                          website_error_page='',
                          runtime_properties=None):
        ctx = Mock()
        ctx.node.properties = {
            'name': bucket_name,
            'use_existing_resource': use_existing_resource,
            'permissions': permissions,
            'website_index_page': website_index_page,
            'website_error_page': website_error_page,
        }

        if runtime_properties is None:
            runtime_properties = {}
        ctx.instance.runtime_properties = runtime_properties

        return ctx

    def configure_mock_connection(self,
                                  mock_conn,
                                  web_bucket=True,
                                  existing_buckets=(),
                                  bucket_region='eregion'):
        mock_client = Mock()
        mock_client.head_bucket.return_value = {
            'ResponseMetadata': {
                'HTTPHeaders': {
                    'x-amz-bucket-region': bucket_region
                },
            }
        }

        mock_client.list_buckets.return_value = {
            'Buckets': [{'Name': bucket} for bucket in existing_buckets],
        }

        mock_sess = Mock()
        mock_sess.client.return_value = mock_client
        mock_conn.S3ConnectionClient.return_value = mock_sess

        if not web_bucket:
            mock_client.get_bucket_website.side_effect = ClientError(
                {'Error': {'Message': 'NoSuchWebsiteConfiguration'}},
                'test_non_web_bucket'
            )

        return mock_conn

    def test_invalid_bucket_name_uppercase(self):
        self.assertRaises(
            NonRecoverableError,
            bucket.validate_bucket_name,
            'Badbucket',
        )

    def test_invalid_bucket_name_too_short(self):
        self.assertRaises(
            NonRecoverableError,
            bucket.validate_bucket_name,
            'aa',
        )

    def test_invalid_bucket_name_too_long(self):
        self.assertRaises(
            NonRecoverableError,
            bucket.validate_bucket_name,
            'a' * 64,
        )

    def test_invalid_bucket_name_hyphen_start(self):
        self.assertRaises(
            NonRecoverableError,
            bucket.validate_bucket_name,
            '-badbucket',
        )

    def test_invalid_bucket_name_hyphen_end(self):
        self.assertRaises(
            NonRecoverableError,
            bucket.validate_bucket_name,
            'badbucket-',
        )

    def test_invalid_bucket_name_contains_dot(self):
        self.assertRaises(
            NonRecoverableError,
            bucket.validate_bucket_name,
            'bad.bucket',
        )

    def test_valid_bucket_name(self):
        self.assertTrue(
            bucket.validate_bucket_name('1good-bucket')
        )

    @patch('cloudify_aws.s3.bucket.connection')
    def test_get_web_bucket_url(self, mock_conn):
        bucket_name = 'mybucket'

        mock_conn = self.configure_mock_connection(mock_conn)

        ctx = self.make_node_context(bucket_name=bucket_name)

        expected = 'http://mybucket.s3-website-eregion.amazonaws.com/'

        result = bucket._get_bucket_url(ctx)

        self.assertEqual(result, expected)

    @patch('cloudify_aws.s3.bucket.connection')
    def test_get_non_web_bucket_url(self, mock_conn):
        bucket_name = 'mybucket'
        mock_conn = self.configure_mock_connection(
            mock_conn,
            web_bucket=False,
        )

        ctx = self.make_node_context(bucket_name=bucket_name)

        expected = 'https://s3.amazonaws.com/mybucket/'

        result = bucket._get_bucket_url(ctx)

        self.assertEqual(result, expected)

    @patch('cloudify_aws.s3.bucket.connection')
    def test_get_web_bucket_other_failure(self, mock_conn):
        bucket_name = 'mybucket'
        mock_conn = self.configure_mock_connection(
            mock_conn,
            web_bucket=False,
        )

        ctx = self.make_node_context(bucket_name=bucket_name)

        mock_client = (
            mock_conn.S3ConnectionClient.return_value.client.return_value
        )

        mock_client.get_bucket_website.side_effect = ClientError(
            {'Error': {'Message': 'SadThingsHappening'}},
            'test_get_web_bucket_failure'
        )

        self.assertRaises(
            ClientError,
            bucket._get_bucket_url,
            ctx,
        )

    @patch('cloudify_aws.s3.bucket.connection')
    def test_create_bucket_existing_web(self, mock_conn):
        bucket_name = 'existing-1'
        mock_conn = self.configure_mock_connection(
            mock_conn,
            bucket_region='kalimdor-north-1',
            existing_buckets=[bucket_name],
        )

        ctx = self.make_node_context(
            bucket_name=bucket_name,
            use_existing_resource=True,
        )

        expected = (
            'http://existing-1.s3-website-kalimdor-north-1.amazonaws.com/'
        )

        bucket.create(ctx)

        url = ctx.instance.runtime_properties['url']

        self.assertEqual(url, expected)

        self.assertFalse(ctx.instance.runtime_properties['created'])

    @patch('cloudify_aws.s3.bucket.connection')
    def test_create_bucket_existing_not_web(self, mock_conn):
        bucket_name = 'existing-2'
        mock_conn = self.configure_mock_connection(
            mock_conn,
            existing_buckets=[bucket_name],
            web_bucket=False,
        )

        ctx = self.make_node_context(
            bucket_name=bucket_name,
            use_existing_resource=True,
        )

        expected = 'https://s3.amazonaws.com/existing-2/'

        bucket.create(ctx)

        url = ctx.instance.runtime_properties['url']

        self.assertEqual(url, expected)

        self.assertFalse(ctx.instance.runtime_properties['created'])

    @patch('cloudify_aws.s3.bucket.connection')
    def test_create_bucket_existing_does_not_exist(self, mock_conn):
        bucket_name = 'not-existing'
        mock_conn = self.configure_mock_connection(
            mock_conn,
            web_bucket=False,
        )

        ctx = self.make_node_context(
            bucket_name=bucket_name,
            use_existing_resource=True,
        )

        self.assertRaises(
            NonRecoverableError,
            bucket.create,
            ctx,
        )

        self.assertFalse(ctx.instance.runtime_properties['created'])

    @patch('cloudify_aws.s3.bucket.connection')
    def test_create_bucket_existing_bad_name(self, mock_conn):
        # These should be allowed as our rules are slightly stricter than the
        # original AWS rules so that any buckets we create work entirely with
        # the current system (see notes on bucket name validation for details)
        bucket_name = 'existing.bad.name'
        mock_conn = self.configure_mock_connection(
            mock_conn,
            existing_buckets=[bucket_name],
            web_bucket=False,
        )

        ctx = self.make_node_context(
            bucket_name=bucket_name,
            use_existing_resource=True,
        )

        expected = 'https://s3.amazonaws.com/existing.bad.name/'

        bucket.create(ctx)

        url = ctx.instance.runtime_properties['url']

        self.assertEqual(url, expected)

        self.assertFalse(ctx.instance.runtime_properties['created'])

    @patch('cloudify_aws.s3.bucket.connection')
    def test_create_bucket_bad_name(self, mock_conn):
        bucket_name = 'badly.named'
        mock_conn = self.configure_mock_connection(
            mock_conn,
            web_bucket=False,
        )

        ctx = self.make_node_context(bucket_name=bucket_name)

        self.assertRaises(
            NonRecoverableError,
            bucket.create,
            ctx,
        )

        self.assertFalse(ctx.instance.runtime_properties['created'])

    @patch('cloudify_aws.s3.bucket.connection')
    def test_create_bucket_new_but_exists(self, mock_conn):
        bucket_name = 'should-not-exist'
        mock_conn = self.configure_mock_connection(
            mock_conn,
            existing_buckets=[bucket_name],
            web_bucket=False,
        )

        ctx = self.make_node_context(
            bucket_name=bucket_name,
        )

        self.assertRaises(
            NonRecoverableError,
            bucket.create,
            ctx,
        )

        self.assertFalse(ctx.instance.runtime_properties['created'])

    @patch('cloudify_aws.s3.bucket.connection')
    def test_create_bucket_not_web(self, mock_conn):
        bucket_name = 'new-1'
        mock_conn = self.configure_mock_connection(
            mock_conn,
            web_bucket=False,
        )

        ctx = self.make_node_context(
            bucket_name=bucket_name,
            permissions='thinkhappythoughts',
        )

        expected = 'https://s3.amazonaws.com/new-1/'

        bucket.create(ctx)

        url = ctx.instance.runtime_properties['url']

        self.assertEqual(url, expected)

        mock_client = (
            mock_conn.S3ConnectionClient.return_value.client.return_value
        )

        mock_client.create_bucket.assert_called_once_with(
            Bucket=bucket_name,
            ACL='thinkhappythoughts',
        )

        self.assertEqual(mock_client.put_bucket_website.call_count, 0)

        self.assertTrue(ctx.instance.runtime_properties['created'])

    @patch('cloudify_aws.s3.bucket.connection')
    def test_create_bucket_already_created(self, mock_conn):
        # Make sure we don't clear the 'created' flag if we're asked to create
        # a bucket we already created (in case someone accidentally calls
        # install twice, for example)
        bucket_name = 'already-1'
        mock_conn = self.configure_mock_connection(
            mock_conn,
            web_bucket=False,
            existing_buckets=[bucket_name]
        )

        ctx = self.make_node_context(
            bucket_name=bucket_name,
            permissions='thinkhappythoughts',
            runtime_properties={'created': True},
        )

        expected = 'https://s3.amazonaws.com/already-1/'

        bucket.create(ctx)

        url = ctx.instance.runtime_properties['url']

        self.assertEqual(url, expected)

        mock_client = (
            mock_conn.S3ConnectionClient.return_value.client.return_value
        )

        self.assertEqual(mock_client.create_bucket.call_count, 0)

        self.assertEqual(mock_client.put_bucket_website.call_count, 0)

        self.assertTrue(ctx.instance.runtime_properties['created'])

    @patch('cloudify_aws.s3.bucket.connection')
    def test_create_bucket_client_error(self, mock_conn):
        bucket_name = 'new-1'
        mock_conn = self.configure_mock_connection(
            mock_conn,
            web_bucket=False,
        )

        ctx = self.make_node_context(
            bucket_name=bucket_name,
        )

        mock_client = (
            mock_conn.S3ConnectionClient.return_value.client.return_value
        )

        mock_client.create_bucket.side_effect = ClientError(
            {'Error': {'Message': 'ItAllWentWrong'}},
            'test_failure',
        )

        self.assertRaises(
            NonRecoverableError,
            bucket.create,
            ctx,
        )

        self.assertFalse(ctx.instance.runtime_properties['created'])

    @patch('cloudify_aws.s3.bucket.connection')
    def test_create_bucket_web_index_but_not_error(self, mock_conn):
        bucket_name = 'brokeweb-1'
        mock_conn = self.configure_mock_connection(
            mock_conn,
            web_bucket=False,
        )

        ctx = self.make_node_context(
            bucket_name=bucket_name,
            website_index_page='test',
        )

        self.assertRaises(
            NonRecoverableError,
            bucket.create,
            ctx,
        )

        # Although there was an error, a bucket will have been created
        self.assertTrue(ctx.instance.runtime_properties['created'])

    @patch('cloudify_aws.s3.bucket.connection')
    def test_create_bucket_web_error_but_not_index(self, mock_conn):
        bucket_name = 'brokeweb-2'
        mock_conn = self.configure_mock_connection(
            mock_conn,
            web_bucket=False,
        )

        ctx = self.make_node_context(
            bucket_name=bucket_name,
            website_error_page='test',
        )

        self.assertRaises(
            NonRecoverableError,
            bucket.create,
            ctx,
        )

        # Although there was an error, a bucket will have been created
        self.assertTrue(ctx.instance.runtime_properties['created'])

    @patch('cloudify_aws.s3.bucket.connection')
    def test_create_bucket_web_invalid_index(self, mock_conn):
        bucket_name = 'brokeweb-3'
        mock_conn = self.configure_mock_connection(
            mock_conn,
            web_bucket=False,
        )

        ctx = self.make_node_context(
            bucket_name=bucket_name,
            website_index_page='test/index.html',
            website_error_page='error.html',
        )

        self.assertRaises(
            NonRecoverableError,
            bucket.create,
            ctx,
        )

        # Although there was an error, a bucket will have been created
        self.assertTrue(ctx.instance.runtime_properties['created'])

    @patch('cloudify_aws.s3.bucket.connection')
    def test_create_bucket_web(self, mock_conn):
        bucket_name = 'web-1'
        index = 'index.html'
        error = 'error.html'
        mock_conn = self.configure_mock_connection(
            mock_conn,
            bucket_region='krasia-nw-1',
        )

        ctx = self.make_node_context(
            bucket_name=bucket_name,
            permissions='thinkwebbythoughts',
            website_index_page='index.html',
            website_error_page='error.html',
        )

        expected = 'http://web-1.s3-website-krasia-nw-1.amazonaws.com/'

        bucket.create(ctx)

        url = ctx.instance.runtime_properties['url']

        self.assertEqual(url, expected)

        mock_client = (
            mock_conn.S3ConnectionClient.return_value.client.return_value
        )

        mock_client.create_bucket.assert_called_once_with(
            Bucket=bucket_name,
            ACL='thinkwebbythoughts',
        )

        mock_client.put_bucket_website.assert_called_once_with(
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

        self.assertTrue(ctx.instance.runtime_properties['created'])

    @patch('cloudify_aws.s3.bucket.connection')
    def test_delete_bucket_existing(self, mock_conn):
        bucket_name = 'leave-existing-1'
        mock_conn = self.configure_mock_connection(
            mock_conn,
        )

        ctx = self.make_node_context(
            bucket_name=bucket_name,
            use_existing_resource=True,
        )

        bucket.delete(ctx)

        # We shouldn't connect to AWS at all if we are 'deleting' pre-existing
        # resources
        self.assertEqual(mock_conn.S3ConnectionClient.call_count, 0)

    @patch('cloudify_aws.s3.bucket.connection')
    def test_delete_bucket_not_created(self, mock_conn):
        bucket_name = 'leave-not-created-1'
        mock_conn = self.configure_mock_connection(
            mock_conn,
            web_bucket=False,
        )

        ctx = self.make_node_context(
            bucket_name=bucket_name,
            runtime_properties={'created': False}
        )

        correct_error = False
        try:
            bucket.delete(ctx)
        except NonRecoverableError as err:
            correct_error = True
            self.assertIn('creation failed', str(err))

        self.assertTrue(correct_error)

    @patch('cloudify_aws.s3.bucket.connection')
    def test_delete_bucket_default_created(self, mock_conn):
        bucket_name = 'leave-not-created-2'
        mock_conn = self.configure_mock_connection(
            mock_conn,
            web_bucket=False,
        )

        ctx = self.make_node_context(
            bucket_name=bucket_name,
        )

        correct_error = False
        try:
            bucket.delete(ctx)
        except NonRecoverableError as err:
            correct_error = True
            self.assertIn('creation failed', str(err))

        self.assertTrue(correct_error)

    @patch('cloudify_aws.s3.bucket.connection')
    def test_delete_bucket_successfully(self, mock_conn):
        bucket_name = 'delete-1'
        mock_conn = self.configure_mock_connection(
            mock_conn,
        )

        ctx = self.make_node_context(
            bucket_name=bucket_name,
            runtime_properties={'created': True},
        )

        bucket.delete(ctx)

        mock_client = (
            mock_conn.S3ConnectionClient.return_value.client.return_value
        )

        mock_client.delete_bucket.assert_called_once_with(
            Bucket=bucket_name,
        )

    @patch('cloudify_aws.s3.bucket.connection')
    def test_delete_bucket_failure(self, mock_conn):
        bucket_name = 'delete-1'
        mock_conn = self.configure_mock_connection(
            mock_conn,
        )

        ctx = self.make_node_context(
            bucket_name=bucket_name,
            runtime_properties={'created': True},
        )

        mock_client = (
            mock_conn.S3ConnectionClient.return_value.client.return_value
        )
        mock_client.delete_bucket.side_effect = ClientError(
            {'Error': {'Message': 'ItAllWentWrong'}},
            'test_failure',
        )

        self.assertRaises(
            NonRecoverableError,
            bucket.delete,
            ctx,
        )
