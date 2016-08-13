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
import os

from cloudify.state import current_ctx
from cloudify.mocks import MockCloudifyContext
from cloudify.exceptions import NonRecoverableError

from ec2 import constants
from cloudify_aws.s3 import connection


class TestConnection(testtools.TestCase):

    def setUp(self):
        super(TestConnection, self).setUp()

    def set_mock_context(self, test_name, properties):
        """ Sets a mock context."""
        ctx = MockCloudifyContext(
            node_id=test_name,
            properties=properties,
        )

        current_ctx.set(ctx=ctx)

    def get_resources_file_path(self, resource):
        resources = os.path.join(
            os.path.split(__file__)[0],
            'resources',
        )
        return os.path.join(resources, resource)

    def test_no_connection_on_init(self):
        # Instantiating a class shouldn't result in outbound calls
        conn = connection.S3ConnectionClient()
        self.assertIsNone(conn.connection)

    def test_get_config_property_returns_key_from_context(self):
        expected = 'test'
        self.set_mock_context(
            test_name='get_config_property_returns_key',
            properties={
                constants.AWS_CONFIG_PROPERTY: expected
            }
        )
        conn = connection.S3ConnectionClient()
        self.assertEqual(conn._get_aws_config_property(), expected)

    def test_get_config_property_returns_none_missing_key(self):
        self.set_mock_context(
            test_name='get_config_property_returns_key',
            properties={},
        )
        conn = connection.S3ConnectionClient()
        self.assertIsNone(conn._get_aws_config_property())

    @patch('cloudify_aws.s3.connection.os')
    def test_get_boto_config_file_path_found_in_env(self, mock_os):
        expected = 'testpath'
        mock_os.environ = {
            constants.AWS_CONFIG_PATH_ENV_VAR_NAME: expected
        }
        conn = connection.S3ConnectionClient()
        self.assertEqual(conn._get_boto_config_file_path(), expected)

    @patch('cloudify_aws.s3.connection.os')
    def test_get_boto_config_file_path_not_found_in_env(self, mock_os):
        mock_os.environ = {}
        conn = connection.S3ConnectionClient()
        self.assertIsNone(conn._get_boto_config_file_path())

    def test_get_aws_config_from_file(self):
        expected = 'gotfromfile'
        fakefile = 'myfile'
        conn = connection.S3ConnectionClient()

        conn._parse_config_file = Mock()
        conn._parse_config_file.return_value = expected
        conn._get_boto_config_file_path = Mock()
        conn._get_boto_config_file_path.return_value = fakefile

        self.assertEqual(conn._get_aws_config_from_file(), expected)
        conn._parse_config_file.assert_called_once_with(fakefile)

    def test_get_aws_config_from_no_file(self):
        conn = connection.S3ConnectionClient()

        conn._parse_config_file = Mock()
        conn._get_boto_config_file_path = Mock()
        conn._get_boto_config_file_path.return_value = None

        self.assertIsNone(conn._get_aws_config_from_file())
        # We should not try to parse no file
        self.assertEqual(conn._parse_config_file.call_count, 0)

    def test_parse_config_file_not_file(self):
        conn = connection.S3ConnectionClient()
        self.assertRaises(
            NonRecoverableError,
            conn._parse_config_file,
            'not/a/real/file',
        )

    def test_parse_config_file_empty_file(self):
        conn = connection.S3ConnectionClient()
        self.assertRaises(
            NonRecoverableError,
            conn._parse_config_file,
            self.get_resources_file_path('empty'),
        )

    def test_parse_config_file_unsupported_sections(self):
        conn = connection.S3ConnectionClient()
        self.assertRaises(
            NonRecoverableError,
            conn._parse_config_file,
            self.get_resources_file_path('bad_sections_config'),
        )

    def test_parse_config_file_unsupported_options(self):
        conn = connection.S3ConnectionClient()
        self.assertRaises(
            NonRecoverableError,
            conn._parse_config_file,
            self.get_resources_file_path('bad_options_config'),
        )

    def test_parse_config_file(self):
        expected = {
            'aws_access_key_id': 'notreal',
            'aws_secret_access_key': 'hiddenbythestars',
        }
        conn = connection.S3ConnectionClient()
        result = conn._parse_config_file(
            self.get_resources_file_path('good_config'),
        )
        self.assertEqual(result, expected)

    @patch('cloudify_aws.s3.connection.boto3')
    def test_client_config_from_context_priority(self, mock_boto3):
        mock_boto3.Session = Mock()

        expected = {
            'aws_access_key_id': 'real',
            'aws_secret_access_key': 'concealedbyburningthings',
            'region': 'somewhere-directional-number',
        }
        self.set_mock_context(
            test_name='get_config_property_returns_key',
            properties={
                constants.AWS_CONFIG_PROPERTY: expected
            }
        )

        conn = connection.S3ConnectionClient()
        conn._get_boto_config_file_path = Mock()
        conn._get_boto_config_file_path.return_value = (
            self.get_resources_file_path('good_config')
        )
        conn._parse_config_file = Mock()

        conn.client()
        mock_boto3.Session.assert_called_once_with(
            expected['aws_access_key_id'],
            expected['aws_secret_access_key'],
            region_name=expected['region'],
        )
        # We should not try to parse the config if we have the context
        self.assertEqual(conn._parse_config_file.call_count, 0)

    @patch('cloudify_aws.s3.connection.boto3')
    def test_client_config_from_file_fallback(self, mock_boto3):
        mock_boto3.Session = Mock()
        expected = {
            'aws_access_key_id': 'notreal',
            'aws_secret_access_key': 'hiddenbythestars',
            'region': 'us-east-1',
        }

        self.set_mock_context(
            test_name='get_config_property_returns_key',
            properties={},
        )

        conn = connection.S3ConnectionClient()
        conn._get_boto_config_file_path = Mock()
        conn._get_boto_config_file_path.return_value = (
            self.get_resources_file_path('good_config')
        )
        conn.client()
        mock_boto3.Session.assert_called_once_with(
            expected['aws_access_key_id'],
            expected['aws_secret_access_key'],
            region_name=expected['region'],
        )

    @patch('cloudify_aws.s3.connection.boto3')
    def test_client_config_default_region(self, mock_boto3):
        mock_boto3.Session = Mock()
        expected = {
            'aws_access_key_id': 'notreal',
            'aws_secret_access_key': 'hiddenbythestars',
            'region': 'us-east-1',
        }
        context_props = {
            key: value for key, value in expected.items()
            if key != 'region'
        }
        self.set_mock_context(
            test_name='get_config_property_returns_key',
            properties={
                constants.AWS_CONFIG_PROPERTY: context_props,
            }
        )

        conn = connection.S3ConnectionClient()
        conn.client()

        mock_boto3.Session.assert_called_once_with(
            expected['aws_access_key_id'],
            expected['aws_secret_access_key'],
            region_name=expected['region'],
        )

    @patch('cloudify_aws.s3.connection.boto3')
    def test_client_config_no_aws_config(self, mock_boto3):
        mock_boto3.Session = Mock()
        self.set_mock_context(
            test_name='get_config_property_returns_key',
            properties={},
        )

        conn = connection.S3ConnectionClient()
        conn.client()

        mock_boto3.Session.assert_called_once_with()

    @patch('cloudify_aws.s3.connection.boto3')
    def test_client_returns_s3_client_no_creds(self, mock_boto3):
        mock_boto3.Session = Mock()
        mock_session_return = Mock()
        mock_session_return.client.return_value = 's3_client'
        mock_boto3.Session.return_value = mock_session_return
        self.set_mock_context(
            test_name='get_config_property_returns_key',
            properties={},
        )

        conn = connection.S3ConnectionClient()
        client = conn.client()

        self.assertEqual(client, 's3_client')
        mock_session_return.client.assert_called_once_with('s3')

    @patch('cloudify_aws.s3.connection.boto3')
    def test_client_returns_s3_client_with_creds(self, mock_boto3):
        mock_boto3.Session = Mock()
        mock_session_return = Mock()
        mock_session_return.client.return_value = 's3_client'
        mock_boto3.Session.return_value = mock_session_return
        self.set_mock_context(
            test_name='get_config_property_returns_key',
            properties={
                'aws_access_key_id': 'real',
                'aws_secret_access_key': 'concealedbyburningthings',
                'region': 'somewhere-directional-number',
            }
        )

        conn = connection.S3ConnectionClient()
        client = conn.client()

        self.assertEqual(client, 's3_client')
        mock_session_return.client.assert_called_once_with('s3')
