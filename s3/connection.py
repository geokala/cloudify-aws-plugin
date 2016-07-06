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

# Builtin Imports
import os
import ConfigParser

# Third-party Imports
from boto import connect_s3

# Cloudify Imports
from ec2 import utils
from ec2 import constants
from cloudify.exceptions import NonRecoverableError


class S3ConnectionClient():
    """Provides functions for getting the S3 Client
    """

    def __init__(self):
        self.connection = None

    def client(self):
        """Represents the S3Connection Client
        """

        aws_config = (self._get_aws_config_property() or
                               self._get_aws_config_from_file())
        if aws_config:
            return connect_s3(
                aws_config['aws_access_key_id'],
                aws_config['aws_secret_access_key'],
            )
        else:
            return connect_s3()

    def _get_aws_config_property(self):
        node_properties = \
            utils.get_instance_or_source_node_properties()
        return node_properties[constants.AWS_CONFIG_PROPERTY]

    def _get_aws_config_from_file(self):
        """Get aws config from a Boto cfg file
        """
        config_path = self._get_boto_config_file_path()
        return self._parse_config_file(config_path) if config_path else None

    def _get_boto_config_file_path(self):
        """Get aws config file path from environment
        """
        return os.environ.get(constants.AWS_CONFIG_PATH_ENV_VAR_NAME)

    def _parse_config_file(self, path):
        """Parse and validate Boto cfg file
        """
        path = str(path)
        if not os.path.isfile(path):
            raise NonRecoverableError('no aws config file at {0}'.format(path))

        parser = ConfigParser.ConfigParser()
        parser.read(path)

        if len(parser.sections()) == 0:
            raise NonRecoverableError("aws config file is empty")

        config_schema = constants.BOTO_CONFIG_SCHEMA
        config = {}

        # validate sections
        invalid_sections = \
            [s for s in parser.sections() if s not in config_schema]
        if invalid_sections:
            raise NonRecoverableError("Unsupported Boto section(s): {0}".
                                      format(', '.join(invalid_sections)))

        # validate options and populate config dict (option > value)
        invalid_options = []

        for section in parser.sections():
            allowed_opt_list = config_schema[section]
            for opt, value in parser.items(section):
                if opt in allowed_opt_list:
                    config[opt] = value
                else:
                    invalid_options.append((section, opt))

        if invalid_options:
            raise NonRecoverableError("Unsupported Boto option(s): {0}".
                                      format(invalid_options))

        return config
