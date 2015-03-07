########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
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

# Boto Imports
import boto.exception

# Cloudify imports
from cloudify import ctx
from cloudify.exceptions import NonRecoverableError
from cloudify.decorators import operation
from ec2 import connection
from ec2 import utils
from ec2 import constants


@operation
def create(**_):
    ec2_client = connection.EC2ConnectionClient().client()

    if create_external_securitygroup(ctx=ctx):
        return

    if 'description' not in ctx.node.properties:
        raise NonRecoverableError(
            'Required description not in security group properties.')

    if 'rules' not in ctx.node.properties:
        raise NonRecoverableError(
            'Required rules not in security group properties.')

    name = ctx.node.properties['resource_id']
    description = ctx.node.properties['description']
    rules = ctx.node.properties['rules']

    ctx.logger.debug('Creating Security Group: {0}'.format(name))

    try:
        group_object = ec2_client.create_security_group(name, description)
    except (boto.exception.EC2ResponseError,
            boto.exception.BotoServerError) as e:
        raise NonRecoverableError('{0}.'.format(str(e)))

    utils.set_external_resource_id(
        group_object.id, external=False, ctx=ctx)
    authorize_by_id(ec2_client, group_object.id, rules)


@operation
def delete(**_):
    ec2_client = connection.EC2ConnectionClient().client()

    group_id = utils.get_external_resource_id_or_raise(
        'delete security group', ctx=ctx)

    ctx.logger.debug('Deleting Security Group: {0}'.format(group_id))

    try:
        ec2_client.delete_security_group(group_id=group_id)
    except (boto.exception.EC2ResponseError,
            boto.exception.BotoServerError) as e:
        raise NonRecoverableError('{0}'.format(str(e)))

    securitygroup = utils.get_security_group_from_id(group_id, ctx=ctx)

    if not securitygroup:
        del(ctx.instance.runtime_properties[constants.EXTERNAL_RESOURCE_ID])
        ctx.logger.info('Deleted Security Group: {0}.'.format(group_id))
    else:
        return ctx.operation.retry(
            message='Verifying that Security Group {0} '
            'has been deleted from your account.'.format(securitygroup.id))


@operation
def creation_validation(**_):
    """ This checks that all user supplied info is valid """
    required_properties = ['resource_id', 'use_external_resource',
                           'rules']

    for property_key in required_properties:
        utils.validate_node_property(property_key, ctx=ctx)

    if ctx.node.properties['use_external_resource']:
        if not utils.get_security_group_from_id(
                ctx.node.properties['resource_id'], ctx=ctx):
            raise NonRecoverableError('use_external_resource was specified, '
                                      'but the security group does not exist.')


@operation
def authorize(**_):
    ec2_client = connection.EC2ConnectionClient().client()

    group_id = utils.get_external_resource_id_or_raise(
        'authorize security group', ctx=ctx)

    if 'rules' not in ctx.node.properties:
        raise NonRecoverableError(
            'No rules provided. Unable to authorize security group.')

    if ctx.node.properties['use_external_resource']:
        ctx.logger.info(
            'Not authorizing security group, '
            'because use_external_resource is true.')
        return

    ctx.logger.debug('Attempting to authorize security group.')

    rules = ctx.node.properties['rules']
    authorize_by_id(ec2_client, group_id, rules)


def authorize_by_id(ec2_client, group, rules):
    """ For each rule listed in the blueprint,
        this will add the rule to the group with the given id.
    """

    for r in rules:
        try:
            ec2_client.authorize_security_group(
                group_id=group,
                ip_protocol=r['ip_protocol'],
                from_port=r['from_port'],
                to_port=r['to_port'],
                cidr_ip=r['cidr_ip'])
        except (boto.exception.EC2ResponseError,
                boto.exception.BotoServerError) as e:
            raise NonRecoverableError('{0}'.format(str(e)))


def create_external_securitygroup(ctx):

    if not ctx.node.properties['use_external_resource']:
        return False
    else:
        group_id = ctx.node.properties['resource_id']
        group = utils.get_security_group_from_id(group_id, ctx=ctx)
        if not group:
            raise NonRecoverableError(
                'External security group was indicated, but the given '
                'security group or Name does not exist.')
        utils.set_external_resource_id(group.id, ctx=ctx)
        return True
