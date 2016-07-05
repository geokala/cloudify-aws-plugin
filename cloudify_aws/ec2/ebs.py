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

# Built in Imports
import datetime

# Third-party Imports
from boto import exception

# Cloudify imports
from cloudify_aws import utils, constants
from cloudify import ctx
from cloudify.exceptions import NonRecoverableError
from cloudify.decorators import operation
from cloudify_aws.base import AwsBaseNode, AwsBaseRelationship


@operation
def creation_validation(**_):
    return Ebs().creation_validation()


@operation
def create(args, **_):
    return Ebs().created()


@operation
def delete(**_):
    return Ebs().deleted()


@operation
def create_snapshot(args, **_):
    return Ebs().snapshot_created()


@operation
def attach(**_):
    return VolumeInstanceConnection().attached()


@operation
def detach(args, **_):
    return VolumeInstanceConnection().detached(args)


class VolumeInstanceConnection(AwsBaseRelationship):

    def __init__(self, client=None):
        super(VolumeInstanceConnection, self).__init__(client=client)
        self.not_found_error = constants.EBS['NOT_FOUND_ERROR']
        self.resource_id = None
        self.source_get_all_handler = {
            'function': self.client.get_all_volumes,
            'argument':
                '{0}_ids'.format(constants.EBS['AWS_RESOURCE_TYPE'])
        }

    def attached(self):

        if self.attach():
            return self.post_attach()

    def attach(self, **_):

        volume_id = \
            utils.get_external_resource_id_or_raise(
                    'attach volume', ctx.source.instance)
        instance_id = \
            utils.get_external_resource_id_or_raise(
                    'attach volume', ctx.target.instance)

        if ctx.source.node.properties[constants.ZONE] not in \
                ctx.target.instance.runtime_properties.get('placement'):
            ctx.logger.info(
                    'Volume Zone {0} and Instance Zone {1} do not match. '
                    'This may lead to an error.'.format(
                            ctx.source.node.properties[constants.ZONE],
                            ctx.target.instance.runtime_properties
                            .get('placement')
                    )
            )

        if self._attach_external_volume_or_instance(instance_id):
            return
        volume_object = self._get_volumes_from_id(volume_id)

        if not volume_object:
            raise NonRecoverableError(
                    'EBS volume {0} not found in account.'.format(volume_id))

        if constants.EBS['VOLUME_CREATING'] in volume_object.update():
            return ctx.operation.retry(
                    message='Waiting for volume to be ready. '
                            'Volume in state {0}'
                            .format(volume_object.status))
        elif constants.EBS['VOLUME_AVAILABLE'] not in volume_object.update():
            raise NonRecoverableError(
                    'Cannot attach Volume {0} because it is in state {1}.'
                    .format(volume_object.id, volume_object.status))

        attach_args = dict(
                volume_id=volume_id,
                instance_id=instance_id,
                device=ctx.source.node.properties['device']
        )

        return self.execute(self.client.attach_volume, attach_args,
                            raise_on_falsy=True)

    def _get_volumes_from_id(self, volume_id):
        """Returns the EBS Volume object for a given EBS Volume id.

        :param volume_id: The ID of an EBS Volume.
        :returns The boto EBS volume object.
        """

        volumes = self._get_volumes(list_of_volume_ids=volume_id)

        return volumes[0] if volumes else volumes

    def _get_volumes(self, list_of_volume_ids):
        """Returns a list of EBS Volumes for a given list of volume IDs.

        :param list_of_volume_ids: A list of EBS volume IDs.
        :returns A list of EBS objects.
        :raises NonRecoverableError: If Boto errors.
        """

        try:
            volumes = self.client.get_all_volumes(
                    volume_ids=list_of_volume_ids)
        except exception.EC2ResponseError as e:
            if constants.EBS['NOT_FOUND_ERROR'] in e:
                all_volumes = self.client.get_all_volumes()
                utils.log_available_resources(all_volumes)
            return None
        except exception.BotoServerError as e:
            raise NonRecoverableError('{0}'.format(str(e)))

        return volumes

    def detached(self, args):

        ctx.logger.info(
                'Attempting to detach {0} from {1}.'
                .format(self.source_resource_id, self.target_resource_id))

        if self.detach(args):
            return self.post_detach()

    def detach(self, args, **_):

        """ Detaches an EBS Volume created by Cloudify from an EC2 Instance
        that was also created by Cloudify.
        """

        volume_id = self.source_resource_id
        instance_id = self.target_resource_id

        if self._detach_external_volume_or_instance():
            return

        volume_object = self._get_volumes_from_id(volume_id)

        if not volume_object:
            raise NonRecoverableError(
                    'EBS volume {0} not found in account.'.format(volume_id))

        detach_args = dict(
                volume_id=volume_id,
                instance_id=instance_id,
                device=ctx.source.node.properties['device']
        )
        if args:
            detach_args.update(args)

        try:
            detached = self.execute(self.client.detach_volume, detach_args,
                                    raise_on_falsy=True)
        except (exception.EC2ResponseError,
                exception.BotoServerError) as e:
            raise NonRecoverableError('{0}'.format(str(e)))

        if not detached:
            raise NonRecoverableError(
                    'Failed to detach volume {0} from instance {1}'
                    .format(volume_id, instance_id))

        utils.unassign_runtime_property_from_resource(
                'instance_id', ctx.source.instance)
        ctx.logger.info(
                'Detached volume {0} from instance {1}.'
                .format(volume_id, instance_id))

        return True

    def post_attach(self):

        ctx.source.instance.runtime_properties['instance_id'] = \
            self.target_resource_id

        ctx.logger.info(
                'Attached EBS volume {0} with instance {1}.'
                .format(self.source_resource_id, self.target_resource_id))

        return True

    def _detach_external_volume_or_instance(self):
        """Pretends to detach an external EBC volume with an EC2 instance
        but if one was not created by Cloudify, it just sets runtime_properties
        and exits the operation.

        :return False: At least one is a Cloudify resource. Continue operation.
        :return True: Both are External resources. Set runtime_properties.
            Ignore operation.
        """

        if not utils.use_external_resource(ctx.source.node.properties) \
                or not utils.use_external_resource(
                        ctx.target.node.properties):
            return False

        utils.unassign_runtime_property_from_resource(
                'instance_id', ctx.source.instance)
        ctx.logger.info(
                'Either instance or EBS volume is an external resource so not '
                'performing detach operation.')
        return True

    def post_detach(self):
        utils.unassign_runtime_property_from_resource(
                'instance_id', ctx.source.instance)
        ctx.logger.info(
                'Detached volume {0} from instance {1}.'
                .format(self.source_resource_id, self.target_resource_id))

        return True

    def _attach_external_volume_or_instance(self, instance_id):
        """Pretends to attach an external EBC volume with an EC2 instance
        but if one was not created by Cloudify, it just sets runtime_properties
        and exits the operation.

        :return False: At least one is a Cloudify resource. Continue operation.
        :return True: Both are External resources. Set runtime_properties.
            Ignore operation.
        """

        if not utils.use_external_resource(ctx.source.node.properties) \
                or not utils.use_external_resource(
                        ctx.target.node.properties):
            return False

        ctx.source.instance.runtime_properties['instance_id'] = \
            instance_id
        ctx.logger.info(
                'Either instance or volume is an external resource so not '
                'performing attach operation.')
        return True


class Ebs(AwsBaseNode):

    def __init__(self):
        super(Ebs, self).__init__(
                constants.EBS['AWS_RESOURCE_TYPE'],
                constants.EBS['REQUIRED_PROPERTIES']
        )
        self.not_found_error = constants.EBS['NOT_FOUND_ERROR']
        self.get_all_handler = {
            'function': self.client.get_all_volumes,
            'argument': '{0}_ids'.format(constants.EBS['AWS_RESOURCE_TYPE'])
        }

    def create(self, args=None, **_):
        """Creates an EBS volume.
        """

        if self._create_external_volume():
            return

        create_volume_args = dict(
                size=ctx.node.properties['size'],
                zone=ctx.node.properties[constants.ZONE]
        )
        if args:
            create_volume_args.update(args)

        ctx.logger.info('ARGS: {0}'.format(create_volume_args))

        new_volume = self.execute(self.client.create_volume,
                                  create_volume_args, raise_on_falsy=True)
        ctx.instance.runtime_properties[constants.ZONE] = new_volume.zone

        utils.set_external_resource_id(
                new_volume.id, ctx.instance, external=False)

        self.resource_id = new_volume.id

        return True

    def _create_external_volume(self):
        """If use_external_resource is True, this will set the runtime_properties,
        and then exit.

        :return False: Cloudify resource. Continue operation.
        :return True: External resource. Set runtime_properties.
        Ignore operation.
        """

        if not utils.use_external_resource(ctx.node.properties):
            return False

        volume_id = ctx.node.properties['resource_id']

        volume = self._get_volumes_from_id(volume_id)
        if not volume:
            raise NonRecoverableError(
                    'External EBS volume was indicated, but the '
                    'volume id does not exist.')
        utils.set_external_resource_id(volume.id, ctx.instance)
        return True

    def _get_volumes_from_id(self, volume_id):
        """Returns the EBS Volume object for a given EBS Volume id.

        :param volume_id: The ID of an EBS Volume.
        :returns The boto EBS volume object.
        """

        volumes = self._get_volumes(list_of_volume_ids=volume_id)

        return volumes[0] if volumes else volumes

    def _get_volumes(self, list_of_volume_ids):
        """Returns a list of EBS Volumes for a given list of volume IDs.

        :param list_of_volume_ids: A list of EBS volume IDs.
        :returns A list of EBS objects.
        :raises NonRecoverableError: If Boto errors.
        """

        try:
            volumes = self.client.get_all_volumes(
                    volume_ids=list_of_volume_ids)
        except exception.EC2ResponseError as e:
            if 'InvalidVolume.NotFound' in e:
                all_volumes = self.client.get_all_volumes()
                utils.log_available_resources(all_volumes)
            return None
        except exception.BotoServerError as e:
            raise NonRecoverableError('{0}'.format(str(e)))

        return volumes

    def delete(self, **_):
        """ Deletes an EBS Volume.
        """

        if self._delete_external_volume():
            return

        if not self._delete_volume():
            return ctx.operation.retry(
                    message='Failed to delete volume {0}.'
                    .format(self.resource_id))

        utils.unassign_runtime_property_from_resource(
                constants.ZONE, ctx.instance)

        utils.unassign_runtime_property_from_resource(
                constants.EXTERNAL_RESOURCE_ID, ctx.instance)

        return True

    def deleted(self):

        if self.delete():
            return self.post_delete()

    def _delete_volume(self):
        """

        :param volume_id:
        :return: True if the item is deleted,
        False if the item cannot be deleted yet.
        """

        volume_id = self.resource_id

        volume_to_delete = self._get_volumes_from_id(volume_id)

        if not volume_to_delete:
            ctx.logger.info(
                    'Volume id {0} does\'t exist.'
                    .format(volume_id))
            return True

        if volume_to_delete.status not in \
                constants.EBS['VOLUME_AVAILABLE'] or \
                volume_to_delete.status in \
                constants.EBS['VOLUME_IN_USE']:
            return False

        delete_args = dict(volume_id=volume_id)
        return self.execute(self.client.delete_volume,
                            delete_args, raise_on_falsy=True)

    def _delete_external_volume(self):
        """If use_external_resource is True, this will delete the runtime_properties,
        and then exit.

        :return False: Cloudify resource. Continue operation.
        :return True: External resource. Unset runtime_properties.
            Ignore operation.
        """

        if not utils.use_external_resource(ctx.node.properties):
            return False

        ctx.logger.info(
                'External resource. Not deleting EBS volume from account.')
        utils.unassign_runtime_property_from_resource(
                constants.EXTERNAL_RESOURCE_ID, ctx.instance)
        return True

    def create_snapshot(self, args=None, **_):
        """ Create a snapshot of an EBS Volume
        """

        volume_id = \
            utils.get_external_resource_id_or_raise(
                    'create snapshot', ctx.instance)

        ctx.logger.info(
                'Trying to create a snapshot of EBS volume {0}.'
                .format(volume_id))

        if not args:
            snapshot_desc = \
                unicode(datetime.datetime.now()) + \
                ctx.instance.runtime_properties[constants.EXTERNAL_RESOURCE_ID]
            args = dict(volume_id=volume_id, description=snapshot_desc)

        try:
            new_snapshot = self.execute(self.client.create_snapshot,
                                        args, raise_on_falsy=True)
        except (exception.EC2ResponseError,
                exception.BotoServerError) as e:
            raise NonRecoverableError('{0}'.format(str(e)))

        ctx.logger.info(
                'Created snapshot of EBS volume {0}.'.format(volume_id))

        if constants.EBS['VOLUME_SNAPSHOT_ATTRIBUTE'] not in \
                ctx.instance.runtime_properties:
            ctx.instance.runtime_properties[
                constants.EBS['VOLUME_SNAPSHOT_ATTRIBUTE']] = list()

        ctx.instance.runtime_properties[
            constants.EBS['VOLUME_SNAPSHOT_ATTRIBUTE']].append(new_snapshot.id)

    def snapshot_created(self):
        ctx.logger.info(
                'Attempting to create {0} {1}.'
                .format(self.aws_resource_type,
                        self.cloudify_node_instance_id))

        if self.create_snapshot():
            return True
