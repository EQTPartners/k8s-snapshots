#!/usr/bin/env python3
"""Written in asyncio as a learning experiment. Python because the
backup expiration logic is already in tarsnapper and well tested.
"""
import asyncio
import functools
import json
import os
import signal
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Optional

import attr
import confcollect
import logbook
import pendulum
import pykube
import structlog
from aiochannel import Channel, ChannelEmpty
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials
from tarsnapper.config import parse_deltas, ConfigError
from tarsnapper.expire import expire

from k8s_snapshots.asyncutils import combine_latest, exec
from k8s_snapshots import errors

# TODO: prevent a backup loop: A failsafe mechanism to make sure we
#   don't create more than x snapshots per disk; in case something
#   is wrong with the code that loads the exsting snapshots from GCloud.
# TODO: Support http ping after every backup.
# TODO: Support loading configuration from a configmap.
# TODO: We could use a third party resource type, too.

_logger = structlog.get_logger('daemon')

_shutdown = False

DEFAULT_CONFIG = {
    'structlog_dev': False,
    'log_level': 'INFO',
    'gcloud_project': '',
    'gcloud_json_keyfile_name': '',
    'gcloud_json_keyfile_string': '',
    'kube_config_file': '',
    'use_claim_name': False,
    'deltas_annotation_key': 'backup.kubernetes.io/deltas'
}


def validate_config(config):
    required_keys = ('gcloud_project',)

    result = True
    for key in required_keys:
        if not config.get(key):
            _logger.error(
                'Environment variable is required',
                key=key.upper())
            result = False

    return result


class Context:

    def __init__(self, config=None):
        self.config = DEFAULT_CONFIG.copy()
        if config:
            self.config.update(config)
        self.kube = self.make_kubeclient()
        self.gcloud = self.make_gclient()

    def make_kubeclient(self):
        cfg = None

        kube_config_file = self.config.get('kube_config_file')

        if kube_config_file:
            _logger.info('Loading kube config', file=kube_config_file)
            cfg = pykube.KubeConfig.from_file(kube_config_file)

        if not cfg:
            # See where we can get it from.
            default_file = os.path.expanduser('~/.kube/config')
            if os.path.exists(default_file):
                _logger.info('Loading default kube config', file=default_file)
                cfg = pykube.KubeConfig.from_file(default_file)

        # Maybe we are running inside Kubernetes.
        if not cfg:
            _logger.info('Loading kube config from service account')
            cfg = pykube.KubeConfig.from_service_account()

        return pykube.HTTPClient(cfg)

    def make_gclient(self):
        SCOPES = 'https://www.googleapis.com/auth/compute'
        credentials = None

        if self.config.get('gcloud_json_keyfile_name'):
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                self.config.get('gcloud_json_keyfile_name'),
                scopes=SCOPES)

        if self.config.get('gcloud_json_keyfile_string'):
            keyfile = json.loads(self.config.get('gcloud_json_keyfile_string'))
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                keyfile, scopes=SCOPES)

        if not credentials:
            raise RuntimeError("Auth for Google Cloud was not configured")

        compute = discovery.build('compute', 'v1', credentials=credentials)
        return compute


@attr.s(slots=True)
class Rule:
    """
    A rule describes how and when to make backups.
    """

    name = attr.ib()
    namespace = attr.ib()
    deltas = attr.ib()
    deltas_unparsed = attr.ib()
    gce_disk = attr.ib()
    gce_disk_zone = attr.ib()
    claim_name = attr.ib()

    @property
    def pretty_name(self):
        return self.claim_name or self.name

    def __str__ (self):
        return self.name


def filter_snapshots_by_rule(snapshots, rule):
    def match_disk(snapshot):
        url_part = '/zones/{zone}/disks/{name}'.format(
            zone=rule.gce_disk_zone, name=rule.gce_disk)
        return snapshot['sourceDisk'].endswith(url_part)
    return filter(match_disk, snapshots)


def determine_next_snapshot(snapshots, rules):
    """
    Given a list of snapshots, and a list of rules, determine the next snapshot
    to be made.

    Returns a 2-tuple (rule, target_datetime)
    """
    next_rule = None
    next_timestamp = None

    for rule in rules:
        # Find all the snapshots that match this rule
        filtered = filter_snapshots_by_rule(snapshots, rule)
        # Rewrite the list to snapshot
        filtered = map(lambda s: pendulum.parse(s['creationTimestamp']), filtered)
        # Sort by timestamp
        filtered = sorted(filtered, reverse=True)
        filtered = list(filtered)

        # There are no snapshots for this rule; create the first one.
        if not filtered:
            _logger.debug('No snapshot yet for rule, creating one now', rule=rule)
            next_rule = rule
            next_timestamp = pendulum.now('utc') + timedelta(seconds=10)
            break

        target = filtered[0] + rule.deltas[0]
        _logger.debug('Next snapshot found', rule=rule, target=target)
        if not next_timestamp or target < next_timestamp:
            next_rule = rule
            next_timestamp = target

    return next_rule, next_timestamp


def deltas_from_gce_pd(volume, api, annotation_key) -> Optional[str]:
    """
    Get the delta string based on a GCE PD volume.

    Parameters
    ----------

    volume : pykube.objects.PersistentVolume
        GCE PD Volume.
    api : pykube.HTTPClient
        Kubernetes client.
    annotation_key : str
        The annotation key to get deltas from..

    Returns
    -------
    The delta string

    """
    deltas_str = volume.annotations.get(annotation_key)

    _log = _logger.new(
        annotation_key=annotation_key,
        volume_name=volume.name,
        volume=volume.obj,
    )

    if deltas_str is not None and deltas_str:
        _log.info(
            'Found delta annotation for volume',
            deltas_str=deltas_str,
        )
        return deltas_str

    _log.debug(
        'Volume has no deltas annotation',
        deltas_str=deltas_str
    )

    # If volume is not annotated, attempt ot read deltas from
    # PersistentVolumeClaim referenced in volume.claimRef

    claim_ref = volume.obj['spec'].get('claimRef')
    _log = _log.bind(claim_ref=claim_ref)

    if claim_ref is None:
        _log.debug(
            'Volume has no claimRef',
        )
        return

    volume_claim = (
        pykube.objects.PersistentVolumeClaim.objects(api)
        .filter(namespace=claim_ref['namespace'])
        .get_or_none(name=claim_ref['name'])
    )
    _log = _log.bind(
        volume_claim_name=volume_claim.name,
        volume_claim=volume_claim.obj,
    )

    if volume_claim is None:
        _log.debug(
            'Volume claim does not exist',
        )
        return

    deltas_str = volume_claim.annotations.get(annotation_key)

    if deltas_str is not None and deltas_str:
        _log.debug(
            'Found deltas for volume claim',
            deltas_str=deltas_str,
        )
        return deltas_str

    _log.debug(
        'Volume claim has no deltas annotation',
        deltas_str=deltas_str,
    )
    return


def rule_from_pv(volume, api, deltas_annotation_key, use_claim_name=False):
    """Given a persistent volume object, create a backup role
    object. Can return None if this volume is not configured for
    backups, or is not suitable.

    Parameters

    `use_claim_name` - if the persistent volume is bound, and it's
    name is auto-generated, then prefer to use the name of the claim
    for the snapshot.
    """
    _log = _logger.new(
        volume_name=volume.name,
        volume=volume.obj,
    )

    provider = volume.annotations.get('pv.kubernetes.io/provisioned-by')
    _log = _log.new(provider=provider)
    if provider != 'kubernetes.io/gce-pd':
        _log.debug('Volume not a GCE persistent disk', volume=volume)
        return

    deltas_unparsed = deltas_from_gce_pd(volume, api, deltas_annotation_key)
    _log = _log.bind(deltas_str=deltas_unparsed)
    if deltas_unparsed is None:
        _log.info('No deltas found for volume')
        return

    try:
        _log.debug('Parsing deltas', unparsed_deltas=deltas_unparsed)
        deltas = parse_deltas(deltas_unparsed)

        if not deltas:
            raise ConfigError(
                'parse_deltas did not raise, but returned invalid deltas: '
                '{!r}'.format(deltas)
            )
    except ConfigError as e:
        _log.error(
            'Volume deltas are not valid',
            error=e,
        )
        return

    gce_disk = volume.obj['spec']['gcePersistentDisk']['pdName']


    # How can we know the zone? In theory, the storage class can
    # specify a zone; but if not specified there, K8s can choose a
    # random zone within the master region. So we really can't trust
    # that value anyway.
    # There is a label that gives a failure region, but labels aren't
    # really a trustworthy source for this.
    # Apparently, this is a thing in the Kubernetes source too, see:
    # getDiskByNameUnknownZone in pkg/cloudprovider/providers/gce/gce.go,
    # e.g. https://github.com/jsafrane/kubernetes/blob/2e26019629b5974b9a311a9f07b7eac8c1396875/pkg/cloudprovider/providers/gce/gce.go#L2455
    gce_disk_zone = volume.labels.get('failure-domain.beta.kubernetes.io/zone')

    claim_name = None
    if use_claim_name and volume.obj['spec'].get('claimRef'):
        if volume.annotations.get('kubernetes.io/createdby') == 'gce-pd-dynamic-provisioner':
            ref = volume.obj['spec'].get('claimRef')
            claim_name = '{1}--{0}'.format(ref['name'], ref['namespace'])

    rule = Rule(
        name=volume.name,
        namespace=volume.namespace,
        deltas=deltas,
        deltas_unparsed=deltas_unparsed,
        gce_disk=gce_disk,
        gce_disk_zone=gce_disk_zone,
        claim_name=claim_name,
    )

    return rule


def sync_get_rules(ctx):
    rules = {}
    api = ctx.make_kubeclient()

    _logger.debug('Observe persistent volume stream')
    stream = pykube.objects.PersistentVolume.objects(api).watch().object_stream()

    for event in stream:
        volume_name = event.object.name
        _log = _logger.new(
            volume_name=volume_name,
            watch_event_type=event.type,
            watch_event_object=event.object,
        )

        _log.debug('Event in persistent volume stream')

        if event.type == 'ADDED' or event.type == 'MODIFIED':
            rule = rule_from_pv(
                event.object,
                api,
                ctx.config.get('deltas_annotation_key'),
                use_claim_name=ctx.config.get('use_claim_name'))

            if rule:
                if event.type == 'ADDED' or not volume_name in rules:
                    _log.info(
                        'Volume added to list of backup jobs',
                        deltas_str=rule.deltas_unparsed
                    )
                else:
                    _log.info('Backup job for volume was updated')
                rules[volume_name] = rule
            else:
                if volume_name in rules:
                    _log.info('Volume removed from list of backup jobs')
                rules.pop(volume_name, False)

        if event.type == 'DELETED':
            rules.pop(volume_name, False)

        yield list(rules.values())


async def get_rules(ctx):
    _logger.debug('Getting rules')

    channel = Channel()
    _log = _logger.new(channel=channel)

    def worker():
        try:
            _log.debug('Iterating in thread')
            for value in sync_get_rules(ctx):
                channel.put(value)

        except errors.WorkerCancelledError:
            _log.exception('Worker cancelled')
        finally:
            _log.debug('Closing channel')
            channel.close()

    thread = threading.Thread(
        target=worker,
        name='get_rules',
        daemon=True
    )
    _log = _log.bind(thread=thread)

    _log.debug('Starting iterator thread')
    thread.start()

    try:
        async for item in channel:
            _log.debug('Waiting for new item')
            yield ctx.config.get('rules') + item

    except asyncio.CancelledError:
        _log.exception('Cancelled worker')
    except Exception:
        _log.exception('Unhandled exception while iterating')
    finally:
        _log.debug('Iterator exiting')
        #thread.join(1)



async def load_snapshots(ctx):
    r = await exec(ctx.gcloud.snapshots().list(project=ctx.config['gcloud_project']).execute)
    return r.get('items', [])


async def get_snapshots(ctx, reload_trigger):
    """Query the existing snapshots from Google Cloud.

    If the channel "reload_trigger" contains any value, we
    refresh the list of snapshots. This will then cause the
    next backup to be scheduled.
    """
    yield await load_snapshots(ctx)
    async for _ in reload_trigger:
        yield await load_snapshots(ctx)


async def watch_schedule(ctx, trigger):
    """Continually yields the next backup to be created.

    It watches two input sources: the rules as defined by
    Kubernetes resources, and the existing snapshots, as returned
    from Google Cloud. If either of them change, a new backup
    is scheduled.
    """

    rulesgen = get_rules(ctx)
    snapgen = get_snapshots(ctx, trigger)
    combined = combine_latest(
        rules=rulesgen,
        snapshots=snapgen,
        defaults={'snapshots': None, 'rules': None})

    try:
        async for item in combined:
            rules = item.get('rules')
            snapshots = item.get('snapshots')

            # Never schedule before we don't have data from both rules and snapshots
            if rules is None or snapshots is None:
                continue

            yield determine_next_snapshot(snapshots, rules)
    except asyncio.CancelledError:
        _logger.exception('Caught CancelledError while watching schedule')


async def make_backup(ctx, rule):
    """Execute a single backup job.

    1. Create the snapshot
    2. Wait until the snapshot is finished.
    3. Expire old snapshots
    """
    name = '{}-{}'.format(rule.pretty_name, pendulum.now('utc').format('%d%m%y-%H%M%S'))
    _log = _logger.new(
        name=name,
        rule=rule)

    _log.info('Creating a snapshot')

    try:
        result = await exec(ctx.gcloud.disks().createSnapshot(
            disk=rule.gce_disk,
            project=ctx.config['gcloud_project'],
            zone=rule.gce_disk_zone,
            body={"name": name}).execute)
    except Exception:
        _log.exception('Error creating snapshot')
        raise errors.Snap()

    # Immediately after creating the snapshot, it sometimes seems to
    # take some seconds before it can be queried.
    await asyncio.sleep(10)

    _log.debug('Waiting for snapshot to be ready')
    while result['status'] in ('PENDING', 'UPLOADING', 'CREATING'):
        await asyncio.sleep(2)
        result = await exec(ctx.gcloud.snapshots().get(
            snapshot=name,
            project=ctx.config['gcloud_project']).execute)

    if not result['status'] == 'READY':
        _log.error('Snapshot status is unexpected',
                   result=result,
                   status=result['status'])
        return

    await expire_snapshots(ctx, rule)


async def expire_snapshots(ctx, rule):
    """
    Expire existing snapshots for the rule.
    """
    _logger.info('Expire existing snapshots')

    snapshots = await load_snapshots(ctx)
    snapshots = filter_snapshots_by_rule(snapshots, rule)
    snapshots = {s['name']: pendulum.parse(s['creationTimestamp']) for s in snapshots}

    to_keep = expire(snapshots, rule.deltas)
    _logger.info('ex',
        len(snapshots), len(to_keep))
    for snapshot_name in snapshots:
        if snapshot_name in to_keep:
            logbook.debug('Keeping snapshot {}', snapshot_name)
            continue

        if snapshot_name not in to_keep:
            logbook.info('Deleting snapshot {}', snapshot_name)
            result = await exec(ctx.gcloud.snapshots().delete(
                snapshot=snapshot_name,
                project=ctx.config['gcloud_project']).execute)


async def scheduler(ctx, scheduling_chan, snapshot_reload_trigger):
    """The "when to make a backup schedule" depends on the backup delta
    rules as defined in Kubernetes volume resources, and the existing
    snapshots.

    This simpy observes a stream of 'next planned backup' events and
    sends then to the channel given. Note that this scheduler
    doesn't plan multiple backups in advance. Only ever a single
    next backup is scheduled.
    """

    _logger.info('Started scheduler task')

    async for schedule in watch_schedule(ctx, snapshot_reload_trigger):
        _logger.debug('Scheduler determined a new target backup')
        await scheduling_chan.put(schedule)


async def backuper(ctx, scheduling_chan, snapshot_reload_trigger):
    """Will take tasks from the given queue, then execute the backup.
    """
    _logger.info('Started backup executor task')

    current_target_time = current_target_rule = None
    while True:
        await asyncio.sleep(0.1)

        try:
            current_target_rule, current_target_time = scheduling_chan.get_nowait()

            # Log a message
            if not current_target_time:
                _logger.info('schedule updated: No backups scheduled')
            else:
                _logger.info(
                    'schedule updated: Next backup changed',
                    rule=current_target_rule,
                    target_time=current_target_time,
                    target_time_utc=current_target_time.in_timezone('utc'),
                    diff=current_target_time.diff(),
                    difference_str=current_target_time.diff_for_humans(),
                )
        except ChannelEmpty:
            pass

        if not current_target_time:
            continue

        if pendulum.now('utc') > current_target_time:
            try:
                await make_backup(ctx, current_target_rule)
            finally:
                await snapshot_reload_trigger.put(True)
                current_target_time = current_target_rule = None


async def daemon(config, *, loop=None):
    """Main app; it runs two tasks; one schedules backups, the other
    one executes the.
    """
    loop = loop or asyncio.get_event_loop()

    ctx = Context(config)

    # Using this channel, we can trigger a refresh of the list of
    # disk snapshots in the Google Cloud.
    snapshot_reload_trigger = Channel()

    # The backup task consumes this channel for the next backup task.
    scheduling_chan = Channel()

    schedule_task = asyncio.ensure_future(
        scheduler(ctx, scheduling_chan, snapshot_reload_trigger))
    backup_task = asyncio.ensure_future(
        backuper(ctx, scheduling_chan, snapshot_reload_trigger))

    tasks = [schedule_task, backup_task]

    _logger.debug('Gathering tasks', tasks=tasks)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        _logger.exception(
            'Received CancelledError',
            tasks=tasks
        )

        for task in tasks:
            task.cancel()
            _logger.debug('daemon cancelled task', task=task)

        while True:
            finished, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED)

            _logger.debug(
                'task completed',
                finished=finished,
                pending=pending)

            if not pending:
                _logger.debug('all tasks done')
                raise


def read_volume_config():
    """Read the volume configuration from the environment
    """
    def read_volume(name):
        env_name = name.replace('-', '_').upper()
        deltas = os.environ.get('VOLUME_{}_DELTAS'.format(env_name))
        if not deltas:
            raise ConfigError('A volume {} was defined, but {} is not set'.format(name, env_name))

        zone = os.environ.get('VOLUME_{}_ZONE'.format(env_name))
        if not zone:
            raise ConfigError('A volume {} was defined, but {} is not set'.format(name, env_name))

        _logger.info('Loading env-defined volume', volume=name, deltas=deltas)

        rule = Rule()
        rule.name = name
        rule.namespace = ''
        rule.deltas = parse_deltas(deltas)
        rule.deltas_unparsed = deltas
        rule.gce_disk = name
        rule.gce_disk_zone = zone
        return rule

    volumes = filter(bool, map(lambda s: s.strip(), os.environ.get('VOLUMES', '').split(',')))
    config = {}
    config['rules'] = list(filter(bool, map(read_volume, volumes)))
    return config


def configure_logging(config):
    handler = logbook.StderrHandler(
        level=config['log_level'],
        format_string='{record.message}')

    handler.push_application()
    logger_factory = functools.partial(logbook.Logger, level=logbook.DEBUG)

    def add_severity(logger, method_name, event_dict):
        if method_name == 'warn':
            method_name = 'warning'

        event_dict['severity'] = method_name
        return event_dict

    def add_func_name(logger, method_rame, event_dict):
        record = event_dict.get('_record')
        if record is None:
            return event_dict

        event_dict['function'] = record.funcName

        return event_dict

    processors = [
        add_func_name,
        structlog.stdlib.add_logger_name,
    ]

    if config['structlog_dev']:
        structlog.configure(
            processors=processors + [
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M.%S"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.dev.ConsoleRenderer()  # <===
            ],
            context_class=dict,
            logger_factory=logger_factory,
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
    else:
        structlog.configure(
            processors=processors + [
                add_severity,
                structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M.%S"),
                structlog.processors.JSONRenderer(indent=2, sort_keys=True)
            ],
            context_class=dict,
            logger_factory=logger_factory
        )

    global _logger
    _logger = structlog.get_logger()


def main():
    config = DEFAULT_CONFIG.copy()
    config.update(confcollect.from_environ(by_defaults=DEFAULT_CONFIG))

    configure_logging(config)

    # Read manual volume definitions
    try:
        config.update(read_volume_config())
    except ValueError as exc:
        _logger.error('config.read-volume-config.error', error=exc)
        return 1

    if not validate_config(config):
        return 1

    _logger.bind(
        gcloud_projec=config['gcloud_project'],
        deltas_annotation_key=config['deltas_annotation_key'],
    )

    loop = asyncio.get_event_loop()

    main_task = asyncio.ensure_future(daemon(config))

    _log = _logger.new(loop=loop, main_task=main_task)

    def handle_signal(name, timeout=10):
        _log.info('Received signal', signal_name=name)
        print_tasks()

        if main_task.cancelled():
            _log.info('main task already cancelled, forcing a quit')
            return

        _log.info(
            'Cancelling main task',
            task_cancel=main_task.cancel()
        )

    for sig_name in ['SIGINT', 'SIGTERM']:
        loop.add_signal_handler(
            getattr(signal, sig_name),
            functools.partial(handle_signal, sig_name))

    loop.add_signal_handler(signal.SIGUSR1, print_tasks)

    try:
        loop.run_until_complete(main_task)
    except asyncio.CancelledError:
        _log.exception('main task cancelled')
    except Exception:
        _log.exception('Unhandled exception in main task')
    finally:
        loop.run_until_complete(shutdown(loop=loop))


def print_tasks():
    _logger.debug('print tasks', task=asyncio.Task.all_tasks())


async def shutdown(*, loop=None):
    loop = loop or asyncio.get_event_loop()

    global _shutdown
    if _shutdown:
        _logger.warning('Already shutting down')
        return

    _shutdown = True

    _logger.debug(
        'shutting down',
    )

    print_tasks()

    _logger.info('Shutdown complete')


if __name__ == '__main__':
    sys.exit(main() or 0)
