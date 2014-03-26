# coding=utf-8

"""
Collect the elasticsearch stats for the local node

#### Dependencies

 * urlib2

"""

import urllib2
import re

try:
    import json
    json  # workaround for pyflakes issue #13
except ImportError:
    import simplejson as json

import diamond.collector

RE_LOGSTASH_INDEX = re.compile('^(.*)-\d\d\d\d\.\d\d\.\d\d$')


def walk_rec(path, datas):
    for key, value in datas.items():
        if type(value) is dict:
            for kv in walk_rec(path + [key], value):
                yield kv
        else:
            yield path + [key], value


def walk(datas):
    for path, value in walk_rec([], datas):
        yield ".".join(path), value


class ElasticSearchCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(ElasticSearchCollector,
                            self).get_default_config_help()
        config_help.update({
            'host': "",
            'port': "",
            'stats': "Available stats: \n"
            + " - jvm (JVM information) \n"
            + " - thread_pool (Thread pool information) \n"
            + " - indices (Individual index stats)\n",
            'logstash_mode': "If 'indices' stats are gathered, remove "
            + "the YYYY.MM.DD suffix from the index name "
            + "(e.g. logstash-adm-syslog-2014.01.03) and use that "
            + "as a bucket for all 'day' index stats.",
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(ElasticSearchCollector, self).get_default_config()
        config.update({
            'host':     '127.0.0.1',
            'port':     9200,
            'path':     'elasticsearch',
            'stats':    ['jvm', 'thread_pool', 'indices'],
            'logstash_mode': False,
        })
        return config

    def _get(self, path):
        url = 'http://%s:%i/%s' % (
            self.config['host'], int(self.config['port']), path)
        try:
            response = urllib2.urlopen(url)
        except Exception, err:
            self.log.error("%s: %s", url, err)
            return False

        try:
            return json.load(response)
        except (TypeError, ValueError):
            self.log.error("Unable to parse response from elasticsearch as a"
                           + " json object")
            return False

    def _copy_one_level(self, metrics, prefix, data, filter=lambda key: True):
        for key, value in data.iteritems():
            if filter(key):
                metric_path = '%s.%s' % (prefix, key)
                self._set_or_sum_metric(metrics, metric_path, value)

    def _copy_two_level(self, metrics, prefix, data, filter=lambda key: True):
        for key1, d1 in data.iteritems():
            self._copy_one_level(metrics, '%s.%s' % (prefix, key1), d1, filter)

    def _index_metrics(self, metrics, prefix, index):
        if self.config['logstash_mode']:
            """Remove the YYYY.MM.DD bit from logstash indices.
            This way we keep using the same metric naming and not polute
            our metrics system (e.g. Graphite) with new metrics every day."""
            m = RE_LOGSTASH_INDEX.match(prefix)
            if m:
                prefix = m.group(1)

                # keep a telly of the number of indexes
                self._set_or_sum_metric(metrics,
                                        '%s.indexes_in_group' % prefix, 1)

        self._add_metric(metrics, '%s.docs.count' % prefix, index,
                         ['docs', 'count'])
        self._add_metric(metrics, '%s.docs.deleted' % prefix, index,
                         ['docs', 'deleted'])
        self._add_metric(metrics, '%s.datastore.size' % prefix, index,
                         ['store', 'size_in_bytes'])

        # publish all 'total' and 'time_in_millis' stats
        self._copy_two_level(
            metrics, prefix, index,
            lambda key: key.endswith('total') or key.endswith('time_in_millis'))

    def _add_metric(self, metrics, metric_path, data, data_path):
        """If the path specified by data_path (a list) exists in data,
        add to metrics.  Use when the data path may not be present"""
        current_item = data
        for path_element in data_path:
            current_item = current_item.get(path_element)
            if current_item is None:
                return

        self._set_or_sum_metric(metrics, metric_path, current_item)

    def _set_or_sum_metric(self, metrics, metric_path, value):
        """If we already have a datapoint for this metric, lets add
        the value. This is used when the logstash mode is enabled."""
        if metric_path in metrics:
            metrics[metric_path] += value
        else:
            metrics[metric_path] = value

    def collect(self):
        if json is None:
            self.log.error('Unable to import json')
            return {}
        self.collect_cluster()
        self.collect_health()
        self.collect_nodes()

    def collect_cluster(self):

        metrics = {}

        result = self._get('_cluster/stats')
        if result:
            cluster = result
            metrics['cluster.indices.count'] = cluster['indices']['count']
            metrics['cluster.indices.docs.count'] = cluster['indices']['docs']['count']
            metrics['cluster.indices.docs.deleted'] = cluster['indices']['docs']['deleted']
            metrics['cluster.indices.store.size'] = cluster['indices']['store']['size_in_bytes']
            metrics['cluster.indices.fielddata.memory_size'] = cluster['indices']['fielddata']['memory_size_in_bytes']
            metrics['cluster.indices.filter_cache.memory_size'] = cluster['indices']['filter_cache']['memory_size_in_bytes']
            metrics['cluster.indices.id_cache.memory_size'] = cluster['indices']['id_cache']['memory_size_in_bytes']
            metrics['cluster.indices.completion.size'] = cluster['indices']['completion']['size_in_bytes']
            metrics['cluster.indices.segments.count'] = cluster['indices']['segments']['count']
            metrics['cluster.indices.segments.memory'] = cluster['indices']['segments']['memory_in_bytes']
            metrics['cluster.indices.percolate.total'] = cluster['indices']['percolate']['total']
            metrics['cluster.indices.percolate.time'] = cluster['indices']['percolate']['time_in_millis']
            metrics['cluster.indices.percolate.current'] = cluster['indices']['percolate']['current']
            metrics['cluster.indices.percolate.memory_size'] = cluster['indices']['percolate']['memory_size_in_bytes']
            metrics['cluster.indices.percolate.queries'] = cluster['indices']['percolate']['queries']
            metrics['cluster.nodes.count.master_only'] = cluster['nodes']['count']['master_only']
            metrics['cluster.nodes.count.data_only'] = cluster['nodes']['count']['data_only']
            metrics['cluster.nodes.count.master_data'] = cluster['nodes']['count']['master_data']
            metrics['cluster.nodes.count.client'] = cluster['nodes']['count']['client']
            status = cluster['status']
            for other in ['red', 'yellow', 'green']:
                metrics['cluster.status.%s' % other] = 0
            metrics['cluster.status.%s' % status] = 1

            for key, value in metrics.items():
                self.publish(key, value)

    def collect_health(self):

        metrics = {}

        result = self._get('_cluster/health')
        if result:
            for key in ['number_of_nodes', 'number_of_data_nodes',
                        'active_primary_shards', 'active_shards',
                        'relocating_shards', 'initializing_shards',
                        'unassigned_shards']:
                metrics['cluster.health.%s' % key] = result[key]
            for key, value in metrics.items():
                self.publish(key, value)

    def collect_nodes(self):
        metrics = {}
        result = self._get('_nodes/stats?all=true')
        if not result:
            return

        for node, data in result['nodes'].items():
            name = data['name']

            #
            # http connections to ES
            metrics['nodes.%s.http.current' % name] = data['http']['current_open']

            #
            # indices
            for k, v in walk(data['indices']):
                if k not in ['percolate.memory_size']: # this key is human readable.
                    metrics['nodes.%s.indices.%s' % (name, k)] = v

            #
            # thread_pool
            if 'thread_pool' in self.config['stats']:
                for k, v in walk(data['thread_pool']):
                    metrics['nodes.%s.thread_pool.%s' % (name, k)] = v

            for k, v in walk(data['process']):
                if k not in ['timestamp']:
                    metrics['nodes.%s.process.%s' % (name, k)] = v
            #
            # jvm
            if 'jvm' in self.config['stats']:
                for k, v in walk(data['jvm']):
                    if k not in ['timestamp', 'uptime_in_millis']:
                        metrics['nodes.%s.jvm.%s' % (name, k)] = v

            for k, v in walk(data['transport']):
                metrics['nodes.%s.transport.%s' % (name, k)] = v

            for n, datas in enumerate(data['fs']['data']):
                for k, v in datas.items():
                    if k not in ['path', 'mount', 'dev']:
                        metrics['nodes.%s.fs.datas.%i.%s' % (name, n, k)] = v


            if 'indices' in self.config['stats']:
                #
                # individual index stats
                result = self._get('_stats?clear=true&docs=true&store=true&'
                                + 'indexing=true&get=true&search=true')
                if not result:
                    return

                _all = result['_all']
                self._index_metrics(metrics, 'indices._all', _all['primaries'])

                if 'indices' in _all:
                    indices = _all['indices']
                elif 'indices' in result:          # elasticsearch >= 0.90RC2
                    indices = result['indices']
                else:
                    return

                for name, index in indices.iteritems():
                    self._index_metrics(metrics, 'indices.%s' % name,
                                        index['primaries'])

        for key, value in metrics.items():
            self.publish(key, value)
