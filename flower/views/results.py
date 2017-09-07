from __future__ import absolute_import

import logging

import json

try:
    from itertools import imap
except ImportError:
    imap = map

from tornado import web

from rediscluster import StrictRedisCluster

from ..views import BaseHandler
from ..config import CELERY_REDIS_CLUSTER_SETTINGS

logger = logging.getLogger(__name__)


class ResultView(BaseHandler):
    @web.authenticated
    def get(self, task_id):
        r = StrictRedisCluster(startup_nodes=CELERY_REDIS_CLUSTER_SETTINGS['startup_nodes'], decode_responses=True)
        info = r.get('failed.task.info.' + task_id)
        result = r.get('celery-task-meta-' + task_id)

        if result is None:
            raise web.HTTPError(404, "Unknown task '%s'" % task_id)

        self.render("result.html", result=json.loads(result), info=json.loads(info))

class ResultsView(BaseHandler):
    @web.authenticated
    def get(self):
        r = StrictRedisCluster(startup_nodes=CELERY_REDIS_CLUSTER_SETTINGS['startup_nodes'], decode_responses=True)
        self.render(
            "results.html",
            results=[ item.split('failed.task.info.')[1] for item in r.keys('failed.task.info.*') if item and 'failed.task.info.' in item ],
        )
