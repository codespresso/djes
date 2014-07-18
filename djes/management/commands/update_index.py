from __future__ import print_function
from __future__ import unicode_literals
from datetime import timedelta
from optparse import make_option
import logging
import os

from django import db
from django.conf import settings
from django.core.management.base import LabelCommand, CommandError
from django.db import reset_queries
from django.db.models import get_model

try:
    from django.utils.encoding import force_text
except ImportError:
    from django.utils.encoding import force_unicode as force_text

try:
    from django.utils.encoding import smart_bytes
except ImportError:
    from django.utils.encoding import smart_str as smart_bytes

try:
    from django.utils.timezone import now
except ImportError:
    from datetime import datetime
    now = datetime.now

from elasticsearch import Elasticsearch
from search.models import SQS
from search.conf import INDEXES

DEFAULT_BATCH_SIZE = 1000
DEFAULT_AGE = None


def worker(bits):
    # We need to reset the connections, otherwise the different processes
    # will try to share the connection, which causes things to blow up.
    from django.db import connections

    for alias, info in connections.databases.items():
        # We need to also tread lightly with SQLite, because blindly wiping
        # out connections (via ``... = {}``) destroys in-memory DBs.
        if not 'sqlite3' in info['ENGINE']:
            try:
                db.close_connection()
                if isinstance(connections._connections, dict):
                    del(connections._connections[alias])
                else:
                    delattr(connections._connections, alias)
            except KeyError:
                pass

    index, doctype, start, end, total, start_date, end_date, remove, verbosity = bits
    backend = Elasticsearch(settings.ELASTICSEARCH_NODES)

    qs = getattr(index, "%s_queryset" % doctype)(start_date=start_date, end_date=end_date)
    do_update(backend, index, doctype, qs, start, end, total, remove, verbosity=verbosity)


def do_update(backend, index, doctype, qs, start, end, total, remove, verbosity=1):
    # Get a clone of the QuerySet so that the cache doesn't bloat up
    # in memory. Useful when reindexing large amounts of data.
    small_cache_qs = qs.all()
    current_qs = small_cache_qs[start:end]
    sqs = SQS(index.index_name, doctype, backend=backend)

    if verbosity >= 2:
        if hasattr(os, 'getppid') and os.getpid() == os.getppid():
            print("  indexed %s - %d of %d." % (start + 1, end, total))
        else:
            print("  indexed %s - %d of %d (by %s)." % (start + 1, end, total, os.getpid()))

    for item in current_qs:
        if getattr(item, index.active_field):
            sqs.index(item.pk, item.get_search_dict())
        elif remove:
            sqs.remove(item.pk)

    # Clear out the DB connections queries because it bloats up RAM.
    reset_queries()


class Command(LabelCommand):
    help = "Freshens the index for the given app"
    base_options = (
        make_option('-a', '--age', action='store', dest='age',
            default=DEFAULT_AGE, type='int',
            help='Number of minutes back to consider objects new.'
        ),
        make_option('-s', '--start', action='store', dest='start_date',
            default=None, type='string',
            help='The start date for indexing within. Can be any dateutil-parsable string, recommended to be YYYY-MM-DDTHH:MM:SS.'
        ),
        make_option('-e', '--end', action='store', dest='end_date',
            default=None, type='string',
            help='The end date for indexing within. Can be any dateutil-parsable string, recommended to be YYYY-MM-DDTHH:MM:SS.'
        ),
        make_option('-b', '--batch-size', action='store', dest='batchsize',
            default=DEFAULT_BATCH_SIZE, type='int',
            help='Number of items to index at once.'
        ),
        make_option('-r', '--remove', action='store_true', dest='remove',
            default=False, help='Remove objects from the index that are no longer present in the database.'
        ),
        make_option('-k', '--workers', action='store', dest='workers',
            default=0, type='int',
            help='Allows for the use multiple workers to parallelize indexing. Requires multiprocessing.'
        ),
    )
    option_list = LabelCommand.option_list + base_options

    def handle(self, *items, **options):
        self.verbosity = int(options.get('verbosity', 1))
        self.batchsize = options.get('batchsize', DEFAULT_BATCH_SIZE)
        self.start_date = None
        self.end_date = None
        self.remove = options.get('remove', False)
        self.workers = int(options.get('workers', 0))
        self.backend = Elasticsearch(settings.ELASTICSEARCH_NODES)

        age = options.get('age', DEFAULT_AGE)
        start_date = options.get('start_date')
        end_date = options.get('end_date')

        if age is not None:
            self.start_date = now() - timedelta(minutes=int(age))
            self.end_date = now()

        if start_date is not None:
            from dateutil.parser import parse as dateutil_parse

            try:
                self.start_date = dateutil_parse(start_date)
            except ValueError:
                pass

        if end_date is not None:
            from dateutil.parser import parse as dateutil_parse

            try:
                self.end_date = dateutil_parse(end_date)
            except ValueError:
                pass

        if not items:
            items = []
            for index in INDEXES.keys():
                items.append(index)

        return super(Command, self).handle(*items, **options)

    def handle_label(self, label, **options):
        try:
            self.update_backend(label)
        except:
            logging.exception("Error updating %s", label)
            raise

    def update_backend(self, label):
        if self.workers > 0:
            import multiprocessing

            # workers resetting connections leads to references to models / connections getting
            # stale and having their connection disconnected from under them. Resetting before
            # the loop continues and it accesses the ORM makes it better.
            db.close_connection()

        if len(label.split('.')) > 1:
            index_name = label.split('.')[0]
            doc_type = label.split('.')[1]

            index = INDEXES[index_name]()
            doc_types = [doc_type]
        else:
            index = INDEXES[label]()
            doc_types = index.doc_types

        for doctype in doc_types:
            qs = getattr(index, "%s_queryset" % doctype)(start_date=self.start_date, end_date=self.end_date)
            total = qs.count()

            if self.verbosity >= 1:
                print(u"Indexing %d %s-%s" % (total, label, doctype))

            batch_size = self.batchsize

            if self.workers > 0:
                ghetto_queue = []

            for start in range(0, total, batch_size):
                end = min(start + batch_size, total)

                if self.workers == 0:
                    do_update(self.backend, index, doctype, qs, start, end, total, self.remove, self.verbosity)
                else:
                    ghetto_queue.append((index, doctype, start, end, total, self.start_date, self.end_date, self.remove, self.verbosity))

            if self.workers > 0:
                pool = multiprocessing.Pool(self.workers)
                pool.map(worker, ghetto_queue)
                pool.terminate()
