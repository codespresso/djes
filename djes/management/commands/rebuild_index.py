from django.core.management.base import LabelCommand
from django.core.management import call_command
from django.conf import settings

from elasticsearch import Elasticsearch
from search.management.commands.update_index import Command as UpdateCommand
from search import conf
from search.models import SQS


class Command(LabelCommand):
    help = "Completely rebuilds the search index by removing the old data and then updating."
    option_list = list(LabelCommand.option_list) + \
                  [option for option in UpdateCommand.base_options if option.get_opt_string() != '--verbosity']

    def handle_label(self, label, **options):
        backend = Elasticsearch(settings.ELASTICSEARCH_NODES)
        if len(label.split('.')) > 1:
            index = conf.INDEXES[label.split('.')[0]]
        else:
            index = conf.INDEXES[label]
        sqs = SQS(index.index_name, backend=backend)

        if sqs.check_index():
            sqs.delete_index()
        sqs.create_index(index.settings)
        call_command('update_index', label, **options)
