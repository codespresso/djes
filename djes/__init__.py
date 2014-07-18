from django.conf import settings

from elasticsearch import Elasticsearch

es = Elasticsearch(settings.ELASTICSEARCH_NODES)