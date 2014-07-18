from fq.curator.models import Item
from fq.collage.models import Spread


class ContentIndex(object):
    """
    Content Index
    """
    index_name = "content"
    doc_types = ["item", "spread"]
    active_field = "published"

    item_mapping = {
        # Item Mapping
        "title": {"type": "string", "store": "yes", "analyzer": "fq", "boost": "8.0"},
        "url": {"type": "string", "store": "yes", "index": "no"},
        "item_type": {"type": "integer", "store": "yes"},
        "category": {"type": "string", "store": "yes", "analyzer": "fq", "boost": "10.0"},
        "categoryid": {"type": "integer", "store": "yes"},
        "store": {"type": "string", "store": "yes", "analyzer": "fq", "boost": "2.0"},
        "storeid": {"type": "integer", "store": "yes"},
        "brand": {"type": "string", "store": "yes", "analyzer": "fq", "boost": "5.0"},
        "brandid": {"type": "integer", "store": "yes"},
        "price_currency": {"type": "string", "store": "yes", "index": "no"},
        "price": {"type": "float", "store": "yes"},
    }

    spread_mapping = {
        # Spread Mapping
        "title": {"type": "string", "store": "yes", "analyzer": "fq"},
        "user": {"type": "string", "store": "yes", "analyzer": "fq"},
        "url": {"type": "string", "store": "yes", "index": "no"},
        "tags": {"type": "string", "store": "yes", "analyzer": "fq"},
    }

    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "analysis": {
                "analyzer": {
                    "fq": {
                        "tokenizer": "standard",
                        "filter": ["standard", "lowercase", "stop", "porter_stem"]
                    }
                }
            }
        },
        "mappings": {
            "item": {
                "properties": item_mapping
            },
            "spread": {
                "properties": spread_mapping
            }
        }
    }

    def item_queryset(self, start_date, end_date):
        item = Item.objects.filter(published=True)
        if start_date and end_date:
            return item.filter(modified_date__range=(start_date, end_date))
        else:
            return item

    def spread_queryset(self, start_date, end_date):
        if start_date and end_date:
            return Spread.objects.filter(modified_date__range=(start_date, end_date))
        else:
            return Spread.objects.all()


