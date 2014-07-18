
class Query(object):
    """
    Search Query maker. This class is responsible for converting
    params into appropriate dictionary to be later dumped as json
    and passed to ES
    """
    def __init__(self, index, doc_type, backend):
        self.index = index
        self.doc_type = doc_type
        self.backend = backend

        self.search_terms = None
        self.filter_and_terms = None
        self.filter_or_terms = None
        self.params = {}
        self.function_score = None
        self.facets = None
        self.sort = None
        self.raw_query = None
        self.raw_params = None
        self.offset = 0
        self.size = 20

        self.mlt_query = False
        self.mlt_doc = None
        self.mlt_fields = []
        self.mlt_options = {}

        self._results = None
        self._hit_count = None
        self._facet_counts = None
        self._suggestions = None

    def add_filter_and(self, kwargs):
        """
        Add AND Filter Query
        """
        if self.filter_and_terms is None:
            self.filter_and_terms = {"must":[]}

        for key, val in kwargs.items():
            if key == "ids":
                filter_query = {"ids":{"values":val}}
            else:
                key, operator = key.split("__") if len(key.split("__")) > 1 else [key, None]

                if operator in ["gt", "gte", "lt", "lte"]:
                    filter_query = {"range":{key:{operator:val}}}
                else:
                    filter_type = "terms" if operator == "in" or type(val) == list else "term"
                    filter_query = {filter_type:{key:val}}

            self.filter_and_terms['must'].append(filter_query)

    def add_filter_or(self, kwargs):
        """
        Add OR Filter Query
        """
        if self.filter_or_terms is None:
            self.filter_or_terms = {"should":[]}

        for key, val in kwargs.items():
            if key == "ids":
                filter_query = {"ids":{"values":val}}
            else:
                key, operator = key.split("__") if len(key.split("__")) > 1 else [key, None]

                if operator in ["gt", "gte", "lt", "lte"]:
                    filter_query = {"range":{key:{operator:val}}}
                else:
                    filter_type = "terms" if operator == "in" or type(val) == list else "term"
                    filter_query = {filter_type:{key:val}}

            self.filter_or_terms['should'].append(filter_query)

    def add_search_query(self, content, search_fields):
        """
        Add main search query using multi_match query
        """
        if search_fields is not None:
            search_query = {"multi_match": {"query": content, "fields": search_fields, "lenient":True}}
        else:
            search_query = {"match": {"_all": content}}

        self.search_terms = search_query

    def add_fields(self, fields):
        """
        Add list of fields to be sent back in response
        """
        self.params['fields'] = fields

    def add_suggestion(self, suggest_text, suggest_field, suggest_mode, suggest_size):
        """
        Add suggestion query in search query to have suggestions returned as part of part
        search results
        """
        self.params['suggest_text'] = suggest_text
        self.params['suggest_field'] = suggest_field
        self.params['suggest_mode'] = suggest_mode
        self.params['suggest_size'] = suggest_size

    def add_term_facet(self, field, **kwargs):
        """
        Add terms facets to the query. If additional parameters are passed
        then they are attached to the terms query
        """
        facets = {}
        facets.update({field:{"terms":{"field":field}}})

        for key, val in kwargs.items():
            facets[field]['terms'][key] = val

        if self.facets is None:
            self.facets = facets
        else:
            self.facets.update(facets)

    def add_term_facet_filter(self, field, **filter_query):
        """
        Add facet filter to term facets. If field is not found
        in the list of facets then it is added
        """
        if self.facets is None or not self.facets.get(field):
            self.add_term_facet(field)

        facet = self.facets[field]

        for key, val in filter_query.items():
            field, operator = key.split("__") if len(key.split("__")) > 1 else [key, None]
            filter_type = "terms" if operator == "in" or type(val) == list else "term"
            facet.setdefault("facet_filter", {"and":[]})['and'].append({filter_type:{field:val}})

    def add_function_score(self, query):
        """
        Add function score query to final query mainly to boost
        """
        self.function_score = query

    def add_sort(self, args):
        """
        Add sort parameters to main query. args should be a list of mentioned
        fields, to be iterated and form a final list of dictionary.
        """
        sort = []
        for field in args:
            if field.startswith("-"):
                sort.append({field[1:]:"desc"})
            else:
                sort.append(field)

        self.sort = sort

    def add_raw_query(self, query):
        """
        Set raw query
        """
        self.raw_query = query

    def add_raw_params(self, params):
        """
        Set raw params
        """
        self.raw_params = params

    def mlt(self, docid, fields, **kwargs):
        self.mlt_query = True
        self.mlt_doc = docid
        self.mlt_fields = fields
        self.mlt_options = kwargs

    def build_query(self):
        """
        Combines all parameters relevant to Query DSL and builds the
        final query to be sent to ES
        """
        if self.raw_query is not None:
            return self.raw_query

        if self.function_score is not None:
            query = {"function_score":{"query":{"match_all":{}}}}
            query['query']['function_score'].update(self.function_score)
            query = query['query']['function_score']['query']['filtered']
        else:
            query = {"query":{"match_all":{}}}

        filter_query = {"filter":{"bool":{}}}
        facet_query = {"facets":{}}

        if self.search_terms is not None:
            query['query'] = self.search_terms

        if self.filter_and_terms is not None:
            filter_query['filter']['bool'].update(self.filter_and_terms)

        if self.filter_or_terms is not None:
            filter_query['filter']['bool'].update(self.filter_or_terms)

        if self.filter_and_terms or self.filter_or_terms:
            query.update(filter_query)

        if self.facets is not None:
            facet_query['facets'].update(self.facets)
            query.update(facet_query)

        if self.sort is not None:
            query['sort'] = self.sort

        if self.raw_params is not None:
            query.update(self.raw_params)

        return query

    def _reset(self):
        """
        Reset query, to make a fresh hit to ES
        """
        self._results = None
        self._hit_count = None
        self._facet_counts = None
        self._suggestions = None

    def set_limits(self, start=None, bound=None):
        """
        Restricts the query by altering either the start, end or both offsets
        """
        if start is not None:
            self.offset = int(start)

        if bound is not None:
            self.size = int(bound) - int(start)

    def get_results(self):
        """
        Return results of query. If the query has not run yet, then this will
        make a hit to ES
        """
        if self._results is None:
            if self.mlt_query:
                self.run_mlt()
            else:
                self.run()

        return self._results

    def get_count(self):
        """
        Return actual query results count. If query has not
        run yet then this will run the query
        """
        if self._hit_count is None:
            if self.mlt_query:
                self.run_mlt()
            else:
                self.run()

        return self._hit_count

    def get_facet_counts(self):
        """
        Returns facet counts. It executes the query if the query has not
        run yet
        """
        if self._facet_counts is None:
            self.run()

        return self._facet_counts

    def get_suggestions(self):
        """
        Returns suggestions. It executes the query if the query has not
        run yet or if suggestions were not found for a given query
        """
        if self._suggestions is None:
            self.run()

        return self._suggestions

    def run(self):
        """
        This method makes the actual hit to ES Search Api after computing all params
        """
        final_query = self.build_query()
        self.params['from_'] = self.offset
        self.params['size'] = self.size

        results = self.backend.search(index=self.index, doc_type=self.doc_type, body=final_query, **self.params)

        self._results = results['hits']['hits']
        self._hit_count = results['hits']['total']
        self._facet_counts = self.process_facets(results.get('facets', {}))
        self._suggestions = self.process_suggestions(results.get('suggest', None))

    def run_mlt(self):
        """
        This method makes the actual hit to ES More Like This Api after computing all params
        """
        self.mlt_options['search_from'] = self.offset
        self.mlt_options['search_size'] = self.size

        results = self.backend.mlt(index=self.index, doc_type=self.doc_type, id=self.mlt_doc, mlt_fields=self.mlt_fields, body=self.build_query(),**self.mlt_options)

        self._results = results['hits']['hits']
        self._hit_count = results['hits']['total']
        self._facet_counts = self.process_facets(results.get('facets', {}))
        self._suggestions = self.process_suggestions(results.get('suggest', None))

    def has_run(self):
        """
        Indicates if any query has been been run
        """
        return None not in (self._results, self._hit_count)

    def process_facets(self, facet_result):
        facet_counts = {}

        for facet_field, facet_details in facet_result.items():
            tmplist = []
            for term in facet_details.get('terms'):
                tmplist.append((term['term'], term['count']))

            facet_counts[facet_field] = tmplist

        return facet_counts

    def process_suggestions(self, suggestion_result):
        if suggestion_result is None:
            return None

        raw_suggestions = suggestion_result[self.params['suggest_field']]
        suggestions = {}

        for term in raw_suggestions:
            if term['options']:
                suggestions[term['text']] = term['options'][0]['text']
            else:
                suggestions[term['text']] = None

        return suggestions
