import six
import json

from search.query import Query
from search import conf


class SQS(object):
    """
    Search Queryset Class
    """
    def __init__(self, index_name, doc_type=None, query=None, backend=None, process_results=True):
        self.index_name = index_name
        self.doc_type = doc_type
        self.process_results = process_results

        if backend is not None:
            self.backend = backend
        else:
            from search import es
            self.backend = es

        if query is not None:
            self.query = query
        else:
            self.query = Query(index_name, doc_type, self.backend)

        self._result_cache = []
        self._result_count = None

    def __repr__(self):
        """
        Representation of SQS object. Called when object is accessed/printed.
        """
        data = list(self[:conf.REPR_OUTPUT_SIZE])

        if len(self) > conf.REPR_OUTPUT_SIZE:
            data[-1] = "...(remaining elements truncated)..."

        return repr(data)

    def __len__(self):
        """
        Returns actual count of results for a query. Called when len(sqs) is called.
        """
        if self._result_count is None:
            self._result_count = self.query.get_count()

        return self._result_count

    def __iter__(self):
        """
        Called whenever SQS object is iterated over. Tries to return results from cache
        initially and make a call to ES for rest.
        """
        if self._cache_is_full():
            # We've got a fully populated cache. Let Python do the hard work.
            return iter(self._result_cache)

        return self._manual_iter()

    def __getitem__(self, k):
        """
        Retrieves an item or slice from the set of results.
        """
        if not isinstance(k, (slice, six.integer_types)):
            raise Exception
        assert ((not isinstance(k, slice) and (k >= 0))
                or (isinstance(k, slice) and (k.start is None or k.start >= 0)
                    and (k.stop is None or k.stop >= 0))), \
                "Negative indexing is not supported."

        # Remember if it's a slice or not. We're going to treat everything as
        # a slice to simply the logic and will `.pop()` at the end as needed.
        if isinstance(k, slice):
            is_slice = True
            
            if k.start is not None:
                start = int(k.start)
            else:
                start = 0

            if k.stop is not None:
                bound = int(k.stop)
            else:
                bound = start + conf.SIZE_PER_QUERY
        else:
            is_slice = False
            start = k
            bound = k + 1

        # We need check to see if we need to populate more of the cache.
        if len(self._result_cache) <= 0 or (None in self._result_cache[start:bound] and not self._cache_is_full()):
            try:
                self._fill_cache(start, bound)
            except StopIteration:
                # There's nothing left, even though the bound is higher.
                pass

        # Cache should be full enough for our needs.
        if is_slice:
            return self._result_cache[start:bound]
        else:
            return self._result_cache[start]

    def _cache_is_full(self):
        """
        Checks if required results are available in cache or not
        """
        if not self.query.has_run():
            return False

        if len(self) <= 0:
            return True

        try:
            self._result_cache.index(None)
            return False
        except ValueError:
            # No ``None``s found in the results. Check the length of the cache.
            return len(self._result_cache) > 0

    def _manual_iter(self):
        # If we're here, our cache isn't fully populated.
        # For efficiency, fill the cache as we go if we run out of results.
        # Also, this can't be part of the __iter__ method due to Python's rules
        # about generator functions.
        current_position = 0
        current_cache_max = 0

        while True:
            if len(self._result_cache) > 0:
                try:
                    current_cache_max = self._result_cache.index(None)
                except ValueError:
                    current_cache_max = len(self._result_cache)

            while current_position < current_cache_max:
                yield self._result_cache[current_position]
                current_position += 1

            if self._cache_is_full():
                raise StopIteration

            # We've run out of results and haven't hit our limit.
            # Fill more of the cache.
            if not self._fill_cache(current_position, current_position + conf.SIZE_PER_QUERY):
                raise StopIteration

    def _fill_cache(self, start, end, **kwargs):
        # Tell the query where to start from and how many we'd like.
        self.query._reset()
        self.query.set_limits(start, end)
        results = self.query.get_results(**kwargs)

        if results == None or len(results) == 0:
            return False

        # Setup the full cache now that we know how many results there are.
        # We need the ``None``s as placeholders to know what parts of the
        # cache we have/haven't filled.
        # Using ``None`` like this takes up very little memory. In testing,
        # an array of 100,000 ``None``s consumed less than .5 Mb, which ought
        # to be an acceptable loss for consistent and more efficient caching.
        if len(self._result_cache) == 0:
            self._result_cache = [None for i in range(self.query.get_count())]

        if start is None:
            start = 0

        if end is None:
            end = self.query.get_count()

        to_cache = self.post_process_results(results)

        # Assign by slice.
        self._result_cache[start:start + len(to_cache)] = to_cache
        return True

    def post_process_results(self, results):
        """
        Converts list of dictionary of results into list of SearchResult objects
        """
        results_list = []
        for result in results:
            if result.get('fields'):
                if self.process_results:
                    results_list.append(SearchResult(self.index_name, result['_type'], result['_id'], result['_score'], result.get('fields')))
                else:
                    data = result.get('fields').copy()
                    data['doc_type'] = result['_type']
                    results_list.append(data)
            else:
                if self.process_results:
                    results_list.append(SearchResult(self.index_name, result['_type'], result['_id'], result['_score'], result.get('_source')))
                else:
                    data = result.get('_source').copy()
                    data['doc_type'] = result['_type']
                    results_list.append(data)

        return results_list

    def _clone(self):
        """
        Creates a new instance of SQS and assigns it existing query
        and returns this instance. This makes the queryset chainable
        """
        clone = SQS(self.index_name, self.doc_type, self.query, self.backend, self.process_results)
        return clone

    def create_index(self, body):
        """
        Create a New Index
        """
        return self.backend.indices.create(self.index_name, body)

    def delete_index(self):
        """
        Delete existing Index
        """
        return self.backend.indices.delete(self.index_name)

    def check_index(self):
        """
        Check if Index exists
        """
        return self.backend.indices.exists(self.index_name)

    def get_mapping(self):
        """
        Retrieve Mapping for a particular index/doc_type
        """
        return self.backend.indices.get_mapping([self.index_name], [self.doc_type])

    def get_settings(self):
        """
        Retrieve Settings for a particular index
        """
        return self.backend.indices.get_settings(self.index_name)

    def update_settings(self, body):
        """
        Update Index Setting
        """
        return self.backend.indices.put_settings(index=self.index_name, body=body)

    def refresh_index(self):
        """
        Refresh index
        """
        return self.backend.indices.refresh(self.index_name)

    def get_stats(self):
        """
        Get Index Stats
        """
        return self.backend.indices.stats(self.index_name)

    def get_status(self):
        """
        Get Index Stats
        """
        return self.backend.indices.status(self.index_name)

    def show_query(self):
        """
        Displays Query DSL created so far
        """
        return self.query.build_query()

    def reset(self):
        """
        Resets the current query and flushes all the results to make a fresh
        hit to ES. This might be needed if results have been accessed and then there
        is a change in the query
        """
        self.query._reset()

    def index(self, doc_id, doc_body):
        """
        Create or Update a document in index
        """
        return self.backend.index(self.index_name, self.doc_type, doc_body, doc_id)

    def remove(self, doc_id):
        """
        Remove the specified document
        """
        try:
            return self.backend.delete(self.index_name, self.doc_type, doc_id)
        except:
            return None

    def get(self, doc_id, fields=None):
        """
        Get specified document
        """
        try:
            if fields:
                result = self.backend.get(self.index_name, doc_id, self.doc_type, fields=fields)
                if self.process_results:
                    return SearchResult(self.index_name, self.doc_type, result['_id'], 0, result.get('fields'))
                else:
                    return result.get('fields')
            else:
                result = self.backend.get(self.index_name, doc_id, self.doc_type)
                if self.process_results:
                    return SearchResult(self.index_name, self.doc_type, result['_id'], 0, result.get('_source'))
                else:
                    return result.get('_source')
        except:
            return None

    def search(self, content, search_fields=None):
        """
        Add main search query using user entered text terms
        """
        clone = self._clone()
        clone.query.add_search_query(content, search_fields)
        return clone

    def filter(self, **kwargs):
        """
        Add filter query
        """
        clone = self._clone()
        if conf.DEFAULT_FILTER == "AND":
            clone.query.add_filter_and(kwargs)
        else:
            clone.query.add_filter_or(kwargs)
        return clone

    def filter_and(self, **kwargs):
        """
        Add AND Filter query
        """
        clone = self._clone()
        clone.query.add_filter_and(kwargs)
        return clone

    def filter_or(self, **kwargs):
        """
        Add OR Filter query
        """
        clone = self._clone()
        clone.query.add_filter_or(kwargs)
        return clone

    def only(self, *fields):
        """
        Restrict query to send only requested fields in response
        """
        clone = self._clone()
        clone.query.add_fields(fields)
        return clone

    def sort(self, *args):
        """
        Add sorting to final query. Takes comma separated list of fields.
        The order of fields is important as sorting is done basis the order
        in which you specify fields. Also to sort in descending order on a field
        just specify '-' sign before field name.
        """
        clone = self._clone()
        clone.query.add_sort(args)
        return clone

    def raw_query(self, query):
        """
        Add raw query to main query dsl. This method expects a query in the form
        of a dictionary which gets updated to 'query' part of final request body
        """
        clone = self._clone()
        clone.query.add_raw_query(query)
        return clone

    def raw_params(self, params):
        """
        Add raw params to final query. This method expects a query in the form
        of a dictionary which gets updated at the root of query dsl
        """
        clone = self._clone()
        clone.query.add_raw_params(params)
        return clone

    def function_score(self, query):
        """
        Add function score query to final query
        """
        clone = self._clone()
        clone.query.add_function_score(query)
        return clone

    def facet(self, field, **kwargs):
        """
        Add facet for passed field. Adds terms facet to query
        """
        clone = self._clone()
        clone.query.add_term_facet(field, **kwargs)
        return clone

    def facet_filter(self, field, **filter_query):
        """
        Adds facet filter to a facet query. If field is not defined as facet earlier
        then this will also add it as a facet query
        """
        clone = self._clone()
        clone.query.add_term_facet_filter(field, **filter_query)
        return clone

    def count(self):
        """
        Return count of results for the query. This will execute the query.
        """
        return len(self)

    def facet_counts(self):
        """
        Return facet counts for the query. This will run the query if not already
        and should only be used while displaying data
        """
        return self.query.get_facet_counts()

    def suggest(self, suggest_text, suggest_field, suggest_mode="missing", suggest_size=1):
        """
        Adds suggestion query to search query to have suggestions returned with search
        results. Default mode 'missing' only gives out suggestions incase where query is
        missing in index. Can be used to override main query and run correct query and send
        out results.
        """
        clone = self._clone()
        clone.query.add_suggestion(suggest_text, suggest_field, suggest_mode, suggest_size)
        return clone

    def get_suggestions(self):
        """
        Returns suggestions in the form of dictionary where suggested term is listed
        as a value against original query term being the key
        """
        return self.query.get_suggestions()

    def autocomplete(self, querystring, autocomplete_field, size=10):
        """
        Return autocomplete results using ES Completion Suggester. This method
        makes a hit to ES everytime its called
        """
        body = {"suggest":{"text":querystring, "completion":{"field":autocomplete_field, "fuzzy":True, "size":size}}}
        resp = self.backend.suggest(body=body)
        return resp

    def mlt(self, docid, fields=None, **kwargs):
        """
        Return similar documents matching the specified docid. If fields are specified
        then matching is done using these fields only
        """
        clone = self._clone()
        clone.query.mlt(docid, fields, **kwargs)
        return clone


class SearchResult(object):
    """
    A single Search Result. Results returned from ES in the form of
    dictionary is converted into an object of this class for easy access
    """
    def __init__(self, index_name, doc_type, pk, score, body):
        self.index_name = index_name
        self.doc_type = doc_type
        self.pk = pk
        self.score = score

        for key,val in body.items():
            if not key in self.__dict__:
                self.__dict__[key] = val

    def __repr__(self):
        return "<SearchResult: %s.%s (pk=%r)>" % (self.index_name, self.doc_type, self.pk)

    def __unicode__(self):
        return self.__repr__()

    def __getstate__(self):
        """
        Returns a dictionary representing the ``SearchResult`` in order to
        make it pickleable.
        """
        # The ``log`` is excluded because, under the hood, ``logging`` uses
        # ``threading.Lock``, which doesn't pickle well.
        ret_dict = self.__dict__.copy()
        return ret_dict


class SearchEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, SearchResult):
            return obj.__dict__.copy()

        return json.JSONEncoder.default(self, obj)
