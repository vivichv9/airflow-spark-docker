from airflow.providers.http.hooks.http import HttpHook


class RickAndMortyHook(HttpHook):
    """
    Interact with Rick and Morty API
    """
    def __init__(self, http_conn_id: str, **kwargs):
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_list_of_locations(self, page_number: int):
        """
        :param page_number: number of the page
        :return List of locations
        """
        return self.run(f'api/location?page={page_number}').json()['results']

    def get_page_count(self):
        """
        return count of pages in API
        """
        return self.run(f'api/location').json()['info']['pages']


