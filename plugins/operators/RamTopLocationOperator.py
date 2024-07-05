from airflow.models import BaseOperator
from hooks.RickAndMortyHook import RickAndMortyHook
import logging


class RamTopLocationsOperator(BaseOperator):
    """
    Get Top n locations with the largest number of residents
    """
    template_fields = ('locations_count',)
    ui_color = '#847891'
    locations_data = list()

    def __init__(self, locations_count: int = 3, **kwargs):
        super().__init__(**kwargs)
        self.locations_count = locations_count

    def execute(self, context):
        """
        :return:  info of locations
        """
        logging.info(f'Start getting top {self.locations_count} locations')

        hook = RickAndMortyHook('RaM_conn')
        page_count = hook.get_page_count()

        for i in range(1, page_count + 1):
            logging.info(f'PAGE {i}')
            locations = hook.get_list_of_locations(i)
            for el in locations:
                self.locations_data.append((el['id'], el['name'], el['type'], el['dimension'], len(el['residents'])))

        logging.info(f'End getting top {self.locations_count} locations')
        self.locations_data.sort(key=lambda x: x[4], reverse=True)
        return self.locations_data[:self.locations_count]
