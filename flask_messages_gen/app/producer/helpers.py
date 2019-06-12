import random
from datetime import datetime, timedelta


def gen_datetime(min_year=1900, max_year=datetime.now().year, format=None):
    # generate a datetime in format yyyy-mm-dd hh:mm:ss.000000
    start = datetime(min_year, 1, 1, 00, 00, 00)
    years = max_year - min_year + 1
    end = start + timedelta(days=365 * years)
    random_date = start + (end - start) * random.random()
    if format:
        return random_date.strftime(format)
    return random_date


def create_booking_event_generator(event_count: int):
    for i in range(event_count):
        srch_ci = gen_datetime(min_year=2017)
        srch_co = srch_ci + timedelta(days=random.randrange(0, 60))
        yield {
            'date_time': gen_datetime(min_year=2017, format='%Y-%m-%d'),
            'site_name': random.randrange(1, 100),
            'posa_continent': random.randrange(1, 8),
            'user_location_country': random.randrange(1, 50),
            'user_location_region': random.randrange(1, 20),
            'user_location_city': random.randrange(1, 1000),
            'orig_destination_distance': random.randrange(100, 5000),
            'user_id': random.randrange(1, 10000),
            'is_mobile': random.choice((0, 1)),
            'is_package': random.choice((0, 1)),
            'channel': random.randrange(1, 10),
            'srch_ci': srch_ci.strftime('%Y-%m-%d'),
            'srch_co': srch_co.strftime('%Y-%m-%d'),
            'srch_adults_cnt': random.randrange(1, 10),
            'srch_children_cnt': random.randrange(1, 7),
            'srch_rm_cnt': random.randrange(1, 4),
            'srch_destination_id': random.randrange(1, 5000),
            'srch_destination_type_id': random.randrange(1, 10000),
            'is_booking': random.choice((0, 1)),
            'cnt': random.randrange(1, 100),
            'hotel_continent': random.randrange(1, 8),
            'hotel_country': random.randrange(1, 50),
            'hotel_market': random.randrange(1, 100),
            'hotel_cluster': random.randrange(1, 150)
        }


def generate_booking_event():
    srch_ci = gen_datetime(min_year=2017)
    srch_co = srch_ci + timedelta(days=random.randrange(0, 60))
    return {
        'date_time': gen_datetime(min_year=2017, format='%Y-%m-%d'),
        'site_name': random.randrange(1, 100),
        'posa_continent': random.randrange(1, 8),
        'user_location_country': random.randrange(1, 50),
        'user_location_region': random.randrange(1, 20),
        'user_location_city': random.randrange(1, 1000),
        'orig_destination_distance': random.randrange(100, 5000),
        'user_id': random.randrange(1, 10000),
        'is_mobile': random.choice((0, 1)),
        'is_package': random.choice((0, 1)),
        'channel': random.randrange(1, 10),
        'srch_ci': srch_ci.strftime('%Y-%m-%d'),
        'srch_co': srch_co.strftime('%Y-%m-%d'),
        'srch_adults_cnt': random.randrange(1, 10),
        'srch_children_cnt': random.randrange(1, 7),
        'srch_rm_cnt': random.randrange(1, 4),
        'srch_destination_id': random.randrange(1, 5000),
        'srch_destination_type_id': random.randrange(1, 10000),
        'is_booking': random.choice((0, 1)),
        'cnt': random.randrange(1, 100),
        'hotel_continent': random.randrange(1, 8),
        'hotel_country': random.randrange(1, 50),
        'hotel_market': random.randrange(1, 100),
        'hotel_cluster': random.randrange(1, 150)
    }

