
import gevent
import grequests
import requests
from random import random

BAD_URL_NETWORK_PROBLEM = 'Bad url or network problem.'

COOK_COUNTY_JAIL_INMATE_DETAILS_URL = \
    'http://www2.cookcountysheriff.org/search2/details.asp?jailnumber='

_STD_INITIAL_SLEEP_PERIOD = 0.1
_STD_NUMBER_ATTEMPTS = 5
_STD_SLEEP_PERIODS = [1.61, 7, 13, 23, 41]


class Http:

    GET_REQUEST = grequests.get
    POST_REQUEST = grequests.post

    def __init__(self):
        pass

    def make_request(self, func, url, number_attempts=_STD_NUMBER_ATTEMPTS, 
                        initial_sleep_period=_STD_INITIAL_SLEEP_PERIOD, data=None):
        attempt = 1
        sleep_period = initial_sleep_period
        while attempt <= number_attempts:
            gevent.sleep(sleep_period)
            try:
                if func == self.GET_REQUEST:
                    request = func(url)
                elif func == self.POST_REQUEST and data:
                    request = func(url, data=data)
                grequests.map([request])
                if request.response is not None:
                    if request.response.status_code == requests.codes.ok:
                        return True, request.response.text
                else:
                    return False, BAD_URL_NETWORK_PROBLEM
            except requests.exceptions.RequestException:
                return False, BAD_URL_NETWORK_PROBLEM
            sleep_period = _get_next_sleep_period(sleep_period, attempt)
            attempt += 1
        return False, {'status-code': request.response.status_code}

    def get(self, url, number_attempts=_STD_NUMBER_ATTEMPTS, 
                initial_sleep_period=_STD_INITIAL_SLEEP_PERIOD):
        return self.make_request(self.GET_REQUEST, url, number_attempts, initial_sleep_period)

    def post(self, url, data, number_attempts=_STD_NUMBER_ATTEMPTS, 
                initial_sleep_period=_STD_INITIAL_SLEEP_PERIOD):
        return self.make_request(self.POST_REQUEST, url, number_attempts, initial_sleep_period, data=data)


def _get_next_sleep_period(current_sleep_period, attempt):
    """
    get_next_sleep_period - implements a cascading fall off sleep period with
    a bit of randomness control the periods by setting the values in the
    array, STD_SLEEP_PERIODS
    """
    index = attempt - 1
    if index >= len(_STD_SLEEP_PERIODS):
        index = -1
    return current_sleep_period * random() + _STD_SLEEP_PERIODS[index]
