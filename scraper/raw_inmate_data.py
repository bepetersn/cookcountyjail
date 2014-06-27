
from os import path
from pyquery import PyQuery
from collections import OrderedDict
from itertools import chain
import csv, shutil, requests

RAW_INMATE_DATA_RELEASE_URL = 'http://cookcountyjail.recoveredfactory.net/raw_inmate_data/'
RAW_INMATE_DATA_STARTING_YEAR = '2014'

RAW_INMATE_DATA_BUILD_DIR = 'CCJ_RAW_INMATE_DATA_BUILD_DIR'
RAW_INMATE_DATA_RELEASE_DIR = 'CCJ_RAW_INMATE_DATA_RELEASE_DIR'
STORE_RAW_INMATE_DATA = 'CCJ_STORE_RAW_INMATE_DATA'

FEATURE_CONTROL_IDS = [RAW_INMATE_DATA_BUILD_DIR, RAW_INMATE_DATA_RELEASE_DIR]
FEATURE_SWITCH_IDS = [STORE_RAW_INMATE_DATA]


class RawInmateData:

    HEADER_METHOD_NAMES = OrderedDict([
        ('Booking_Id', 'jail_id'),
        ('Booking_Date', 'booking_date'),
        ('Inmate_Hash', 'hash_id'),
        ('Gender', 'gender'),
        ('Race', 'race'),
        ('Height', 'height'),
        ('Weight', 'weight'),
        ('Age_At_Booking', 'age_at_booking'),
        ('Housing_Location', 'housing_location'),
        ('Charges', 'charges'),
        ('Bail_Amount', 'bail_amount'),
        ('Court_Date', 'next_court_date'),
        ('Court_Location', 'court_house_location')
    ])


    def __init__(self, snap_shot_date, feature_controls, monitor):
        if feature_controls is None:
            featu.path
            ass = type(self)
        self.__klass_name = self.__klass.__name__
        self.__monitor = monitor
        self.__snap_shot_date = snap_shot_date
        self.__raw_inmate_dir = None
        self.__build_dir = None
        self.__build_file_writer = None
        self.__build_file = None
        self.__build_file_name = None
        self.__feature_activated = False
        self.__configure_feature(feature_controls)


    @staticmethod
    def available_dates():
        """ Return a list of dates for which there is csv data available. 
            The dates are in text format, as follows: YYYY-MM-DD. """
        year_to_try = RAW_INMATE_DATA_STARTING_YEAR
        result = True
        dates = []
        while result:
            result = RawInmateData._available_dates_for_year(year_to_try)
            year_to_try = str(int(year_to_try) + 1)    
            if result:
                dates.extend(result)

        return dates

    @staticmethod
    def _available_dates_for_year(year):
        """ Given a year, query the raw inmate data API, and return
            a list of dates for which there is csv data available there. 
            The dates are in text format, as follows: YYYY-MM-DD. If 
            there is no data for the year, returns None. """
        try:
            result = requests.get(RAW_INMATE_DATA_RELEASE_URL + year)
        except requests.RequestException:
            return None

        if result.status_code != requests.codes.ok:
            return None

        doc = PyQuery(result.content)
        # get a list of links from the directory page
        # ignore the first link, which points to the dir above
        dates = doc('a:not(:first-child)')
        # drop the '.csv'
        return [d.text_content()[:-4] for d in dates]

    def add(self, inmate_details):
        if not self.__feature_activated:
            return
        if self.__build_file_writer is None:
            self.__open_build_file()
        inmate_info = [
            getattr(inmate_details, method_name)() for method_name in RawInmateData.HEADER_METHOD_NAMES.itervalues()
        ]
        self.__build_file_writer.writerow(inmate_info)

    def __configure_feature(self, feature_controls):
        if not (STORE_RAW_INMATE_DATA in feature_controls and feature_controls[STORE_RAW_INMATE_DATA]):
            return
        okay, self.__build_dir = self.__feature_control(feature_controls, RAW_INMATE_DATA_BUILD_DIR)
        if not okay:
            return
        okay, self.__raw_inmate_dir = self.__feature_control(feature_controls, RAW_INMATE_DATA_RELEASE_DIR)
        if not okay:
            return
        self.__feature_activated = True

    def __debug(self, msg, debug_level=None):
        self.__monitor.debug('{0}: {1}'.format(self.__klass_name, msg), debug_level)

    def __ensure_year_dir(self):
        year_dir = path.join(self.__raw_inmate_dir, self.__snap_shot_date.strftime('%Y'))
        try:
            os.makedirs(year_dir)
        except OSError:
            if not path.isdir(year_dir):
                raise
        return year_dir

    def __feature_control(self, feature_controls, feature_control):
        okay, dir_name = False, None
        if feature_control in feature_controls:
            dir_name = feature_controls[feature_control]
            okay = path.isdir(dir_name)
            if not okay:
                self.__debug("'%s' does not exist or is not a directory" % dir_name)
        return okay, dir_name

    @staticmethod
    def fetch_data_for_date(date):
        """ Return the raw inmate data for the supplied date, in YYYY-MM-DD format. 
            If the data can't be fetched, for whatever reason, returns None. """
        if date not in RawInmateData.available_dates():
            return None

        chosen_year = date[:4]
        query_url = RAW_INMATE_DATA_RELEASE_URL + chosen_year + '/' + date + '.csv'
        try:
            result = requests.get(query_url)
        except requests.RequestException:
            return None

        if result.status_code != requests.codes.ok:
            return None

        return result.content

    def __file_name(self):
        return self.__snap_shot_date.strftime('%Y-%m-%d.csv')

    def finish(self):
        if not self.__feature_activated:
            return
        self.__build_file.close()
        year_dir = self.__ensure_year_dir()
        shutil.move(self.__build_file_name, year_dir)

    def __open_build_file(self):
        self.__build_file_name = path.join(self.__build_dir, self.__file_name())
        self.__build_file = open(self.__build_file_name, "w")
        self.__build_file_writer = csv.writer(self.__build_file)
        header_names = [header_name for header_name in RawInmateData.HEADER_METHOD_NAMES.iterkeys()]
        self.__build_file_writer.writerow(header_names)




