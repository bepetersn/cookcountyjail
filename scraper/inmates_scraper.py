from monitor import MONITOR_VERBOSE_DMSG_LEVEL
from concurrent_base import ConcurrentBase

WORKERS_TO_START = 25

CCJ_INMATE_DETAILS_URL = 'http://www2.cookcountysheriff.org/search2/details.asp?jailnumber='
CCJ_V2_API_PROCESS_URL = 'http://localhost:5000/process'

class InmatesScraper(ConcurrentBase):

    def __init__(self, http, inmates, inmate_details_class, monitor, workers_to_start=WORKERS_TO_START):
        super(InmatesScraper, self).__init__(monitor, workers_to_start)
        self._http = http
        self._inmates = inmates
        self._inmate_details_class = inmate_details_class

    def create_if_exists(self, arg):
        self._put(self._create_if_exists, arg)

    def _create_if_exists(self, inmate_id):
        self._debug('check for inmate - %s' % inmate_id, MONITOR_VERBOSE_DMSG_LEVEL)
        worked, inmate_details_in_html = self._http.get(CCJ_INMATE_DETAILS_URL + inmate_id)
        if worked:
            inmate_details = self._inmate_details_class(inmate_details_in_html)
            self._inmates.add(inmate_id, inmate_details)

            # send the reuslts to v2 API as well
            inmate_json_dict = inmate_details.to_json()
            worked, result = self._http.post(CCJ_V2_API_PROCESS_URL, data={'data': inmate_json_dict})
            if worked:
                self._debug('post succeeded, jail_id: {0}'.format(inmate_id))
            else:
                self._debug('post failed, jail_id: {0}, result: {1}'.format(inmate_id, result))

    def resurrect_if_found(self, inmate_id):
        self._put(self._resurrect_if_found, inmate_id)

    def _resurrect_if_found(self, inmate_id):
        self._debug('check if really discharged inmate %s' % inmate_id, MONITOR_VERBOSE_DMSG_LEVEL)
        worked, inmate_details_in_html = self._http.get(CCJ_INMATE_DETAILS_URL + inmate_id)
        if worked:
            self._debug('resurrected discharged inmate %s' % inmate_id, MONITOR_VERBOSE_DMSG_LEVEL)
            inmate_details = self._inmate_details_class(inmate_details_in_html)
            self._inmates.update(inmate_id, inmate_details)

            # send the reuslts to v2 API as well
            inmate_json_dict = inmate_details.to_json()
            worked, result = self._http.post(CCJ_V2_API_PROCESS_URL, data={'data': inmate_json_dict})
            if worked:
                self._debug('post succeeded, jail_id: {0}'.format(inmate_id))
            else:
                self._debug('post failed, jail_id: {0}, result: {1}'.format(inmate_id, result))

    def update_inmate_status(self, inmate_id):
        self._put(self._update_inmate_status, inmate_id)

    def _update_inmate_status(self, inmate_id):
        worked, inmate_details_in_html = self._http.get(CCJ_INMATE_DETAILS_URL + inmate_id)
        if worked:
            inmate_details = self._inmate_details_class(inmate_details_in_html)
            self._inmates.update(inmate_id, inmate_details)

            # send the reuslts to v2 API as well
            inmate_json_dict = inmate_details.to_json()
            worked, result = self._http.post(CCJ_V2_API_PROCESS_URL, data={'data': inmate_json_dict})
            if worked:
                self._debug('post succeeded, jail_id: {0}'.format(inmate_id))
            else:
                self._debug('post failed, jail_id: {0}, result: {1}'.format(inmate_id, result))
        else:
            self._inmates.discharge(inmate_id)

