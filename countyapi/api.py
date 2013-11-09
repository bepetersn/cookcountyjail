from copy import copy
import csv
import os

from django.http import HttpResponse
from django.core.exceptions import ObjectDoesNotExist
from django.conf import settings

from tastypie.exceptions import ApiFieldError, Unauthorized
from tastypie.bundle import Bundle
from tastypie.fields import ToManyField, ToOneField
from tastypie.resources import ModelResource, ALL, ALL_WITH_RELATIONS
from tastypie.serializers import Serializer
from tastypie.authorization import Authorization

from countyapi.models import CountyInmate, CourtLocation, CourtDate, HousingLocation, HousingHistory, DailyPopulationCounts, DailyBookingsCounts, ChargesHistory
from countyapi.management.commands.utils import convert_to_int


NEGATIVE_VALUES = set(['0', 'false'])


def use_caching():
    """
    For now this alwasy returns true until we can get better caching mechanism to work
    See earlier version to get original implementation of function
    """
    return True


def cache_ttl():
    default_ttl = 60 * 12  # Time to Live in Cache: 12 minutes
    cache_ttl = os.environ.get('CACHE_TTL')
    return convert_to_int(cache_ttl, default_ttl) if cache_ttl else default_ttl


if use_caching():
    from tastypie.cache import SimpleCache


DISCLAIMER = """
Cook County Jail Inmate data, scraped from
http://www2.cookcountysheriff.org/search2/ nightly.

Learn more about this API at
https://github.com/sc3/cookcountyjail/wiki/API-guide

Learn more about this project at
https://github.com/sc3/cookcountyjail/

The data on jail inmates is not verified. Do not take the data as
definitive, but rather as suggestive. This data can be used to develop
interesting questions, but it cannot be cited as factual.

Developed by the Supreme Chi-Town Coding Crew
(https://github.com/sc3/sc3)
"""

COURT_DATE_URL = '/api/1.0/courtdate/'
COURT_LOCATION_URL = '/api/1.0/courtlocation/'
COUNTY_INMATE_URL = '/api/1.0/countyinmate/'
HOUSING_HISTORY_URL = '/api/1.0/housinghistory/'
HISTORY_LOCATION_URL = '/api/1.0/historylocation/'
CHARGES_HISTORY_URL = '/api/1.0/chargeshistory/'


class JailToOneField(ToOneField):
    def dehydrate(self, bundle, for_list=False):
        foreign_obj = None

        if isinstance(self.attribute, basestring):
            attrs = self.attribute.split('__')
            foreign_obj = bundle.obj

            for attr in attrs:
                previous_obj = foreign_obj
                try:
                    foreign_obj = getattr(foreign_obj, attr, None)
                except ObjectDoesNotExist:
                    foreign_obj = None
        elif callable(self.attribute):
            foreign_obj = self.attribute(bundle)

        if not foreign_obj:
            if not self.null:
                raise ApiFieldError("The model '%r' has an empty attribute '%s' and doesn't allow a null value."
                                    % (previous_obj, attr))

            return None

        if bundle.request.GET.get('related') == '1':
            self.fk_resource = self.get_related_resource(foreign_obj)
            fk_bundle = Bundle(obj=foreign_obj, request=bundle.request)
            return self.dehydrate_related(fk_bundle, self.fk_resource)
        else:
            return super(JailToOneField, self).dehydrate(bundle)


class JailToManyField(ToManyField):
    def dehydrate(self, bundle, for_list=False):
        if not bundle.obj or not bundle.obj.pk:
            if not self.null:
                raise ApiFieldError("The model '%r' does not have a primary key and can not be used in a ToMany context."
                                    % bundle.obj)

            return []

        the_m2ms = None
        previous_obj = bundle.obj
        attr = self.attribute

        if isinstance(self.attribute, basestring):
            attrs = self.attribute.split('__')
            the_m2ms = bundle.obj

            for attr in attrs:
                previous_obj = the_m2ms
                try:
                    the_m2ms = getattr(the_m2ms, attr, None)
                except ObjectDoesNotExist:
                    the_m2ms = None

                if not the_m2ms:
                    break

        elif callable(self.attribute):
            the_m2ms = self.attribute(bundle)

        if not the_m2ms:
            if not self.null:
                raise ApiFieldError("The model '%r' has an empty attribute '%s' and doesn't allow a null value."
                                    % (previous_obj, attr))

            return []

        self.m2m_resources = []
        m2m_dehydrated = []

        # TODO: Also model-specific and leaky. Relies on there being a
        #       ``Manager`` there.
        if bundle.request.GET.get('related') == '1':
            for m2m in the_m2ms.all():
                m2m_resource = self.get_related_resource(m2m)
                m2m_bundle = Bundle(obj=m2m, request=bundle.request)
                self.m2m_resources.append(m2m_resource)
                m2m_dehydrated.append(self.dehydrate_related(m2m_bundle, m2m_resource))


class JailSerializer(Serializer):
    """
    Serialize to json, jsonp, xml, and csv.
    """

    formats = ['json', 'jsonp', 'xml', 'csv']
    content_types = {
        'json': 'application/json',
        'jsonp': 'text/javascript',
        'xml': 'application/xml',
        'yaml': 'text/yaml',
        'html': 'text/html',
        'plist': 'application/x-plist',
        'csv': 'text/csv',
    }

    def to_csv(self, data, options=None):
        """
        Write to a simple CSV format.
        """
        options = options or {}
        data = self.to_simple(data, options)
        response = HttpResponse(mimetype='text/csv')
        response['Content-Disposition'] = 'attachment; filename="cookcountyjail.csv"'

        writer = csv.writer(response)
        writer.writerow(data['objects'][0].keys())

        for item in data['objects']:
            writer.writerow(item.values())

        return response


class JailAuthorization(Authorization):
    def ip_check(self, object_list, bundle):
        if bundle.request.META['REMOTE_ADDR'] in settings.ALLOWED_POST_IPS:
            return True
        raise Unauthorized("You are not allowed to access that resource.")

    def read_list(self, object_list, bundle):
        return object_list

    def read_detail(self, object_list, bundle):
        return True

    def create_list(self, object_list, bundle):
        if self.ip_check(object_list, bundle):
            return object_list
        return []

    def create_detail(self, object_list, bundle):
        return self.ip_check(object_list, bundle)

    def update_list(self, object_list, bundle):
        if self.ip_check(object_list, bundle):
            return object_list
        return []

    def update_detail(self, object_list, bundle):
        return self.ip_check(object_list, bundle)

    def delete_list(self, object_list, bundle):
        if self.ip_check(object_list, bundle):
            return object_list
        return []

    def delete_detail(self, object_list, bundle):
        return self.ip_check(object_list, bundle)


class JailResource(ModelResource):
    """
    ModelResource overrides for our project. Add caching and disclaimer.
    """
    def __init__(self, api_name=None):
        """
        Patched init that doesn't use deepcopy,
        see https://github.com/toastdriven/django-tastypie/issues/720
        """
        self.fields = {k: copy(v) for k, v in self.base_fields.iteritems()}

        if not api_name is None:
            self._meta.api_name = api_name

    def alter_detail_data_to_serialize(self, request, data):
        """
        Add message to data.
        """
        data.data['about_this_data'] = DISCLAIMER
        return data

    def alter_list_data_to_serialize(self, request, data):
        """
        Add message to meta.
        """
        data['meta']['about_this_data'] = DISCLAIMER
        return data


class CourtLocationResource(JailResource):
    """
    API endpoint for CourtLocation model, which represents court room.
    """

    class Meta:
        queryset = CourtLocation.objects.all()
        limit = 100
        max_limit = 0
        if use_caching():
            cache = SimpleCache(timeout=cache_ttl())
        serializer = JailSerializer()
        filtering = {
            'location': ALL,
        }

    def dehydrate(self, bundle, for_list=False):
        """
        Show court dates in location lists and detail views.
        """
        if bundle.request.path.startswith(COURT_LOCATION_URL) and \
                (bundle.request.path != COURT_LOCATION_URL or
                 bundle.request.REQUEST.get('related') == '1'):
            dates = bundle.obj.court_dates.all()
            resource = CourtDateResource()
            bundle.data['court_dates'] = []
            for court_date in dates:
                date_bundle = resource.build_bundle(obj=court_date,
                                                    request=bundle.request)
                bundle.data["court_dates"].append(resource.full_dehydrate(date_bundle,
                                                                          for_list=for_list).data)
        return bundle


class CourtDateResource(JailResource):
    """
    API endpoint for CourtDate model, the unique combination of courtroom,
    inmate, and date used to represent the court history.
    """
    location = JailToOneField(CourtLocationResource, "location", null=True,
                              full=False)
    inmate = JailToOneField('countyapi.api.CountyInmateResource', "inmate",
                            null=True, full=False)

    class Meta:
        queryset = CourtDate.objects.select_related('location').select_related('inmate').all()
        allowed_methods = ['get']
        limit = 100
        max_limit = 0
        if use_caching():
            cache = SimpleCache(timeout=cache_ttl())
        serializer = JailSerializer()
        filtering = {
            'date': ALL,
            'location': ALL_WITH_RELATIONS,
            'inmate': ALL_WITH_RELATIONS,
        }
        ordering = filtering.keys()

    def dehydrate(self, bundle, for_list=False):
        """
        Set up bidirectional relationships based on request.
        """

        # Include inmate ID when called from location
        if bundle.request.path.startswith(COURT_LOCATION_URL):
            bundle.data["inmate"] = bundle.obj.inmate.pk

        # Include location when called from inmate
        if bundle.request.path.startswith(COUNTY_INMATE_URL):
            location = bundle.obj.location
            resource = CourtLocationResource()
            location_bundle = resource.build_bundle(obj=location,
                                                    request=bundle.request)
            bundle.data["location"] = resource.full_dehydrate(location_bundle,
                                                              for_list=for_list).data

        # Include primary keys on court dates
        if bundle.request.path.startswith(COURT_DATE_URL) and not \
                bundle.request.REQUEST.get('related') == '1':
            bundle.data["location_id"] = bundle.obj.location.pk
            bundle.data["location"] = bundle.obj.location.location
            bundle.data["inmate_jail_id"] = bundle.obj.inmate.pk

        # Include full inmate in related query
        if bundle.request.path.startswith(COURT_DATE_URL) and \
                bundle.request.REQUEST.get('related') == '1':
            inmate = bundle.obj.inmate
            resource = CountyInmateResource()
            inmate_bundle = resource.build_bundle(obj=inmate,
                                                  request=bundle.request)
            bundle.data["inmate"] = resource.full_dehydrate(inmate_bundle,
                                                            for_list=for_list).data

            location = bundle.obj.location
            resource = CourtLocationResource()
            location_bundle = resource.build_bundle(obj=location,
                                                    request=bundle.request)
            bundle.data["location"] = resource.full_dehydrate(location_bundle,
                                                              for_list=for_list).data

        return bundle


class HousingLocationResource(JailResource):
    """
    API endpoint for HousingLocation model, a place or status in the jail.
    """

    class Meta:
        queryset = HousingLocation.objects.all()
        allowed_methods = ['get']
        limit = 100
        max_limit = 0
        if use_caching():
            cache = SimpleCache(timeout=cache_ttl())
        serializer = JailSerializer()
        filtering = {
            'housing_location': ALL,
            'division': ALL,
            'sub_division': ALL,
            'sub_division_location': ALL,
            'in_jail': ALL,
            'in_program': ALL
        }
        ordering = filtering.keys()


class HousingHistoryResource(JailResource):
    """
    API endpoint for HousingHistory model, the unique combination of housing
    location, inmate, and date used to represent the housing history.
    """
    housing_location = JailToOneField(HousingLocationResource,
                                      "housing_location", null=True, full=False)
    inmate = JailToOneField('countyapi.api.CountyInmateResource',
                            "inmate", null=True, full=False)

    class Meta:
        queryset = HousingHistory.objects.select_related('location').select_related('inmate').all()
        allowed_methods = ['get']
        serializer = JailSerializer()
        limit = 100
        max_limit = 0
        if use_caching():
            cache = SimpleCache(timeout=cache_ttl())
        filtering = {
            'inmate': ALL_WITH_RELATIONS,
            'housing_date': ALL,
            'housing_location': ALL_WITH_RELATIONS,
        }
        ordering = filtering.keys()

    def dehydrate(self, bundle, for_list=False):
        """
        Set up bidirectional relationships based on request.
        """

        # Include inmate ID when called from location
        if bundle.request.path.startswith(HISTORY_LOCATION_URL):
            bundle.data["inmate"] = bundle.obj.inmate.pk

        # Include location when called from inmate
        if bundle.request.path.startswith(COUNTY_INMATE_URL):
            location = bundle.obj.housing_location
            resource = HousingLocationResource()
            location_bundle = resource.build_bundle(obj=location,
                                                    request=bundle.request)
            bundle.data["housing_location"] = resource.full_dehydrate(location_bundle,
                                                                      for_list=for_list).data

        # Include primary keys on court dates
        if bundle.request.path.startswith(HOUSING_HISTORY_URL) and not \
                bundle.request.REQUEST.get('related') == '1':
            bundle.data["location_id"] = bundle.obj.housing_location.pk
            bundle.data["inmate_jail_id"] = bundle.obj.inmate.pk

        # Include full inmate in related query
        if bundle.request.path.startswith(HOUSING_HISTORY_URL) and \
                bundle.request.REQUEST.get('related') == '1':
            inmate = bundle.obj.inmate
            resource = CountyInmateResource()
            inmate_bundle = resource.build_bundle(obj=inmate,
                                                  request=bundle.request)
            bundle.data["inmate"] = resource.full_dehydrate(inmate_bundle,
                                                            for_list=for_list).data

            location = bundle.obj.housing_location
            resource = HousingLocationResource()
            location_bundle = resource.build_bundle(obj=location,
                                                    request=bundle.request)
            bundle.data["housing_location"] = resource.full_dehydrate(location_bundle,
                                                                      for_list=for_list).data

        return bundle


class ChargesHistoryResource(JailResource):
    """
    API endpoint for ChargesHistory model.
    """
    inmate = JailToOneField('countyapi.api.CountyInmateResource',
                            "inmate", null=True, full=False)

    class Meta:
        queryset = ChargesHistory.objects.select_related('inmate').all()
        allowed_methods = ['get']
        serializer = JailSerializer()
        limit = 100
        max_limit = 0
        if use_caching():
            cache = SimpleCache(timeout=cache_ttl())
        filtering = {
            'inmate': ALL_WITH_RELATIONS,
            'charges': ALL,
            'charges_citation': ALL,
            'date_seen': ALL,
            'housing_location': ALL_WITH_RELATIONS,
        }
        ordering = filtering.keys()

    def dehydrate(self, bundle, for_list=False):
        """
        Set up bidirectional relationships based on request.
        """

        # Include primary keys on court dates
        if bundle.request.path.startswith(CHARGES_HISTORY_URL) and not \
                bundle.request.REQUEST.get('related') == '1':
            bundle.data["inmate_jail_id"] = bundle.obj.inmate.pk

        # Include full inmate in related query
        if bundle.request.path.startswith(HOUSING_HISTORY_URL) and \
                bundle.request.REQUEST.get('related') == '1':
            inmate = bundle.obj.inmate
            resource = CountyInmateResource()
            inmate_bundle = resource.build_bundle(obj=inmate,
                                                  request=bundle.request)
            bundle.data["inmate"] = resource.full_dehydrate(inmate_bundle,
                                                            for_list=for_list).data

        return bundle


class CountyInmateResource(JailResource):
    """
    API endpoint for CountyInmate model, which represents a person in jail.
    """
    court_dates = JailToManyField(CourtDateResource, 'court_dates')
    housing_history = JailToManyField(HousingHistoryResource, 'housing_history')
    charges_history = JailToManyField(ChargesHistoryResource, 'charges_history')

    class Meta:
        queryset = CountyInmate.objects.select_related('housing_history')\
                                       .select_related('charges_history')\
                                       .select_related('court_dates')\
                                       .all()
        allowed_methods = ['get']
        limit = 100
        max_limit = 0
        if use_caching():
            cache = SimpleCache(timeout=cache_ttl())
        serializer = JailSerializer()
        list_allowed_methods = ['get', 'post', 'put', 'delete']
        detail_allowed_methods = ['get', 'post', 'put', 'delete']
        authorization = JailAuthorization()
        excludes = ['last_seen_date', 'url']
        filtering = {
            'jail_id': ALL,
            'booking_date': ALL,
            'discharge_date_earliest': ALL,
            'gender': ALL,
            'age_at_booking': ALL,
            'bail_amount': ALL,
            'housing_location': ALL,
            'charges_citation': ALL,
            'race': ALL,
            'court_dates': ALL_WITH_RELATIONS,
            'housing_history': ALL_WITH_RELATIONS,
            'charges_history': ALL_WITH_RELATIONS,
            'person_id': ALL
        }
        ordering = filtering.keys()

    def dehydrate(self, bundle, for_list=False):
        """
        Show court dates and housing history in inmate lists and detail views.
        """
        if bundle.request.path.startswith(COUNTY_INMATE_URL) and \
                (bundle.request.path != COUNTY_INMATE_URL or
                 bundle.request.REQUEST.get('related') == '1'):
            dates = bundle.obj.court_dates.all()
            resource = CourtDateResource()
            bundle.data['court_dates'] = []
            for court_date in dates:
                date_bundle = resource.build_bundle(obj=court_date,
                                                    request=bundle.request)
                bundle.data["court_dates"].append(resource.full_dehydrate(date_bundle,
                                                                          for_list=for_list).data)

            housings = bundle.obj.housing_history.all()
            resource = HousingHistoryResource()
            bundle.data['housing_history'] = []
            for housing in housings:
                date_bundle = resource.build_bundle(obj=housing,
                                                    request=bundle.request)
                bundle.data["housing_history"].append(resource.full_dehydrate(date_bundle,
                                                                              for_list=for_list).data)

            charges = bundle.obj.charges_history.all()
            resource = ChargesHistoryResource()
            bundle.data['charges_history'] = []
            for charge in charges:
                date_bundle = resource.build_bundle(obj=charge,
                                                    request=bundle.request)
                bundle.data["charges_history"].append(resource.full_dehydrate(date_bundle,
                                                                              for_list=for_list).data)

        return bundle


class DailyPopulationCountsResource(JailResource):
    """
    API endpoint for DailyPopulationCounts
    """

    class Meta:
        queryset = DailyPopulationCounts.objects.all()
        max_limit = 0
        if use_caching():
            cache = SimpleCache(timeout=cache_ttl())
        serializer = JailSerializer()
        filtering = {
            'booking_date': ALL
        }
        ordering = filtering.keys()


class DailyBookingsCountsResource(JailResource):
    """
    API endpoint for DailyBookingsCounts
    """

    class Meta:
        queryset = DailyBookingsCounts.objects.all()
        max_limit = 0
        if use_caching():
            cache = SimpleCache(timeout=cache_ttl())
        serializer = JailSerializer()
        filtering = {
            'booking_date': ALL
        }
        ordering = filtering.keys()
