from __future__ import (absolute_import, division, print_function)

import cgi
import configparser
import datetime
import logging
import os
import urllib

from dateutil.parser import parse

from lxml import etree
from lxml.etree import XMLParser
from lxml.etree import XMLSyntaxError

import pytz

from spyne.application import Application
from spyne.const.http import HTTP_405
from spyne.error import RequestNotAllowed
from spyne.model.fault import Fault
from spyne.model.primitive import AnyXml, Unicode
from spyne.protocol.http import HttpRpc
from spyne.protocol.soap import Soap11
from spyne.protocol.soap.mime import collapse_swa
from spyne.protocol.xml import XmlDocument
from spyne.server.http import HttpTransportContext
from spyne.server.wsgi import WsgiApplication

from wof.WofWsdls import WofWSDL_1_0, WofWSDL_1_1
from wof.apps.spyned_1_0 import TWOFService as wml10
from wof.apps.spyned_1_1 import TWOFService as wml11
from wof.apps.waterml2 import TWOFService as wml2


logging.getLogger('werkzeug').setLevel(logging.CRITICAL)
logging.basicConfig(level=logging.ERROR)
# logging.basicConfig(filename='/home/azureadmin/test/log.log', filemore='w', level=logging.DEBUG)
logging.getLogger('spyne.model.complex').setLevel(logging.ERROR)
logging.getLogger('spyne.interface._base').setLevel(logging.ERROR)
logging.getLogger('spyne.util.appreg').setLevel(logging.ERROR)
logging.getLogger('spyne.interface.xml_schema').setLevel(logging.ERROR)
logging.getLogger('spyne.protocol.dictdoc.simple').setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
logger_invalid = logging.getLogger(__name__ + ".invalid")
# print(logging.getLogger(__name__))
try:
    from wof import __version__
    version = __version__
except ImportError:
    version = 'dev'

_root_dir = os.path.abspath(os.path.dirname(__file__))
_TEMPLATES = os.path.join(_root_dir, 'flask', 'templates')

_SERVICE_PARAMS = {
    "r_type": "rest",
    "s_type": "soap",
    "wml10_tns": "http://www.cuahsi.org/his/1.0/ws/",
    "wml11_tns": "http://www.cuahsi.org/his/1.1/ws/",
    "wml10_rest_name": "WaterOneFlow_rest_1_0",
    "wml10_soap_name": "WaterOneFlow_soap_1_0",
    "wml11_rest_name": "WaterOneFlow_rest_1_1",
    "wml11_soap_name": "WaterOneFlow_soap_1_1",
}
# utc_time_zone = tz(None,0)
UTC_TZ = pytz.utc


def site_map(app):
    output = []
    for rule in app.url_map.iter_rules():
        # methods = ','.join(rule.methods)
        # line = urllib.unquote("{:50s} {:20s} {}".format(
        #     rule.endpoint, methods, rule)
        # )
        line = urllib.unquote("{}".format(rule))
        if '/static/' in line or '/rest/' in line:
            continue
        output.append(line)
    # print("Access Service Path at ")
    # for line in sorted(output):
    #     print(line)
    return sorted(output)


class WOFConfig(object):
        network = 'NETWORK'
        vocabulary = 'VOCABULARY'
        menu_group_name = 'MENU_GROUP_NAME'
        service_wsdl = 'SERVICE_WSDL'
        timezone = None
        timezone_abbr = None

        default_site = None
        default_variable = None
        default_start_date = None
        default_end_date = None
        default_unitid = None
        default_samplemedium = None

        default_west = None
        default_south = None
        default_north = None
        default_east = None


        organization = 'MYORGANIZATION'
        address = '1234 DriveIn'
        city = 'Seattle'
        state = 'Washington'
        zipcode = '98064'
        contactname = 'John Smith'
        contactemail = 'johnsmith@example.com'
        phone = '555-555-555'
        link = 'http://www.example.com/'

        def __init__(self, file_name, templates=None):
            config = configparser.RawConfigParser()
            config.read(file_name)
            #print(logging.ERROR)
            if config.has_option('WOF_1_1', 'Network'):
                self.network = config.get('WOF_1_1', 'Network')
            else:
                self.network = config.get('WOF', 'Network')
            if config.has_option('WOF_1_1', 'Vocabulary'):
                self.vocabulary = config.get('WOF_1_1', 'Vocabulary')
            else:
                self.vocabulary = config.get('WOF', 'Vocabulary')
            if config.has_option('WOF_1_1', 'URLPATH'):
                self.urlpath = config.get('WOF_1_1', 'URLPATH')
            else:
                self.urlpath = config.get('WOF', 'URLPATH')
            if config.has_option('WOF_1_1', 'Menu_Group_Name'):
                self.menu_group_name = config.get('WOF_1_1', 'Menu_Group_Name')
            else:
                self.menu_group_name = config.get('WOF', 'Menu_Group_Name')
            if config.has_option('WOF_1_1', 'Service_WSDL'):
                self.service_wsdl = config.get('WOF_1_1', 'Service_WSDL')
                print(self.service_wsdl)
            else:
                self.service_wsdl = config.get('WOF', 'Service_WSDL')
            if config.has_option('WOF_1_1', 'Timezone'):
                self.timezone = config.get('WOF_1_1', 'Timezone')

            else:
                self.timezone = config.get('WOF', 'Timezone')
            if config.has_option('WOF_1_1', 'TimezoneAbbreviation'):
                self.timezone_abbr = config.get('WOF_1_1',
                                                'TimezoneAbbreviation')
            else:
                self.timezone_abbr = config.get('WOF', 'TimezoneAbbreviation')

            if config.has_section('Default_Params'):
                if config.has_option('Default_Params', 'UnitID'):
                    self.default_unitid = config.get('Default_Params',
                                                     'UnitID')
                if config.has_option('Default_Params', 'SampleMedium'):
                    self.default_samplemedium = config.get('Default_Params',
                                                           'SampleMedium')

                if config.has_option('Default_Params_1_1', 'Site'):
                    self.default_site = config.get('Default_Params_1_1',
                                                   'Site')
                else:
                    self.default_site = config.get('Default_Params', 'Site')
                if config.has_option('Default_Params_1_1', 'Variable'):
                    self.default_variable = config.get('Default_Params_1_1',
                                                       'Variable')
                else:
                    self.default_variable = config.get('Default_Params',
                                                       'Variable')
                if config.has_option('Default_Params_1_1', 'StartDate'):
                    self.default_start_date = config.get('Default_Params_1_1',
                                                         'StartDate')
                else:
                    self.default_start_date = config.get('Default_Params',
                                                         'StartDate')
                if config.has_option('Default_Params_1_1', 'EndDate'):
                    self.default_end_date = config.get('Default_Params_1_1',
                                                       'EndDate')
                else:
                    self.default_end_date = config.get('Default_Params',
                                                       'EndDate')
                if config.has_option('Default_Params_1_1', 'East'):
                    self.default_east = config.get('Default_Params_1_1',
                                                   'East')
                else:
                    if config.has_option('Default_Params', 'East'):
                        self.default_east = config.get('Default_Params',
                                                       'East')
                    else:
                        self.default_east = 180
                if config.has_option('Default_Params_1_1', 'North'):
                    self.default_north = config.get('Default_Params_1_1',
                                                    'North')
                else:
                    if config.has_option('Default_Params', 'North'):
                        self.default_north = config.get('Default_Params',
                                                        'North')
                    else:
                        self.default_north = 90
                if config.has_option('Default_Params_1_1', 'South'):
                    self.default_south = config.get('Default_Params_1_1',
                                                    'South')
                else:
                    if config.has_option('Default_Params', 'South'):
                        self.default_south = config.get('Default_Params',
                                                        'South')
                    else:
                        self.default_south = -90
                if config.has_option('Default_Params_1_1', 'West'):
                    self.default_west = config.get('Default_Params_1_1',
                                                   'West')
                else:
                    if config.has_option('Default_Params', 'West'):
                        self.default_west = config.get('Default_Params',
                                                       'West')
                    else:
                        self.default_west = -180

            if config.has_section('Contact'):
                if config.has_option('Contact', 'Organization'):
                    self.organization = config.get('Contact', 'Organization')
                if config.has_option('Contact', 'Address'):
                    self.address = config.get('Contact', 'Address')
                if config.has_option('Contact', 'City'):
                    self.city = config.get('Contact', 'City')
                if config.has_option('Contact', 'State'):
                    self.state = config.get('Contact', 'State')
                if config.has_option('Contact', 'ZipCode'):
                    self.zipcode = config.get('Contact', 'ZipCode')
                if config.has_option('Contact', 'Name'):
                    self.contactname = config.get('Contact', 'Name')
                if config.has_option('Contact', 'Email'):
                    self.contactemail = config.get('Contact', 'Email')
                if config.has_option('Contact', 'Phone'):
                    self.phone = config.get('Contact', 'Phone')
                if config.has_option('Contact', 'Link'):
                    self.link = config.get('Contact', 'Link')

            if templates is not None:
                self.TEMPLATES = templates
            elif config.has_option('WOFPY', 'Templates'):
                self.TEMPLATES = config.get('WOFPY', 'Templates')
            else:
                self.TEMPLATES = _TEMPLATES


class wofSoap11(Soap11):
    def _wof_parse_xml_string(self, xml_string, parser, charset=None):
        string = b''.join(xml_string)
        if charset:
            # string = string.decode(charset)
            string = string.decode('utf-8-sig')  # Remove BOM.
            string = string.encode(charset)  # Back to original encoding type.
        try:
            try:
                root, xmlids = etree.XMLID(string, parser)
            except ValueError as e:
                logging.debug(
                    'ValueError: de-serializing from unicode strings with '
                    'encoding declaration is not supported by lxml.'
                )
                root, xmlids = etree.XMLID(string.encode(charset), parser)

        except XMLSyntaxError as e:
            logger_invalid.error("%r in string %r", e, string)
            raise Fault('Client.XMLSyntaxError', str(e))

        return root, xmlids

    def create_in_document(self, ctx, charset=None):
        if isinstance(ctx.transport, HttpTransportContext):
            # according to the soap via http standard, soap requests must only
            # work with proper POST requests.
            content_type = ctx.transport.get_request_content_type()
            http_verb = ctx.transport.get_request_method()
            if content_type is None or http_verb != "POST":
                ctx.transport.resp_code = HTTP_405
                raise RequestNotAllowed(
                        "You must issue a POST request with the Content-Type "
                        "header properly set.")

            content_type = cgi.parse_header(content_type)
            collapse_swa(content_type, ctx.in_string)

        ctx.in_document = self._wof_parse_xml_string(
            ctx.in_string,
            XMLParser(**self.parser_kwargs),
            charset
        )


""" returns an array of the applications """


def getSpyneApplications(wof_obj_1_0, wof_obj_1_1, templates=None):

    # wof_obj_1_0 = wof_1_0.WOF(dao, config_file)
    # wof_obj_1_1 = wof_1_1.WOF_1_1(dao,config_file)
    print(wof_obj_1_0.urlpath)
    sensorNetwork = wof_obj_1_0.urlpath.replace('/', '').lower()

    soap_app_1_0 = Application(
        [wml10(wof_obj_1_0, Unicode, _SERVICE_PARAMS["s_type"])],
        tns=_SERVICE_PARAMS["wml10_tns"],
        name=sensorNetwork+'_svc_' + _SERVICE_PARAMS["wml10_soap_name"],
        in_protocol=wofSoap11(validator='lxml'),
        out_protocol=Soap11(),
    )

    rest_app_1_0 = Application(
        [wml10(wof_obj_1_0, AnyXml, _SERVICE_PARAMS["r_type"])],
        tns=_SERVICE_PARAMS["wml10_tns"],
        name=sensorNetwork+'_svc_' + _SERVICE_PARAMS["wml10_rest_name"],
        in_protocol=HttpRpc(validator='soft'),
        out_protocol=XmlDocument(),
    )

    soap_app_1_1 = Application(
        [wml11(wof_obj_1_1, Unicode, _SERVICE_PARAMS["s_type"])],
        tns=_SERVICE_PARAMS["wml11_tns"],
        name=sensorNetwork+'_svc_' + _SERVICE_PARAMS["wml11_soap_name"],
        in_protocol=wofSoap11(validator='lxml'),
        out_protocol=Soap11(),
    )

    rest_app_1_1 = Application(
        [wml11(wof_obj_1_1, AnyXml, _SERVICE_PARAMS["r_type"])],
        tns=_SERVICE_PARAMS["wml11_tns"],
        name=sensorNetwork + '_svc_' + _SERVICE_PARAMS["wml11_rest_name"],
        in_protocol=HttpRpc(validator='soft'),
        out_protocol=XmlDocument(),
    )
    # need to update template to 1_1 object.
    # <gml:Definition gml:id="methodCode-{{ method_result.MethodID  }}">
    #   File "\lib\site-packages\jinja2\environment.py", line 408, in getattr
    #     return getattr(obj, attribute)
    # UndefinedError: 'method_result' is undefined
    rest_app_2 = Application(
        [wml2(wof_obj_1_0, Unicode, _SERVICE_PARAMS["r_type"])],
        tns=_SERVICE_PARAMS["wml11_tns"],
        name=sensorNetwork + '_svc_' + _SERVICE_PARAMS["wml11_rest_name"],
        in_protocol=HttpRpc(validator='soft'),
        # out_protocol=XmlDocument(),
        out_protocol=HttpRpc(mime_type='text/xml'),

    )

    rest_wsgi_wrapper_1_0 = WsgiApplication(rest_app_1_0)
    soap_wsgi_wrapper_1_0 = WsgiApplication(soap_app_1_0)
    rest_wsgi_wrapper_1_1 = WsgiApplication(rest_app_1_1)
    soap_wsgi_wrapper_1_1 = WsgiApplication(soap_app_1_1)
    rest_wsgi_wrapper_2_0 = WsgiApplication(rest_app_2)

    spyneApps = {
        '/' + sensorNetwork+'/rest/1_0': rest_wsgi_wrapper_1_0,
        '/' + sensorNetwork+'/rest/1_1': rest_wsgi_wrapper_1_1,
        '/' + sensorNetwork+'/soap/cuahsi_1_0': soap_wsgi_wrapper_1_0,
        '/' + sensorNetwork+'/soap/cuahsi_1_1': soap_wsgi_wrapper_1_1,
        '/' + sensorNetwork+'/rest/2': rest_wsgi_wrapper_2_0,
    }

    templatesPath = None
    if templates is None:
        if wof_obj_1_1._config is not None:
            templatesPath = os.path.abspath(wof_obj_1_1._config.TEMPLATES)
    else:
        templatesPath = os.path.abspath(templates)

    if templatesPath:
        print(templatesPath)
        if not os.path.exists(templatesPath):
            logging.info('Templates path: {} NOT exists {}'.format(
                templatesPath,
                os.path.exists(templatesPath))
            )
            templatesPath = _TEMPLATES
            logging.info('default temnplate path: %s' % templatesPath)
        # needs to be service_baseURL. in config wof_obj_1_0.service_wsdl
        wsdl10 = WofWSDL_1_0(
            soap_wsgi_wrapper_1_0.doc.wsdl11.interface,
            templates=templatesPath,
            network=sensorNetwork,
            version=version
        )

        # soap_wsgi_wrapper_1_0._wsdl = wsdl10.build_interface_document('/'+ sensorNetwork+'/soap/wateroneflow',templatesPath) #.get_wsdl_1_0('/'+ sensorNetwork+'/soap/wateroneflow')  # noqa
        soap_wsgi_wrapper_1_0.event_manager.add_listener(
            'wsdl',
            wsdl10.on_get_wsdl_1_0_
        )
        # path: /{sensorNetwork}/soap/wateroneflow_1_1/.wsdl returns the WSDL.
        wsdl11 = WofWSDL_1_1(
            soap_wsgi_wrapper_1_1.doc.wsdl11.interface,
            templates=templatesPath,
            network=sensorNetwork,
            version=version
        )
        # soap_wsgi_wrapper_1_1._wsdl = wsdl11.build_interface_document('/'+ sensorNetwork+'/soap/wateroneflow_1_1',templatesPath) #.get_wsdl_1_0('/'+ sensorNetwork+'/soap/wateroneflow')  # noqa
        soap_wsgi_wrapper_1_1.event_manager.add_listener(
            'wsdl',
            wsdl11.on_get_wsdl_1_1_
        )

    return spyneApps


class wofConfig(object):
    dao = None
    config = None
    configObject = None

    def __init__(self, dao=None, wofConfigFile=None):
        self.config = wofConfigFile
        self.dao = dao
        if (self.config is not None):
            try:
                print(self.config)
                self.configObject = WOFConfig(self.config)
            except:
                pass


def _get_datavalues_datetime(obj, local_datetime_attr,
                             utc_datetime_attr):
    """
    Returns a datetime  given an object and the names of the
    attributes for local time and utc date time
    """
    local_datetime = getattr(obj, local_datetime_attr, None)
    if local_datetime:
        if type(local_datetime) == datetime.datetime:
            if not local_datetime.tzinfo:
                raise ValueError("local times must be timezone-aware")
            # return local_datetime.isoformat()
            return local_datetime
        else:
            ldt = parse(local_datetime)
            return ldt
    else:
        utc_datetime = getattr(obj, utc_datetime_attr)
        if type(utc_datetime) == datetime.datetime:
            utcdt = UTC_TZ.localize(utc_datetime)
            return utcdt
        else:
            utcdt = parse(utc_datetime)
            return utcdt
