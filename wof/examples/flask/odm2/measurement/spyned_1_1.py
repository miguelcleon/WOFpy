from __future__ import (absolute_import, division, print_function)

import io
import logging

from lxml import etree

from spyne.decorator import rpc
from spyne.model.complex import Array
from spyne.model.fault import Fault
from spyne.model.primitive import AnyXml, Boolean, Float, Unicode
from spyne.service import ServiceBase
from spyne.util import memoize

import wof

__author__ = 'cyoun'

logger = logging.getLogger(__name__)

NSDEF = 'xmlns="http://www.cuahsi.org/waterML/1.1/"'


@memoize
def TWOFService(wof_inst, T, T_name):
    class WOFService(ServiceBase):

        @rpc(Array(Unicode), Unicode, _returns=AnyXml)
        def GetSitesObject(ctx, site=None, authToken=None):
            try:
                if site is not None:
                    siteArg = ','.join(str(s) for s in site)
                else:
                    siteArg = None
                logging.debug(site)
                logging.debug(siteArg)
                siteResponse = wof_inst.create_get_site_response(siteArg)
                outStream = io.StringIO()
                siteResponse.export(outStream, 0, name_="sitesResponse",
                                    namespacedef_=NSDEF)
                return outStream.getvalue()
            except Exception as inst:
                if type(inst) == Fault:
                    raise inst
                else:
                    raise Fault(faultstring=str(inst))

        # This is the one that returns WITH <![CDATA[...]]>
        @rpc(Array(Unicode), Unicode, _returns=T)
        def GetSites(ctx, site=None, authToken=None):
            siteResult = WOFService.GetSitesObject(ctx, site, authToken)
            if T_name == wof._SERVICE_PARAMS["r_type"]:
                return siteResult
            return siteResult.replace('\n', '')

        @rpc(Float, Float, Float, Float, Boolean, Unicode, _returns=AnyXml)
        def GetSitesByBoxObject(ctx, west, south, east, north,
                                IncludeSeries, authToken=None):
            try:
                siteResponse = wof_inst.create_get_site_box_response(
                    west, south, east, north, IncludeSeries
                )
                outStream = io.StringIO()
                siteResponse.export(outStream, 0, name_="sitesResponse",
                                    namespacedef_=NSDEF)
                return outStream.getvalue()
            except Exception as inst:
                if type(inst) == Fault:
                    raise inst
                else:
                    raise Fault(faultstring=str(inst))

        @rpc(Float, Float, Float, Float, Boolean, Unicode, _returns=T)
        def GetSitesByBox(ctx, west, south, east, north,
                          IncludeSeries, authToken=None):
            sitesResult = WOFService.GetSitesByBoxObject(
                ctx, west, south, east, north, IncludeSeries, authToken
            )
            if T_name == wof._SERVICE_PARAMS["r_type"]:
                return sitesResult
            return sitesResult.replace('\n', '')

        @rpc(Unicode, Unicode, _returns=AnyXml)
        def GetSiteInfoObject(ctx, site, authToken=None):
            print('site!!!!')
            print(site)
            # print(ctx)
            # print(authToken)
            try:
                siteInfoResponse = wof_inst.create_get_site_info_response(site)
                print('site in try no 44')
                print(site)
                outStream = io.StringIO()
                siteInfoResponse.export(
                    outStream, 0, name_="sitesResponse", namespacedef_=NSDEF
                )
                return outStream.getvalue()
            except Exception as inst:
                if type(inst) == Fault:
                    raise inst
                else:
                    print('here44')
                    print(inst)
                    print(siteInfoResponse)
                    # print(ctx)
                    # raise Fault(faultstring=str(inst))
            # print(wof_inst)
            # print(str(inst))
            # print(siteInfoResponse)
            print(site)

        @rpc(Unicode, Unicode, _returns=T)
        def GetSiteInfo(ctx, site, authToken=None):
            # print('ctx')
            # print(ctx)
            print('site')
            print(site)
            # print('authToken')
            # print(authToken)
            siteinfoResult = WOFService.GetSiteInfoObject(ctx, site, authToken)
            if T_name == wof._SERVICE_PARAMS["r_type"]:
                return siteinfoResult
            return siteinfoResult.replace('\n', '')

        @rpc(Array(Unicode), Unicode, _returns=AnyXml)
        def GetSiteInfoMultpleObject(ctx, site, authToken=None):
            if site is not None:
                siteArg = ','.join(str(s) for s in site)
            else:
                siteArg = None
            try:
                siteInfoResponse = \
                    wof_inst.create_get_site_info_multiple_response(siteArg)
                outStream = io.StringIO()
                siteInfoResponse.export(
                    outStream, 0, name_="sitesResponse", namespacedef_=NSDEF
                )
                return outStream.getvalue()
            except Exception as inst:
                if type(inst) == Fault:
                    raise inst
                else:
                    raise Fault(faultstring=str(inst))

        @rpc(Array(Unicode), Unicode, _returns=T)
        def GetSiteInfoMultple(ctx, site, authToken=None):
            sitesinfoResult = WOFService.GetSiteInfoMultpleObject(ctx, site, authToken)  # noqa
            if T_name == wof._SERVICE_PARAMS["r_type"]:
                return sitesinfoResult
            return sitesinfoResult.replace('\n', '')

        @rpc(Unicode, Unicode, _returns=AnyXml)
        def GetVariableInfoObject(ctx, variable, authToken=None):
            try:
                variableInfoResponse = \
                    wof_inst.create_get_variable_info_response(variable)
                outStream = io.StringIO()
                variableInfoResponse.export(outStream, 0,
                                            name_="variablesResponse",
                                            namespacedef_=NSDEF)
                return outStream.getvalue()
            except Exception as inst:
                if type(inst) == Fault:
                    raise inst
                else:
                    raise Fault(faultstring=str(inst))

        @rpc(Unicode, Unicode, _returns=T)
        def GetVariableInfo(ctx, variable, authToken=None):
            varinfoResult = WOFService.GetVariableInfoObject(ctx, variable, authToken)  # noqa
            if T_name == wof._SERVICE_PARAMS["r_type"]:
                return varinfoResult
            return varinfoResult.replace('\n', '')

        @rpc(Unicode, _returns=AnyXml)
        def GetVariablesObject(ctx, authToken=None):
            try:
                variableInfoResponse = wof_inst.create_get_variable_info_response()  # noqa
                outStream = io.StringIO()
                variableInfoResponse.export(outStream, 0,
                                            name_="variablesResponse",
                                            namespacedef_=NSDEF)
                return outStream.getvalue()
            except Exception as inst:
                if type(inst) == Fault:
                    raise inst
                else:
                    raise Fault(faultstring=str(inst))

        @rpc(Unicode, _returns=T)
        def GetVariables(ctx, authToken=None):
            varsResult = WOFService.GetVariablesObject(ctx, authToken)
            if T_name == wof._SERVICE_PARAMS["r_type"]:
                return varsResult
            return varsResult.replace('\n', '')

        @rpc(Unicode, Unicode, Unicode, Unicode, Unicode, _returns=AnyXml)
        def GetValuesObject(ctx, location, variable, startDate=None, endDate=None, authToken=None):  # noqa
            try:
                timeSeriesResponse = wof_inst.create_get_values_response(
                        location, variable, startDate, endDate)
                outStream = io.StringIO()
                timeSeriesResponse.export(
                    outStream, 0, name_="timeSeriesResponse",
                    namespacedef_=NSDEF)
                return outStream.getvalue()
            except Exception as inst:
                if type(inst) == Fault:
                    raise inst
                else:
                    raise Fault(faultstring=str(inst))

        @rpc(Unicode, Unicode, Unicode, Unicode, Unicode, _returns=T)
        def GetValues(ctx, location, variable, startDate=None, endDate=None, authToken=None):  # noqa
            valuesResult = WOFService.GetValuesObject(
                ctx,
                location,
                variable,
                startDate,
                endDate,
                authToken
            )
            if T_name == wof._SERVICE_PARAMS["r_type"]:
                return valuesResult
            return valuesResult.replace('\n', '')

        @rpc(Unicode, Unicode, Unicode, Unicode, _returns=AnyXml)
        def GetValuesForASiteObject(ctx, site, startDate=None, endDate=None, authToken=None):  # noqa
            try:
                timeSeriesResponse = wof_inst.create_get_values_site_response(
                        site, startDate, endDate)
                outStream = io.StringIO()
                timeSeriesResponse.export(
                    outStream, 0, name_="timeSeriesResponse",
                    namespacedef_=NSDEF)
                return outStream.getvalue()
            except Exception as inst:
                if type(inst) == Fault:
                    raise inst
                else:
                    raise Fault(faultstring=str(inst))

        @rpc(Unicode, Unicode, Unicode, Unicode, _returns=T)
        def GetValuesForASite(ctx, site, startDate, endDate, authToken=None):
            valuesResult = WOFService.GetValuesForASiteObject(
                ctx,
                site,
                startDate,
                endDate,
                authToken
            )
            if T_name == wof._SERVICE_PARAMS["r_type"]:
                return valuesResult
            return valuesResult.replace('\n', '')

    def _on_method_return_xml(ctx):
        # whatever etree element you return is the final xml
        # response, so to prevent the extraneous ("%sResult" %
        # method_name) result element, we just need to return an
        # element that has that element removed and replaced with
        # its child, which was the original response

        # TODO: how do we determine which method is being returned from?
        # Since I don't know, I am doing a dumb test for each one
        print('_on_method_return_xml')
        if 'xml' and 'soap' not in list(ctx.out_protocol.type):
            logger.info("protocol types: %s" % list(ctx.out_protocol.type))
            modify_return_xml_object(ctx)
            print("protocol types: %s" % list(ctx.out_protocol.type))
            print(ctx._MethodContext__descriptor.name)
            return ctx

        if ctx._MethodContext__descriptor.name == 'GetValuesObject':
            print('GetValuesObject')
            modify_method_getValueObject(ctx)
            return ctx
        if ctx._MethodContext__descriptor.name == 'GetVariableInfoObject':
            print('GetVariableInfoObject')
            modify_method_getVariableInfoObject(ctx)
            return ctx

        if wof._SERVICE_PARAMS["s_type"] in list(ctx.out_protocol.type):
            print('wof._SERVICE_PARAMS["s_type"]')
            result_element_name_list = [
                'GetSitesObjectResult',
                'GetSiteInfoObjectResult',
                'GetVariableInfoObjectResult',
                'GetValuesObjectResult',
                'GetSitesByBoxObjectResult',
                'GetSiteInfoMultpleObjectResult',
                'GetVariablesObjectResult',
                'GetValuesForASiteObjectResult']
            element = ctx.out_document
            for result_element_name in result_element_name_list:
                print('result_element_name')
                print(result_element_name)
                result_element = element.find(
                        './/{%s}%s' % (
                            element.nsmap['tns'],
                            result_element_name
                        )
                )

                if result_element is not None:
                    parent = result_element.getparent()
                    children = result_element.getchildren()
                    parent.replace(result_element, children[0])
                    print('result_element_name')
                    print(result_element)
                    return ctx
        print('nothing returned')
        return

    def modify_method_getValueObject(ctx):
        logger.info("Modify WOF11 GetValuesObject request")
        result_element_name = 'GetValuesObjectResult'
        response_element_name = 'GetValuesObjectResponse'
        WML_NAMESPACE = "http://www.cuahsi.org/waterML/1.1/"
        WML = "{%s}" % WML_NAMESPACE
        NSMAP = {None: WML_NAMESPACE}
        wrappingElement = etree.Element(WML + "TimeSeriesResponse", nsmap=NSMAP)  # noqa

        element = ctx.out_document
        element.nsmap[None] = NSMAP
        response_element = element.find('.//{%s}%s' % (element.nsmap['tns'], response_element_name))  # noqa
        result_element=element.find('.//{%s}%s' % (element.nsmap['tns'], result_element_name))  # noqa
        parent = response_element.getparent()
        parent.append(wrappingElement)  # Add to tree.
        children = result_element.getchildren()
        wrappingElement.append(children[0])
        # parent.replace(result_element, children[0])

        parent.remove(response_element)

        return ctx

    def modify_method_getVariableInfoObject(ctx):
        logger.info("Modify WOF11 VariableInfoObject request")
        result_element_name = 'GetVariableInfoObjectResult'
        response_element_name = 'GetVariableInfoObjectResponse'
        WML_NAMESPACE = "http://www.cuahsi.org/waterML/1.1/"
        WML = "{%s}" % WML_NAMESPACE
        NSMAP = {None: WML_NAMESPACE}
        wrappingElement = etree.Element(WML + "VariablesResponse", nsmap=NSMAP)  # noqa

        element = ctx.out_document
        element.nsmap[None] = NSMAP
        response_element = element.find('.//{%s}%s' % (element.nsmap['tns'], response_element_name))  # noqa
        result_element=element.find('.//{%s}%s' % (element.nsmap['tns'], result_element_name))  # noqa
        parent = response_element.getparent()
        parent.append(wrappingElement)  # Add to tree.
        children = result_element.getchildren()
        wrappingElement.append(children[0])
        # parent.replace(result_element, children[0])
        parent.remove(response_element)

        return ctx

    def modify_return_xml_object(ctx):
        print('modify_return_xml_object')
        method_name = ctx._MethodContext__descriptor.name
        logger.info("Modify WOF11 %s request" % method_name)
        result_element_name = '%sResult' % method_name
        print('method name')
        print(method_name)
        element = ctx.out_document
        print('find result element')
        print(element.nsmap['ns0'])
        result_element = element.find('.//{%s}%s' % (element.nsmap['ns0'], result_element_name))  # noqa
        if result_element is None:
            result_element = element.find('.//{%s}%s' % (element.nsmap['ns0'], 'GetSiteInfoResponse'))
        print(result_element)
        if not result_element is None:
            children = result_element.getchildren()
            ctx.out_document = children[0]
        # print(ctx)
        return ctx

    WOFService.event_manager.add_listener(
        'method_return_document',
        _on_method_return_xml
    )

    return WOFService
