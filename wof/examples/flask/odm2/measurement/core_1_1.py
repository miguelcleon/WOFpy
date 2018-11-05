from __future__ import (absolute_import, division, print_function)

import datetime
import logging
from xml.sax.saxutils import escape

import dateutil.parser

from wof import WaterML_1_1 as WaterML
from wof import core
from wof import vocabularies as voc


class WOF_1_1(object):

    network = 'NETWORK'
    vocabulary = 'VOCABULARY'
    menu_group_name = 'MENU_GROUP_NAME'
    service_wsdl = 'SERVICE_WSDL'
    timezone = None
    timezone_abbr = None

    dao = None

    default_site = None
    default_variable = None
    default_start_date = None
    default_end_date = None
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

    _config = None
    _templates = None

    def __init__(self, dao, config_file=None, templates=None):
        self.dao = dao
        if templates is not None:
            self._templates = templates
        if config_file:
            self.config_from_file(config_file)

    def config_from_file(self, file_name):
        if self._templates:
            config = core.WOFConfig(file_name, self._templates)
        else:
            config = core.WOFConfig(file_name)
        self._config = config

        self.network = config.network.lower()
        self.vocabulary = config.vocabulary.lower()
        self.urlpath = config.urlpath.lower()
        self.menu_group_name = config.menu_group_name
        self.service_wsdl = config.service_wsdl
        self.timezone = config.timezone
        self.timezone_abbr = config.timezone_abbr

        self.default_site = config.default_site
        self.default_variable = config.default_variable
        self.default_start_date = config.default_start_date
        self.default_end_date = config.default_end_date
        self.default_unitid = config.default_unitid
        self.default_samplemedium = config.default_samplemedium

        self.default_east = config.default_east
        self.default_north = config.default_north
        self.default_south = config.default_south
        self.default_west = config.default_west

        self.organization = config.organization
        self.address = config.address
        self.city = config.city
        self.state = config.state
        self.zipcode = config.zipcode
        self.contactname = config.contactname
        self.contactemail = config.contactemail
        self.phone = config.phone
        self.link = config.link

    def get_site_code(self, siteArg):

        if ':' in siteArg:
            networkname, siteCode = siteArg.split(':', 1)
            networkname = networkname.lower()
	    # print('networkname!!')
	    # print(networkname)
	    # print(siteCode)
            # print(self.network)
            if self.network == networkname:
                return siteCode
            else:
                return None
        return siteArg

    def get_variable_code(self, varArg):

        if ':' in varArg:
            vocabname, varCode = varArg.split(':', 1)
            vocabname = vocabname.lower()
            if self.vocabulary == vocabname:
                return varCode
            else:
                return None
        return varArg

    def create_get_site_response(self, siteArg=None):
        print('here create_get_site_response')
        if siteArg is None or siteArg == '':
            siteResultArr = self.dao.get_all_sites()
        else:
            siteCodesArr = siteArg.split(',')
            siteCodesArr = [self.get_site_code(s)
                            for s in siteCodesArr]
            siteResultArr = self.dao.get_sites_by_codes(siteCodesArr)

        # if len(siteResultArr) == 0:
        #    return None

        siteInfoResponse = WaterML.SiteInfoResponseType()
        queryInfo = WaterML.QueryInfoType(creationTime=datetime.datetime.now())
        # TODO: check on how this should be done for multiple sites.
        pType = WaterML.parameterType(name='site', value=siteArg)
        criteria = WaterML.criteriaType(MethodCalled='GetSites')
        criteria.add_parameter(pType)
        queryInfo.set_criteria(criteria)
        # queryInfoNote = WaterML.NoteType()
        # queryInfo.add_note(queryInfoNote)
        # queryInfo.set_extension('')
        siteInfoResponse.set_queryInfo(queryInfo)

        if siteResultArr:
            for siteResult in siteResultArr:
                s = self.create_site_element(siteResult)
                siteInfoResponse.add_site(s)
        else:
            # site = WaterML.site()
            # siteInfoResponse.add_site(site)
            raise Exception("Site {} Not Found".format(siteArg))

        return siteInfoResponse

    def create_get_site_info_response(self, siteArg, varArg=None):
        print('here create_get_site_info_response')
        siteCode = self.get_site_code(siteArg)
        siteResult = self.dao.get_site_by_code(siteCode)
        print('siteCode!!')
        print(siteCode)
        if (varArg is None or varArg == 'get_series_by_sitecode'):
            print('get_series_by_sitecode')
            seriesResultArr = self.dao.get_series_by_sitecode(siteCode)
        else:
            print('get_series_by_sitecode_and_varcode')
            varCode = self.get_variable_code(varArg)
            seriesResultArr = self.dao.get_series_by_sitecode_and_varcode(
                siteCode, varCode)

        # if len(seriesResultArr) == 0:
        #    return None
        # print(' variable code')
        # print(varCode)
        siteInfoResponse = WaterML.SiteInfoResponseType()

        queryInfo = WaterML.QueryInfoType(creationTime=datetime.datetime.now())
        criteria = WaterML.criteriaType(MethodCalled='GetSiteInfo')
        pType_site = WaterML.parameterType(name='site', value=siteArg)
        criteria.add_parameter(pType_site)
        if varArg is not None:
            pType_var = WaterML.parameterType(name='variable', value=varArg)
            criteria.add_parameter(pType_var)
        queryInfo.set_criteria(criteria)
        # queryInfoNote = WaterML.NoteType()
        # queryInfo.add_note(queryInfoNote)
        # queryInfo.set_extension('')
        siteInfoResponse.set_queryInfo(queryInfo)

        s = self.create_site_element(siteResult, seriesResultArr)
        siteInfoResponse.add_site(s)
        print('site info response!')
        print(siteInfoResponse)
        return siteInfoResponse

    def create_get_site_info_multiple_response(self, siteArg):
        siteCodesArr = siteArg.split(',')
        siteCodesArr = [self.get_site_code(s)
                        for s in siteCodesArr]

        siteInfoResponse = WaterML.SiteInfoResponseType()

        queryInfo = WaterML.QueryInfoType(creationTime=datetime.datetime.now())
        criteria = WaterML.criteriaType(MethodCalled='GetSiteInfo')
        pType_site = WaterML.parameterType(name='site', value=siteArg)
        criteria.add_parameter(pType_site)
        queryInfo.set_criteria(criteria)
        # queryInfoNote = WaterML.NoteType()
        # queryInfo.add_note(queryInfoNote)
        # queryInfo.set_extension('')
        siteInfoResponse.set_queryInfo(queryInfo)

        for siteArg in siteCodesArr:
            siteCode = self.get_site_code(siteArg)
            siteResult = self.dao.get_site_by_code(siteCode)
            seriesResultArr = self.dao.get_series_by_sitecode(siteCode)

            # if len(seriesResultArr) == 0:
            #    return None
            s = self.create_site_element(siteResult, seriesResultArr)
            siteInfoResponse.add_site(s)

        return siteInfoResponse

    def to_bool(self, value):
        valid = {
            'true': True,
            't': True,
            '1': True,
            'false': False,
            'f': False,
            '0': False,
        }

        if isinstance(value, bool):
            return value

        if not isinstance(value, basestring):
            value = False
            return value

        lower_value = value.lower()
        if lower_value in valid:
            return valid[lower_value]
        else:
            value = False
            return value

    def create_get_site_box_response(self, west, south, east, north,
                                     IncludeSeries):

        IncludeSeries = self.to_bool(IncludeSeries)
        siteResultArr = self.dao.get_sites_by_box(west, south, east, north)

        siteInfoResponse = WaterML.SiteInfoResponseType()

        queryInfo = WaterML.QueryInfoType(creationTime=datetime.datetime.now())
        criteria = WaterML.criteriaType(MethodCalled='GetSitesByBox')
        pType_west = WaterML.parameterType(name='west', value=west)
        pType_west = WaterML.parameterType(name='south', value=south)
        pType_west = WaterML.parameterType(name='east', value=east)
        pType_west = WaterML.parameterType(name='north', value=north)
        pType_west = WaterML.parameterType(name='IncludeSeries', value=IncludeSeries)  # noqa
        criteria.add_parameter(pType_west)
        queryInfo.set_criteria(criteria)
        # queryInfoNote = WaterML.NoteType()
        # queryInfo.add_note(queryInfoNote)
        # queryInfo.set_extension('')
        siteInfoResponse.set_queryInfo(queryInfo)

        for siteResult in siteResultArr:
            seriesResultArr = None
            if IncludeSeries:
                seriesResultArr = self.dao.get_series_by_sitecode(siteResult.SiteCode)  # noqa

            # if len(seriesResultArr) == 0:
            #    return None
            s = self.create_site_element(
                siteResult,
                seriesResultArr,
                IncludeSeries
            )
            siteInfoResponse.add_site(s)

        return siteInfoResponse

    def create_get_variable_info_response(self, varArg=None):

        if (varArg is None or varArg == ''):
            variableResultArr = self.dao.get_all_variables()
        else:
            varCodesArr = varArg.split(',')
            varCodesArr = [self.get_variable_code(v)
                           for v in varCodesArr]
            variableResultArr = self.dao.get_variables_by_codes(varCodesArr)

        variableInfoResponse = WaterML.VariablesResponseType()

        # TODO: Should queryInfo be in thois response?  Suds doesn't
        # like when it is.  If it should be in the response, then the
        # WSDL needs to be updated

        queryInfo = WaterML.QueryInfoType()
        criteria = WaterML.criteriaType(MethodCalled='GetVariableInfo')
        if varArg is not None:
            pType_var = WaterML.parameterType(name='variable', value=varArg)
            criteria.add_parameter(pType_var)
        queryInfo.set_criteria(criteria)
        queryInfoNote = WaterML.NoteType('Web Service')
        queryInfo.add_note(queryInfoNote)
        # queryInfo.set_extension('')
        variableInfoResponse.set_queryInfo(queryInfo)

        variables = WaterML.variablesType()
        for variableResult in variableResultArr:
            v = self.create_variable_element(variableResult)
            variables.add_variable(v)
        variableInfoResponse.set_variables(variables)
        return variableInfoResponse

    def create_get_values_response(self, siteArg, varArg, startDateTime=None,
                                   endDateTime=None):

        # TODO: Tim thinks the DAO should handle network and vocab parsing,
        #      not WOF
        siteCode = self.get_site_code(siteArg)
        varCode = self.get_variable_code(varArg)

        valueResultArr = self.dao.get_datavalues(siteCode, varCode,
                                                 startDateTime, endDateTime)
        # if not valueResultArr:
        #    raise Exception(
        #        "ERROR: No data found for {}:{} for dates {} - {}".format(
        #            siteCode, varCode, startDateTime, endDateTime
        #        )
        #    )

        timeSeriesResponse = WaterML.TimeSeriesResponseType()

        queryInfo = WaterML.QueryInfoType(creationTime=datetime.datetime.now())
        criteria = WaterML.criteriaType(MethodCalled='GetValues')
        pType_site = WaterML.parameterType(name='site', value=siteArg)
        criteria.add_parameter(pType_site)
        pType_var = WaterML.parameterType(name='variable', value=varArg)
        criteria.add_parameter(pType_var)
        if startDateTime is not None:
            pType_sdate = WaterML.parameterType(
                name='startDate',
                value=startDateTime
            )
            criteria.add_parameter(pType_sdate)
        if endDateTime is not None:
            pType_edate = WaterML.parameterType(
                name='endDate',
                value=endDateTime
            )
            criteria.add_parameter(pType_edate)
        queryInfo.set_criteria(criteria)
        # queryInfoNote = WaterML.NoteType()
        # queryInfo.add_note(queryInfoNote)
        # queryInfo.set_extension('')
        timeSeriesResponse.set_queryInfo(queryInfo)

        # if not valueResultArr:
        #    timeSeries = WaterML.TimeSeriesType()
        #    timeSeriesResponse.add_timeSeries(timeSeries)
        #    return timeSeriesResponse

        if not valueResultArr:
            raise Exception(
                "Values Not Found for {}:{} for dates {} - {}".format(
                    siteCode, varCode, startDateTime, endDateTime)
                )

        if isinstance(valueResultArr, dict):
            for key in valueResultArr.keys():
                timeSeries = self.create_timeseries(
                    siteCode,
                    key,
                    valueResultArr[key]
                )
                timeSeriesResponse.add_timeSeries(timeSeries)
        else:
            timeSeries = self.create_timeseries(
                siteCode,
                varCode,
                valueResultArr
            )
            timeSeriesResponse.add_timeSeries(timeSeries)
        return timeSeriesResponse

    def create_timeseries(self, siteCode, varCode, valueResultArr):

        timeSeries = WaterML.TimeSeriesType()

        # sourceInfo (which is a siteInfo) element
        siteResult = self.dao.get_site_by_code(siteCode)

        # TODO: Exception?
        if not siteResult:
            pass

        sourceInfo = self.create_site_info_element(siteResult)
        timeSeries.sourceInfo = sourceInfo

        # Variable element.
        varResult = self.dao.get_variable_by_code(varCode)

        # TODO: Exception?
        if not varResult:
            pass

        variable = self.create_variable_element(varResult)
        timeSeries.variable = variable

        # TODO: fill in some more of the attributes in this element.
        values = WaterML.TsValuesSingleVariableType()

        # waterML 1.0
        # values.count = len(valueResultArr)
        # if varResult.VariableUnits:
        #    values.unitsAbbreviation = varResult.VariableUnits.UnitsAbbreviation  # noqa
        #    values.unitsCode = varResult.VariableUnits.UnitsID

        # Need to keep track of unique methodIDs and sourceIDs.
        methodIdSet = set()
        sourceIdSet = set()
        qualifierIdSet = set()
        offsetTypeIdSet = set()
        qualitycontrollevelIdSet = set()
        censorcodeset = {}

        for valueResult in valueResultArr:
            v = self.create_value_element(valueResult)
            values.add_value(v)
            if valueResult.MethodID is not None and valueResult.MethodCode:
                methodIdSet.add(valueResult.MethodID)

            if valueResult.SourceID is not None:
                sourceIdSet.add(valueResult.SourceID)

            if valueResult.QualifierID is not None:
                qualifierIdSet.add(valueResult.QualifierID)

            if valueResult.OffsetTypeID is not None:
                offsetTypeIdSet.add(valueResult.OffsetTypeID)

            if valueResult.QualityControlLevelID is not None:
                qualitycontrollevelIdSet.add(valueResult.QualityControlLevelID)

            if valueResult.CensorCode is not None:
                censorcode = v.get_censorCode()
                censorcodeset[censorcode] = valueResult.CensorCode

        # Add method elements for each unique methodID.
        if methodIdSet:
            methodIdArr = list(methodIdSet)
            methodResultArr = self.dao.get_methods_by_ids(methodIdArr)
            for methodResult in methodResultArr:
                method = self.create_method_element(methodResult)
                values.add_method(method)

        # Add source elements for each unique sourceID.
        if sourceIdSet:
            sourceIdArr = list(sourceIdSet)
            sourceResultArr = self.dao.get_sources_by_ids(sourceIdArr)
            for sourceResult in sourceResultArr:
                source = self.create_source_element(sourceResult)
                values.add_source(source)

        # Add qualifier elements.
        if qualifierIdSet:
            qualIdArr = list(qualifierIdSet)
            qualResultArr = self.dao.get_qualifiers_by_ids(qualIdArr)
            for qualifierResult in qualResultArr:
                q = WaterML.QualifierType(
                    qualifierID=qualifierResult.QualifierID,
                    default=None,
                    network=self.network,
                    vocabulary=self.vocabulary,
                    qualifierCode=qualifierResult.QualifierCode)
                values.add_qualifier(q)

        # Add offset elements
        if offsetTypeIdSet:
            offsetTypeIdArr = list(offsetTypeIdSet)
            offsetTypeResultArr = self.dao.get_offsettypes_by_ids(
                offsetTypeIdArr)
            for offsetTypeResult in offsetTypeResultArr:
                offset = self.create_offset_element(offsetTypeResult)
                values.add_offset(offset)

        # Add qualitycontrollevel elements.
        if qualitycontrollevelIdSet:
            qlevelIdIdArr = list(qualitycontrollevelIdSet)
            try:
                qlevelResultArr = self.dao.get_qualcontrollvls_by_ids(qlevelIdIdArr)  # noqa
                for qlevelResult in qlevelResultArr:
                    qlevel = self.create_qlevel_element(qlevelResult)
                    values.add_qualityControlLevel(qlevel)
            except:
                for qlevelID in qlevelIdIdArr:
                    qlevel = WaterML.QualityControlLevelType(
                        qualityControlLevelID=qlevelID)
                    values.add_qualityControlLevel(qlevel)

        # Add censorcode element.
        if censorcodeset:
            for key in censorcodeset:
                cCode = WaterML.CensorCodeType(
                    censorCode=key,
                    censorCodeDescription=censorcodeset[key]
                )
                values.add_censorCode(cCode)

        timeSeries.add_values(values)
        # timeSeriesResponse.set_timeSeries(timeSeries)
        return timeSeries

    def create_get_values_site_response(self, site,
                                        startDateTime,
                                        endDateTime):

        timeSeriesResponse = WaterML.TimeSeriesResponseType()
        queryInfo = WaterML.QueryInfoType(creationTime=datetime.datetime.now())
        criteria = WaterML.criteriaType(MethodCalled='GetValuesForASite')
        pType_site = WaterML.parameterType(name='site', value=site)
        criteria.add_parameter(pType_site)
        if startDateTime is not None:
            pType_sdate = WaterML.parameterType(
                name='startDate',
                value=startDateTime
            )
            criteria.add_parameter(pType_sdate)
        if endDateTime is not None:
            pType_edate = WaterML.parameterType(
                name='endDate',
                value=endDateTime
            )
            criteria.add_parameter(pType_edate)
        queryInfo.set_criteria(criteria)
        # queryInfoNote = WaterML.NoteType()
        # queryInfo.add_note(queryInfoNote)
        # queryInfo.set_extension('')
        timeSeriesResponse.set_queryInfo(queryInfo)

        siteCode = self.get_site_code(site)
        seriesResultArr = self.dao.get_series_by_sitecode(siteCode)
        if seriesResultArr:
            for seriesResult in seriesResultArr:
                valueResultArr = self.dao.get_datavalues(
                    siteCode,
                    seriesResult.Variable.VariableCode,
                    startDateTime,
                    endDateTime
                )
                # if not valueResultArr:
                #    raise Exception(
                #        ('ERROR: No data found for {} for dates '
                #         '{} - {}').format(site, startDateTime, endDateTime))
                if not valueResultArr:
                    continue

                timeSeries = self.create_timeseries(
                    siteCode,
                    seriesResult.Variable.VariableCode,
                    valueResultArr
                )
                timeSeriesResponse.add_timeSeries(timeSeries)

        return timeSeriesResponse

    def create_qlevel_element(self, qlevelResult):
        qlevel = WaterML.QualityControlLevelType(
            qualityControlLevelID=qlevelResult.QualityControlLevelID,
            qualityControlLevelCode=qlevelResult.QualityControlLevelID,
            definition=qlevelResult.Definition,
            explanation=qlevelResult.Explanation
        )
        return qlevel

    def create_offset_element(self, offsetTypeResult):
        # TODO: where does offsetIsVertical come from.
        # TODO: where does offsetHorizDirectionDegrees come from?
        offset = WaterML.OffsetType(
            offsetTypeID=offsetTypeResult.OffsetTypeID,
            offsetValue=None,
            offsetDescription=offsetTypeResult.OffsetDescription,
            offsetIsVertical=True)

        if offsetTypeResult.OffsetUnits:
            units = WaterML.UnitsType(
                unitID=offsetTypeResult.OffsetUnits.UnitsID,
                unitAbbreviation=offsetTypeResult.OffsetUnits.UnitsAbbreviation,  # noqa
                unitName=offsetTypeResult.OffsetUnits.UnitsName,
                unitType=offsetTypeResult.OffsetUnits.UnitsType,
                unitCode=offsetTypeResult.OffsetUnits.UnitsID
            )

            offset.units = units

        return offset

    def create_method_element(self, methodResult):
        method = WaterML.MethodType(
            methodID=methodResult.MethodID,
            methodCode=methodResult.MethodID,
            methodDescription=methodResult.MethodDescription,
            methodLink=methodResult.MethodLink)

        # Need at least one MethodLink element to meet WaterML 1.0
        # schema validation.
        if method.methodLink is None:
            method.methodLink = ''

        return method

    def create_source_element(self, sourceResult):
        source = WaterML.SourceType(
            sourceID=sourceResult.SourceID,
            sourceCode=sourceResult.SourceCode,
            organization=sourceResult.Organization,
            sourceDescription=sourceResult.SourceDescription,
            sourceLink=sourceResult.SourceLink)

        contactInfo = self.create_contact_info_element(sourceResult)

        source.contactInformation = [contactInfo]

        if sourceResult.Metadata:
            metadata = WaterML.MetaDataType(
                topicCategory=sourceResult.Metadata.TopicCategory,
                title=sourceResult.Metadata.Title,
                abstract=sourceResult.Metadata.Abstract,
                profileVersion=sourceResult.Metadata.ProfileVersion,
                metadataLink=sourceResult.Metadata.MetadataLink)

            source.Metadata = metadata

        return source

    def create_contact_info_element(self, sourceResult):

        if sourceResult.ContactName:

            addressString = ''
            if sourceResult.Address:
                addressString = '{}'.format(sourceResult.Address)

            contactInfo = WaterML.ContactInformationType(
                email=[sourceResult.Email],
                contactName=sourceResult.ContactName,
                phone=[sourceResult.Phone],
                address=[addressString])

            return contactInfo

        return None

    # TODO: lots more stuff to fill out here.
    def create_value_element(self, valueResult):
        # datetime_string = core._get_datavalues_datetime(
        #  valueResult, "LocalDateTime", "DateTimeUTC")
        aDate = core._get_datavalues_datetime(
            valueResult,
            "LocalDateTime",
            "DateTimeUTC"
        )
        # aDate= dateutil.parser.parse(datetime_string)

        if not hasattr(valueResult, 'MethodCode'):
            setattr(valueResult, 'MethodCode', None)
        if not hasattr(valueResult, 'SourceCode'):
            setattr(valueResult, 'SourceCode', None)

        clean_censorCode = voc.check_censorCode(valueResult.CensorCode)
        # clean_qcl = self.check_QualityControlLevel(valueResult.QualityControlLevel)  # noqa

        value = WaterML.ValueSingleVariable(
            qualityControlLevelCode=valueResult.QualityControlLevelID,
            methodCode=valueResult.MethodID,
            sourceCode=valueResult.SourceCode,
            timeOffset=valueResult.UTCOffset,
            censorCode=clean_censorCode,
            sampleID=valueResult.SampleID,
            offsetTypeID=valueResult.OffsetTypeID,
            accuracyStdDev=valueResult.ValueAccuracy,
            offsetValue=valueResult.OffsetValue,
            # dateTime=datetime_string,
            dateTime=aDate,
            dateTimeUTC=valueResult.DateTimeUTC,
            qualifiers=valueResult.QualifierID,
            valueOf_=str(valueResult.DataValue)
        )

        # TODO: value.offset stuff?  Why does value element have all
        # this offset stuff
        # offsetTypeResult = valueResult.OffsetType
        # if offsetTypeResult != None:
        #    value.offsetDescription = offsetTypeResult.OffsetDescription
        #    value.offsetUnitsAbbreviation = offsetTypeResult.OffsetUnits.UnitsAbbreviation  # noqa
        #    value.offsetUnitsCode = offsetTypeResult.OffsetUnits.UnitsID
        return value

    def create_site_element(self, siteResult,
                            seriesResultArr=None, IncludeSeries=True):
        site = WaterML.siteType()
        siteInfo = self.create_site_info_element(siteResult)

        site.set_siteInfo(siteInfo)

        if any([siteResult.County, siteResult.State, siteResult.Comments]):
            if siteResult.County:
                countyNote = WaterML.PropertyType(
                    name="County",
                    valueOf_=siteResult.County
                )
                siteInfo.add_siteProperty(countyNote)

            if siteResult.State:
                stateNote = WaterML.PropertyType(
                    name="State",
                    valueOf_=siteResult.State
                )
                siteInfo.add_siteProperty(stateNote)

            if siteResult.Comments:
                commentsNote = WaterML.PropertyType(
                    name="Site Comments",
                    valueOf_=escape(siteResult.Comments))
                siteInfo.add_siteProperty(commentsNote)

        if IncludeSeries:
            seriesCatalog = WaterML.seriesCatalogType()
            if seriesResultArr is not None:
                seriesCatalog.menuGroupName = self.menu_group_name
                # TODO: Make sure this is set properly in config filename.
                seriesCatalog.serviceWsdl = self.service_wsdl

                for seriesResult in seriesResultArr:
                    series = self.create_series_element(seriesResult)
                    seriesCatalog.add_series(series)

            site.add_seriesCatalog(seriesCatalog)

        # need at least one extension element to meet WaterML 1.0
        # schema validation
        # site.set_extension('')
        return site

    def create_site_info_element(self, siteResult):
        siteInfo = WaterML.SiteInfoType()
        siteInfo.set_siteName(siteResult.SiteName)

        # TODO: agencyName
        siteCode = WaterML.siteCodeType(
            network=self.network,
            siteID=siteResult.SiteID,
            valueOf_=siteResult.SiteCode,
            agencyCode=siteResult.AgencyCode,
            agencyName=siteResult.AgencyName
        )

        siteInfo.add_siteCode(siteCode)

        # TODO: Maybe remove this?  None of the other WOF services
        # return this info probably because it is not that useful
        # timeZoneInfo = WaterML.timeZoneInfoType(
        #    siteUsesDaylightSavingsTime=False,
        #    daylightSavingsTimeZone=None
        # )
        # timeZoneInfo.defaultTimeZone = WaterML.defaultTimeZoneType(
        #    zoneOffset=self.timezone,
        #    zoneAbbreviation=self.timezone_abbr)

        # siteInfo.set_timeZoneInfo(timeZoneInfo)
        geoLocation = WaterML.geoLocationType()
        geogLocation = WaterML.LatLonPointType(
            srs="EPSG:{0}".format(siteResult.LatLongDatum.SRSID),
            latitude=siteResult.Latitude,
            longitude=siteResult.Longitude)
        geoLocation.set_geogLocation(geogLocation)

        if (siteResult.LocalX and siteResult.LocalY):
            localSiteXY = WaterML.localSiteXYType()
            localSiteXY.projectionInformation = siteResult.LocalProjection.SRSName  # noqa
            localSiteXY.X = siteResult.LocalX
            localSiteXY.Y = siteResult.LocalY
            geoLocation.add_localSiteXY(localSiteXY)

        siteInfo.set_geoLocation(geoLocation)
        siteInfo.set_verticalDatum(siteResult.VerticalDatum)

        # need at least one extension element to meet WaterML 1.0
        # schema validation

        # siteInfo.set_extension('')

        # need at least one altname element to meet WaterML 1.0 schema
        # validation
        # siteInfo.set_altname('')
        return siteInfo

    def create_series_element(self, seriesResult):
        series = WaterML.seriesType()

        # Variable
        variable = self.create_variable_element(seriesResult.Variable)
        series.set_variable(variable)

        series.valueCount = WaterML.valueCountType(
            valueOf_=str(seriesResult.ValueCount))

        beginDateTime = core._get_datavalues_datetime(
            seriesResult, "BeginDateTime", "BeginDateTimeUTC")
        endDateTime = core._get_datavalues_datetime(
            seriesResult, "EndDateTime", "EndDateTimeUTC")
        # WML1_1 wants a datetime, no zone. breaks the conversion
        try:
            beginDateTime = dateutil.parser.parse(beginDateTime)
        except Exception as inst:
            logging.warn('bad datetime conversion on beginDateTime:' + inst.message)  # noqa
        try:
            endDateTime = dateutil.parser.parse(endDateTime)
        except Exception as inst:
            logging.warn('bad datetime conversion on endDateTime:' + inst.message)  # noqa

        # TimeInterval.
        if beginDateTime is None:
            beginDateTime = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")  # noqa
        if endDateTime is None:
            endDateTime = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")  # noqa

        variableTimeInt = WaterML.TimeIntervalType(
            beginDateTime=beginDateTime,
            endDateTime=endDateTime)
        series.variableTimeInterval = variableTimeInt

        # Method.
        if seriesResult.Method:
            method = self.create_method_element(seriesResult.Method)
            series.method = method

        # Source.
        if seriesResult.Source:
            source = self.create_source_element(seriesResult.Source)
            series.source = source

        # QualityControlLevel.
        qualityControlLevel = WaterML.QualityControlLevelType(
            qualityControlLevelID=seriesResult.QualityControlLevelID,
            qualityControlLevelCode=seriesResult.QualityControlLevelID,
            definition=seriesResult.Definition,
            explanation=seriesResult.Explanation
        )
        series.qualityControlLevel = qualityControlLevel
        return series

    def create_variable_element(self, variableResult):
        clean_datatype = voc.check_dataTypeEnum(variableResult.DataType)
        clean_medium = voc.check_SampleMedium(variableResult.SampleMedium)
        # GeneralCategoryValidate and ValueTypeValidate optional capability added to support ODM2 CV handling
        _GeneralCategoryValidate = True
        if hasattr(variableResult, 'GeneralCategoryValidate'):
            _GeneralCategoryValidate = variableResult.GeneralCategoryValidate
        clean_category = voc.check_generalCategory(variableResult.GeneralCategory,
                                                  validate=_GeneralCategoryValidate)
        _ValueTypeValidate = True
        if hasattr(variableResult, 'ValueTypeValidate'):
            _ValueTypeValidate = variableResult.ValueTypeValidate
        clean_valuetype = voc.check_valueType(variableResult.ValueType,
                                              validate=_ValueTypeValidate)

        variable = WaterML.VariableInfoType(
            variableName=variableResult.VariableName,
            valueType=clean_valuetype,
            dataType=clean_datatype,
            generalCategory=clean_category,
            sampleMedium=clean_medium,
            noDataValue=variableResult.NoDataValue,
            variableDescription=variableResult.VariableDescription,
            speciation=variableResult.Speciation)

        # For specimen data.
        v_code = variableResult.VariableCode
        v_code_i = v_code.find('::')
        if v_code_i != -1:
            v_code = v_code[0:v_code_i]

        variableCode = WaterML.variableCodeType()
        variableCode.vocabulary = self.vocabulary
        # TODO: What is this, should it always be true?
        variableCode.default = "true"
        variableCode.variableID = variableResult.VariableID
        variableCode.valueOf_ = v_code

        variable.add_variableCode(variableCode)
        # UnitsTypeValidate optional capability added to support ODM2 CV handling
        _UnitsTypeValidate = True
        if hasattr(variableResult.VariableUnits, 'UnitsTypeValidate'):
            _UnitsTypeValidate = variableResult.VariableUnits.UnitsTypeValidate
        clean_variableUnitsType = voc.check_UnitsType(variableResult.VariableUnits.UnitsType,
                                                      validate=_UnitsTypeValidate)

        if variableResult.VariableUnits:
            units = WaterML.UnitsType(
                unitID=variableResult.VariableUnitsID,
                unitAbbreviation=variableResult.VariableUnits.UnitsAbbreviation,  # noqa
                unitCode=variableResult.VariableUnitsID,
                unitType=clean_variableUnitsType,
                unitName=variableResult.VariableUnits.UnitsName)

            variable.set_unit(units)

        timeSupport = WaterML.timeScaleType()
        timeSupport.isRegular = variableResult.IsRegular

        if variableResult.TimeUnits:
            timeUnits = WaterML.UnitsType(
                unitID=variableResult.TimeUnits.UnitsID,
                unitName=variableResult.TimeUnits.UnitsName,
                unitDescription=variableResult.TimeUnits.UnitsName,
                unitType=variableResult.TimeUnits.UnitsType,
                unitAbbreviation=variableResult.TimeUnits.UnitsAbbreviation,
                unitCode=variableResult.TimeUnits.UnitsID)

            timeSupport.set_unit(timeUnits)

        # TODO: time interval is not the same as time support.
        # Time interval refers to a spacing between values for regular data,
        # which isn't stored in ODM.
        if variableResult.TimeSupport:
            # integer in WaterML 1.0
            timeSupport.timeSupport = float(variableResult.TimeSupport)
        variable.set_timeScale(timeSupport)
        return variable

    def create_wml2_values_object(self, siteArg, varArg, startDateTime=None,
                                  endDateTime=None):
        siteCode = self.get_site_code(siteArg)
        varCode = self.get_variable_code(varArg)
        valueResultArr = self.dao.get_datavalues(
            siteCode,
            varCode,
            startDateTime,
            endDateTime
        )
        return valueResultArr
