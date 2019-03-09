from __future__ import (absolute_import, division, print_function)
import os
import yaml

from datetime import datetime
from sqlalchemy import create_engine, distinct, func
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql import and_

from dateutil.parser import parse

from wof.dao import BaseDao
import wof.examples.flask.odm2.measurement.sqlalch_odm2_models as model
import odm2api.ODM2.models as odm2_models
from sqlalchemy.orm import aliased
from sqlalchemy import or_
from sqlalchemy import and_
from sqlalchemy.sql import func

from sqlalchemy.orm import with_polymorphic
from sqlalchemy.dialects import postgresql
import re

class Odm2Dao(BaseDao):

    def __init__(self, db_connection_string):
        self.engine = create_engine(db_connection_string, convert_unicode=True,
            pool_size=100)
        odm2_models.setSchema(self.engine)
        # Default application pool size is 5. Use 100 to improve performance.
        self.db_session = scoped_session(sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine))
        #odm2_models.Base.query = self.db_session.query_property()
        # Read in WaterML -> ODM2 CV Mapping
        with open(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'cvmap_wml_1_1.yml'))) as yml:
            self.yml_dict = yaml.load(yml)
    def __del__(self):
        self.db_session.close()

    def db_check(self):
        try:
            self.db_session.query(odm2_models.SamplingFeatures).first()
        except:
            self.db_session.rollback()
        finally:
            pass

    def get_match(self, cvkey, term):
        for k, v in self.yml_dict[cvkey].items():
            if term in v:
                return k
        return term

    def get_all_sites(self):
        """Get all wof sites from odm2 database.

        :return: List of WOF Sites
        """
        self.db_check()
        s_rArr = self.db_session.query(odm2_models.Sites). \
            join(odm2_models.FeatureActions). \
            join(odm2_models.MeasurementResults). \
            filter(odm2_models.FeatureActions.SamplingFeatureID == odm2_models.Sites.SamplingFeatureID,
                   odm2_models.MeasurementResults.FeatureActionID == odm2_models.FeatureActions.FeatureActionID).distinct()  # noqa

        s_Arr = []
        for s_r in s_rArr:
            s = model.Site(s_r)
            s_Arr.append(s)
        return s_Arr

    def get_site_by_code(self, site_code):
        """Get wof site from odm2 database by site code.

        :param site_code: Site Code Ex. 'USU-LBR-Mendon'
        :return: WOF Site
        """
        self.db_check()
        w_s = None
        try:
            s = self.db_session.query(odm2_models.Sites). \
                filter(odm2_models.Sites.SamplingFeatureCode == site_code).one()

            aff = self.db_session.query(odm2_models.Affiliations). \
                join(odm2_models.ActionBy). \
                join(odm2_models.Actions). \
                join(odm2_models.FeatureActions). \
                join(odm2_models.Sites). \
                filter(odm2_models.Sites.SamplingFeatureCode == s.SamplingFeatureCode).first()
        # filter(odm2_models.ActionBy.IsActionLead == True) : may be too strict,
        # removed on 8/14/17
        except:
            s = None
            aff = None
        if s is not None:
            w_s = model.Site(s, aff)
        return w_s

    def get_sites_by_codes(self, site_codes_arr):
        """Get wof sites from odm2 database by a list of site codes.

        :param site_codes_arr: List of Site Codes Ex. ['USU-LBR-Mendon', 'USU-LBR-Mendon2']
        :return: List of WOF Sites
        """
        self.db_check()
        s_arr = []
        for site_code in site_codes_arr:
            w_s = self.get_site_by_code(site_code)
            if w_s is not None:
                s_arr.append(w_s)
        return s_arr

    def get_sites_by_box(self, west, south, east, north):
        """Get wof sites from odm2 database by a bounding box.

        :param north: north - ymax - latitude
        :param south: south - ymin - latitude
        :param west: west - xmin - longitude
        :param east: east - xmax - longitude
        :return: List of WOF Sites
        """
        self.db_check()
        s_rArr = self.db_session.query(odm2_models.Sites). \
            join(odm2_models.FeatureActions). \
            join(odm2_models.MeasurementResults). \
            filter(odm2_models.FeatureActions.SamplingFeatureID == odm2_models.Sites.SamplingFeatureID,
                   odm2_models.MeasurementResults.FeatureActionID == odm2_models.FeatureActions.FeatureActionID,  # noqa
                   odm2_models.Sites.Latitude >= south,
                   odm2_models.Sites.Latitude <= north,
                   odm2_models.Sites.Longitude >= west,
                   odm2_models.Sites.Longitude <= east).distinct()
        s_Arr = []
        for s_r in s_rArr:
            s = model.Site(s_r)
            s_Arr.append(s)
        return s_Arr


    def get_variable_params(self, var_code):
        """
        For machataria timeseries data: Variable code::unit id-sample medium
        """
        unitid = None
        samplemedium = None
        items = re.split('::|-',var_code)
        numofparams = len(items)
        if numofparams == 1:
            var_code = items[0]
        elif numofparams == 2:
            var_code = items[0]
            unitid = items[1]
        elif numofparams == 3:
            var_code = items[0]
            unitid = items[1]
            samplemedium = items[2]
        return var_code,unitid,samplemedium

    def get_variables_from_results(self,var_codes=None):
        l_var_codes = None
        if var_codes is not None:
            if not isinstance(var_codes, list):
                l_var_codes = []
                l_var_codes.append(var_codes)
            else:
                l_var_codes = var_codes

        r_m_Arr = []
        if l_var_codes is None:
            r_m = self.db_session.query(odm2_models.MeasurementResults).\
                    distinct(odm2_models.MeasurementResults.VariableID,
                             odm2_models.MeasurementResults.UnitsID,
                             odm2_models.MeasurementResults.SampledMediumCV).all()
            r_m_Arr.append(r_m)
        else:
            for item in l_var_codes:
                var_code, unitid, samplemedium = self.get_variable_params(item)

                q = self.db_session.query(odm2_models.MeasurementResults).\
                    distinct(odm2_models.MeasurementResults.VariableID,
                             odm2_models.MeasurementResults.UnitsID,
                             odm2_models.MeasurementResults.SampledMediumCV).\
                    join(odm2_models.Variables)
                q = q.filter(odm2_models.MeasurementResults.VariableID == odm2_models.Variables.VariableID,
                             odm2_models.Variables.VariableCode == var_code)

                if unitid is not None:
                    q = q.filter(odm2_models.MeasurementResults.UnitsID == int(unitid))
                if samplemedium is not None:
                    q = q.filter(odm2_models.MeasurementResults.SampledMediumCV == samplemedium)
                r_m = q.all()
                r_m_Arr.append(r_m)

        v_arr = []
        if len(r_m_Arr) is not 0:
            for result_m in r_m_Arr:
                for result in result_m:
                    v = result.VariableObj
                    u = result.UnitsObj
                    s = result.SampledMediumCV
                    t = result.TimeAggregationIntervalUnitsObj
                    ti = result.TimeAggregationInterval
                    ag = result.AggregationStatisticCV
                    at = result.FeatureActionObj.ActionObj.ActionTypeCV
                    w_v = model.Variable(v, s, u, t, ti, ag, at)
                    # w_v = model.Variable(v,s,u,t,ti)
                    # w_v.AggregationStatisticCV =  self.get_match('datatype', w_v.DataType)
                    w_v.DataType = self.get_match('datatype', w_v.DataType)
                    w_v.SampleMedium = self.get_match('samplemedium', w_v.SampleMedium)
                    v_arr.append(w_v)
        return v_arr

    def get_all_variables(self):
        v_arr = self.get_variables_from_results()
        return v_arr

    def get_variable_by_code(self, var_code):
        w_v = None
        v_arr = self.get_variables_from_results(var_code)
        if len(v_arr) is not 0:
            w_v = v_arr.pop()
        return w_v

    def _get_variables_by_codes(self, var_codes_arr):
        v_arr = self.get_variables_from_results(var_codes_arr)
        return v_arr





    def get_series_by_sitecode(self, site_code):
        site = self.get_site_by_code(site_code)
        # f site is None:
        # print('here OMG')
        # return None
        # odm2_models.MeasurementResults.SampledMediumCV
        # odm2_models.RelatedFeatures.RelatedFeatureID == site.SiteID

        # odm2_models.RelatedFeatures.RelatedFeatureID == site.SiteID
        # ,
        # odm2_models.MeasurementResults.SampledMediumCV
        self.db_check()
        result = self.db_session.query(odm2_models.MeasurementResults).\
                join(odm2_models.FeatureActions).\
                join(odm2_models.SamplingFeatures).\
                filter(odm2_models.SamplingFeatures.SamplingFeatureCode == site_code,
                       odm2_models.MeasurementResults.FeatureActionID == odm2_models.FeatureActions.FeatureActionID).\
                       group_by(odm2_models.MeasurementResults.VariableID,
                     odm2_models.MeasurementResults.ResultID,
                     odm2_models.Results.ResultID)
        # print('get the results')
        # print(len(result.all()))
        # print(str(result.statement.compile(dialect=postgresql.dialect())))
        result = result.all()
        # print('number of results')
        r_arr = []
        aff = None
        first_flag = True
        # for series in result:
        #     for item in r:
        ids = [i.ResultID for i in result]
        edt_dict = _get_msrv_enddatetimes(self.db_session, ids)
        sdt_dict = _get_msrv_startdatetimes(self.db_session, ids)

        # print('about to it results')
        for i in range(len(result)):
                # print('it result ' + str(result[i].ResultID))
                try:
                    # q = self.db_session.query(odm2_models.MeasurementResultValues.ResultID,
                    #                            func.max(
                    #                            odm2_models.MeasurementResultValues.ValueDateTime)).filter(  # noqa
                    #                            odm2_models.MeasurementResultValues.ResultID == result[i].ResultID).group_by(odm2_models.MeasurementResultValues.ResultID)
                    result[i].msrv_EndDateTime = edt_dict[result[i].ResultID] # q.first()[1]# [result[i].ResultID]# .ValueDateTime
                    # print('end date time')
                    # print(result[i].msrv_EndDateTime)
                    # print(str(q.statement.compile(dialect=postgresql.dialect())))
                    # edt_dict[result[i].ResultID]
                    result[i].msrv_BeginDateTime = sdt_dict[result[i].ResultID]
                except Exception as e:
                    print(str(e))
                    continue
                # print('set dates')
                if i == 0:

                    aff = self.db_session.query(odm2_models.Affiliations). \
                    join(odm2_models.ActionBy). \
                    filter(odm2_models.ActionBy.ActionID == result[i].FeatureActionObj.ActionID).first()  # noqa
                if aff is not None:
                    w_r = model.Series(result[i],aff, '2000-01-01T00:00:00', '2018-12-01T00:00:00') #,result[i].BeginDateTime,result[i].EndDateTime
                else:
                    w_r = model.Series(result[i],None, '2000-01-01T00:00:00', '2018-12-01T00:00:00') #,result[i].BeginDateTime,result[i].EndDateTime
                w_r.Variable.DataType = self.get_match('datatype', w_r.Variable.DataType)
                w_r.Variable.SampleMedium = self.get_match('samplemedium', w_r.Variable.SampleMedium)
                w_r.SampleMedium = w_r.Variable.SampleMedium
                # try:
                #    print(w_r.EndDateTimeUTC)
                #    print(w_r.BeginDateTimeUTC)
                # except:
                #     print('Error!')
                # print(w_r.Definition)
                r_arr.append(w_r)
        #  print('done')
        return r_arr

    def get_series_by_sitecode_and_varcode(self, site_code, var_code):

        site = self.get_site_by_code(site_code)
        if site is None:
            return None
        # print('HELLO HELLO')
        r = self.db_session.query(odm2_models.MeasurementResults.VariableID.label('vid'),
                                      odm2_models.MeasurementResults.UnitsID.label('unitid'),
                                      odm2_models.MeasurementResults.SampledMediumCV.label('samplemedium'),
                                      func.min(odm2_models.Actions.BeginDateTime).label("begindate"),
                                      func.max(odm2_models.Actions.EndDateTime).label("enddate")).\
                group_by(odm2_models.MeasurementResults.VariableID,
                         odm2_models.MeasurementResults.UnitsID,
                         odm2_models.MeasurementResults.SampledMediumCV).\
                join(odm2_models.FeatureActions).\
                join(odm2_models.Actions).\
                join(odm2_models.Specimens).\
                join(odm2_models.Variables).\
                filter(odm2_models.Specimens.SamplingFeatureID == odm2_models.RelatedFeatures.SamplingFeatureID,
                       odm2_models.MeasurementResults.FeatureActionID == odm2_models.FeatureActions.FeatureActionID,
                       odm2_models.RelatedFeatures.RelatedFeatureID == site.SiteID,
                       odm2_models.Variables.VariableCode == var_code).all()

        result = self.db_session.query(odm2_models.MeasurementResults).\
                distinct(odm2_models.MeasurementResults.VariableID,
                         odm2_models.MeasurementResults.UnitsID,
                         odm2_models.MeasurementResults.SampledMediumCV).\
                join(odm2_models.FeatureActions).\
                join(odm2_models.Variables).\
                join(odm2_models.Specimens).\
                join(odm2_models.Variables).\
                filter(odm2_models.Specimens.SamplingFeatureID == odm2_models.RelatedFeatures.SamplingFeatureID,
                       odm2_models.MeasurementResults.FeatureActionID == odm2_models.FeatureActions.FeatureActionID,
                       odm2_models.RelatedFeatures.RelatedFeatureID == site.SiteID,
                       odm2_models.Variables.VariableCode == var_code).all()

        r_arr = []
        aff = None
        first_flag = True
        for series in result:
            for item in r:
                if item.vid == series.VariableID and item.unitid == series.UnitsID:
                    if first_flag:
                        first_flag = False
                        aff = self.db_session.query(odm2_models.Affiliations).\
                                filter(odm2_models.Affiliations.OrganizationID == series.FeatureActionObj.ActionObj.MethodObj.OrganizationID).first()
                    w_r = model.Series(series,aff,item.begindate,item.enddate)
                    r_arr.append(w_r)

        return r_arr

    def get_specimen_data(self):
        q = self.db_session.query(odm2_models.MeasurementResultValues).\
                    join(odm2_models.MeasurementResults).\
                    join(odm2_models.FeatureActions).\
                    join(odm2_models.SamplingFeatures).\
                    join(odm2_models.Variables)
        return q

    def get_datavalues(self, site_code, var_code, begin_date_time=None,
                       end_date_time=None):

        site = self.get_site_by_code(site_code)
        var_code, unitid, samplemedium = self.get_variable_params(var_code)
        if site is None:
            return None

        if (not begin_date_time or not end_date_time):
            try:
                # print('HERE HERE')
                q = self.get_specimen_data()
                # print(len(q.all()))
                # print('all spcimen values')
                # print(site_code)
                # print(var_code)
                q = q.filter(
                    odm2_models.MeasurementResults.FeatureActionID == odm2_models.FeatureActions.FeatureActionID,  # noqa
                    odm2_models.MeasurementResults.VariableID == odm2_models.Variables.VariableID,
                    odm2_models.SamplingFeatures.SamplingFeatureCode == site_code,
                    odm2_models.Variables.VariableCode == var_code). \
                    order_by(odm2_models.MeasurementResultValues.ValueDateTime)
                # print(len(q.all()))
                # print('number of values')
                # q.filter(odm2_models.Specimens.SamplingFeatureID == odm2_models.RelatedFeatures.SamplingFeatureID,
                    #          odm2_models.MeasurementResults.FeatureActionID == odm2_models.FeatureActions.FeatureActionID,
                    #          odm2_models.RelatedFeatures.RelatedFeatureID == site.SiteID,
                    #          odm2_models.Variables.VariableCode == var_code)
                if unitid is not None:
                    q = q.filter(odm2_models.MeasurementResults.UnitsID == int(unitid))
                if samplemedium is not None:
                    q = q.filter(odm2_models.MeasurementResults.SampledMediumCV == samplemedium)
                # print(len(q.all()))
                # print('number of values more filtered')
                valueResultArr = q.all()
            except:
                valueResultArr = []
        else:
            # print('parse dates')
            begin_date_time = parse(begin_date_time)
            end_date_time = parse(end_date_time)
            # print(begin_date_time)
            # print(end_date_time)
            try:
                # print(len(q.all()))
                # print('all spcimen values')
                # print(site_code)
                # print(var_code)
                q = self.get_specimen_data()
                q = q.filter(odm2_models.MeasurementResults.FeatureActionID == odm2_models.FeatureActions.FeatureActionID,  # noqa
                    odm2_models.MeasurementResults.VariableID == odm2_models.Variables.VariableID,
                    odm2_models.SamplingFeatures.SamplingFeatureCode == site_code,odm2_models.Variables.VariableCode == var_code,
                             odm2_models.MeasurementResultValues.ValueDateTime >= begin_date_time,
                             odm2_models.MeasurementResultValues.ValueDateTime <= end_date_time).order_by(odm2_models.MeasurementResultValues.ValueDateTime)
                # print(len(q.all()))
                # print('number of values')
                if unitid is not None:
                    q = q.filter(odm2_models.MeasurementResults.UnitsID == int(unitid))
                if samplemedium is not None:
                    q = q.filter(odm2_models.MeasurementResults.SampledMediumCV == samplemedium)
                # print(len(q.all()))
                # print('number of filtered values')
                valueResultArr = q.all()
            except:
                valueResultArr = []

        v_dict = {}
        v_arr = []
        if len(valueResultArr) is not 0:
            aff = None
            first_flag = True
            if unitid is None and samplemedium is None:
                for valueResult in valueResultArr:
                    variable_key = '%s::%s-%s' % (valueResult.ResultObj.VariableObj.VariableCode,
                                              valueResult.ResultObj.UnitsID,
                                              valueResult.ResultObj.SampledMediumCV)
                    if first_flag:
                        first_flag = False
                        org_id = valueResult.ResultObj.FeatureActionObj.ActionObj.MethodObj.OrganizationID
                        aff = self.db_session.query(odm2_models.Affiliations).\
                            filter(odm2_models.Affiliations.OrganizationID == org_id).first()
                    w_v = model.DataValue(valueResult,aff)

                    if variable_key in v_dict:
                        a_list = v_dict.get(variable_key)
                        a_list.append(w_v)
                    else:
                        v_dict[variable_key] = []
                        v_dict[variable_key].append(w_v)
                return v_dict
            else:
                for valueResult in valueResultArr:
                    if first_flag:
                        first_flag = False
                        org_id = valueResult.ResultObj.FeatureActionObj.ActionObj.MethodObj.OrganizationID
                        aff = self.db_session.query(odm2_models.Affiliations).\
                            filter(odm2_models.Affiliations.OrganizationID == org_id).first()
                    w_v = model.DataValue(valueResult,aff)
                    v_arr.append(w_v)
                return v_arr

    def get_method_by_id(self, method_id):
        m = self.db_session.query(odm2_models.Methods).\
            filter(odm2_models.Methods.MethodID == method_id).first()
        w_m = model.Method(m)
        return w_m

    def get_methods_by_ids(self, method_id_arr):
        m = self.db_session.query(odm2_models.Methods).\
            filter(odm2_models.Methods.MethodID.in_(method_id_arr)).all()
        m_arr = []
        for i in range(len(m)):
            w_m = model.Method(m[i])
            m_arr.append(w_m)
        return m_arr

    def get_source_by_id(self, source_id):
        """Get wof source from odm2 database by Affiliation ID.

        :param source_id: Affiliation ID.
        :return: A WOF Source
        """
        self.db_check()
        aff = self.db_session.query(odm2_models.Affiliations). \
            filter(odm2_models.Affiliations.AffiliationID == source_id).one()
        w_aff = model.Source(aff)
        return w_aff

    def get_sources_by_ids(self, source_id_arr):
        """Get wof source from odm2 database by a list of Affiliation ID's.

        :param source_id_arr: List of Affiliation ID.
        :return: List WOF Source
        """
        self.db_check()
        aff = self.db_session.query(odm2_models.Affiliations). \
            filter(odm2_models.Affiliations.AffiliationID.in_(source_id_arr)).all()
        aff_arr = []
        for i in range(len(aff)):
            w_a = model.Source(aff[i])
            aff_arr.append(w_a)
        return aff_arr

    def get_qualcontrollvl_by_id(self, qual_control_lvl_id):
        """Get wof Quality Control Level from odm2 database by Processing Level ID.

        :param qual_control_lvl_id: Processing Level ID.
        :return: A WOF Quality Control Level
        """
        self.db_check()
        pl = self.db_session.query(odm2_models.ProcessingLevels) \
            .filter(odm2_models.ProcessingLevels.ProcessingLevelID == qual_control_lvl_id).first()
        w_pl = model.QualityControlLevel(pl)
        return w_pl

    def get_qualcontrollvls_by_ids(self, qual_control_lvl_id_arr):
        """Get wof Quality Control Level from odm2 database by a list of Processing Level ID's.

        :param qual_control_lvl_id_arr: List Processing Level ID.
        :return: List of WOF Quality Control Level
        """
        self.db_check()
        pl = self.db_session.query(odm2_models.ProcessingLevels) \
            .filter(odm2_models.ProcessingLevels.ProcessingLevelID.in_(qual_control_lvl_id_arr)).all()
        pl_arr = []
        for i in range(len(pl)):
            w_pl = model.QualityControlLevel(pl[i])
            pl_arr.append(w_pl)
        return pl_arr

def _get_msrv_enddatetimes(db_session, resultids):
    """Extracts Latest DateTime from Timeseries Result Values.

    :param db_session: SQLAlchemy Session Object
    :param resultids: List of result id. Ex. [1, 2, 3]
    :return: Dictionary of End Date Time
    """
    edt_dict = dict(db_session.query(odm2_models.MeasurementResultValues.ResultID,
                                           func.max(
                                               odm2_models.MeasurementResultValues.ValueDateTime)).filter(  # noqa
    odm2_models.MeasurementResultValues.ResultID.in_(resultids)). \
                     group_by(odm2_models.MeasurementResultValues.ResultID).all())
    #edt_dict['timezone'] =db_session.query(odm2_models.MeasurementResultValues.ResultID,
    #                                        func.max(
    #                                           odm2_models.MeasurementResultValues.ValueDateTime)).filter(  # noqa
    # odm2_models.MeasurementResultValues.ResultID.in_(resultids)). \
     #                 group_by(odm2_models.MeasurementResultValues.ResultID).all()

    return edt_dict


def _get_msrv_startdatetimes(db_session, resultids):
    """Extracts Latest DateTime from Timeseries Result Values.

    :param db_session: SQLAlchemy Session Object
    :param resultids: List of result id. Ex. [1, 2, 3]
:    return: Dictionary of End Date Time
     """
    sdt_dict = dict(db_session.query(odm2_models.MeasurementResultValues.ResultID,
                                           func.min(
                                               odm2_models.MeasurementResultValues.ValueDateTime)).filter(  # noqas
    odm2_models.MeasurementResultValues.ResultID.in_(resultids)). \
                     group_by(odm2_models.MeasurementResultValues.ResultID).all())
    return sdt_dict
