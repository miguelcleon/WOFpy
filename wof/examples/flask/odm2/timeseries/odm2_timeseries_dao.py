# -*- coding: utf-8 -*-
"""Data Access Objects (DAO) used to retrieve Data from ODM2 Database."""
from __future__ import (absolute_import, division, print_function)

from dateutil.parser import parse
import os
import yaml

from sqlalchemy import (create_engine, func)
from sqlalchemy.orm import (scoped_session, sessionmaker)

from odm2api.ODM2 import models as odm2_models

import wof.examples.flask.odm2.timeseries.sqlalch_odm2_models as model
from wof.dao import BaseDao


class Odm2Dao(BaseDao):
    """Odm2Dao Object for use with WOFpy Server."""
    def __init__(self, db_connection_string):
        """Initialize Odm2Dao Object."""

        # Default application pool size is 5. Use 100 to improve performance.
        self.engine = create_engine(
            db_connection_string,
            convert_unicode=True,
            # pool_size=100
        )

        odm2_models.setSchema(self.engine)

        Session = scoped_session(
            sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )
        )

        self.db_session = Session()

        # Read in WaterML -> ODM2 CV Mapping
        with open(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'cvmap_wml_1_1.yml'))) as yml:
            self.yml_dict = yaml.load(yml)

    def __del__(self):
        """Closes Database Session."""
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
            join(odm2_models.TimeSeriesResults). \
            filter(odm2_models.FeatureActions.SamplingFeatureID == odm2_models.Sites.SamplingFeatureID,
                   odm2_models.TimeSeriesResults.FeatureActionID == odm2_models.FeatureActions.FeatureActionID).distinct()  # noqa

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
            join(odm2_models.TimeSeriesResults). \
            filter(odm2_models.FeatureActions.SamplingFeatureID == odm2_models.Sites.SamplingFeatureID,
                   odm2_models.TimeSeriesResults.FeatureActionID == odm2_models.FeatureActions.FeatureActionID,  # noqa
                   odm2_models.Sites.Latitude >= south,
                   odm2_models.Sites.Latitude <= north,
                   odm2_models.Sites.Longitude >= west,
                   odm2_models.Sites.Longitude <= east).distinct()
        s_Arr = []
        for s_r in s_rArr:
            s = model.Site(s_r)
            s_Arr.append(s)
        return s_Arr

    def get_variables_from_results(self, var_codes=None):
        """Get wof variables from odm2 database by a list of variable codes.

        :param var_codes: List of Variable Codes Ex. ['TEMP', 'SAL']
        :return: List of WOF Variables
        """
        self.db_check()
        # TODO: Need to refine this function; currently seeking database.
        l_var_codes = None
        if var_codes is not None:
            if not isinstance(var_codes, list):
                l_var_codes = [var_codes]
            else:
                l_var_codes = var_codes

        r_t_Arr = []
        if l_var_codes is None:
            if self.engine.name in ('postgresql', 'mssql'):
                r_t = self.db_session.query(odm2_models.TimeSeriesResultValues). \
                    join(odm2_models.TimeSeriesResults). \
                    distinct(odm2_models.TimeSeriesResults.VariableID).all()
            else:
                r_t = self.db_session.query(odm2_models.TimeSeriesResultValues). \
                    join(odm2_models.TimeSeriesResults). \
                    group_by(odm2_models.TimeSeriesResults.VariableID).all()
            r_t_Arr.append(r_t)
        else:
            for item in l_var_codes:
                if self.engine.name in ('postgresql', 'mssql'):
                    r_t = self.db_session.query(odm2_models.TimeSeriesResultValues). \
                        join(odm2_models.TimeSeriesResults). \
                        join(odm2_models.Variables). \
                        filter(
                        odm2_models.Variables.VariableID == odm2_models.TimeSeriesResults.VariableID,
                        odm2_models.Variables.VariableCode == item). \
                        distinct(odm2_models.Variables.VariableID).all()
                else:
                    r_t = self.db_session.query(odm2_models.TimeSeriesResultValues). \
                        join(odm2_models.TimeSeriesResults). \
                        join(odm2_models.Variables). \
                        filter(
                        odm2_models.Variables.VariableID == odm2_models.TimeSeriesResults.VariableID,
                        odm2_models.Variables.VariableCode == item). \
                        group_by(odm2_models.Variables.VariableID).all()
                r_t_Arr.append(r_t)
        v_arr = []
        if len(r_t_Arr) is not 0:
            for result_t in r_t_Arr:
                for result in result_t:
                    v = result.ResultObj.VariableObj
                    u = result.ResultObj.UnitsObj
                    s = result.ResultObj.SampledMediumCV
                    t = result.TimeAggregationIntervalUnitsObj
                    ti = result.TimeAggregationInterval
                    ag = result.AggregationStatisticCV
                    at = result.ResultObj.FeatureActionObj.ActionObj.ActionTypeCV
                    w_v = model.Variable(v, s, u, t, ti, ag, at)
                    w_v.DataType = self.get_match('datatype', w_v.DataType)
                    w_v.SampleMedium = self.get_match('samplemedium', w_v.SampleMedium)
                    v_arr.append(w_v)

        return v_arr

    def get_all_variables(self):
        """Get wof variables from odm2 database.

        :return: List of WOF Variables
        """
        self.db_check()
        v_arr = self.get_variables_from_results()
        return v_arr

    def get_variable_by_code(self, var_code):
        """Get wof variables from odm2 database by a variable code.

        :param var_code: Variable Codes Ex. 'TEMP'
        :return: WOF Variable
        """
        self.db_check()
        w_v = None
        v_arr = self.get_variables_from_results(var_code)
        if len(v_arr) is not 0:
            w_v = v_arr.pop()
        return w_v

    def get_variables_by_codes(self, var_codes_arr):
        """Get wof variables from odm2 database by a list of variable codes.

        :param var_codes_arr: List of Variable Codes Ex. ['TEMP', 'SAL']
        :return: List of WOF Variables
        """
        self.db_check()
        v_arr = self.get_variables_from_results(var_codes_arr)
        return v_arr

    def get_series_by_sitecode(self, site_code):
        """Get wof series from odm2 database by a site code.

        :param site_code: Site Code Ex. 'USU-LBR-Mendon'
        :return: List of WOF Series
        """
        self.db_check()
        r = self.db_session.query(odm2_models.TimeSeriesResults). \
            join(odm2_models.FeatureActions). \
            join(odm2_models.SamplingFeatures). \
            filter(
            odm2_models.TimeSeriesResults.FeatureActionID == odm2_models.FeatureActions.FeatureActionID,
            odm2_models.SamplingFeatures.SamplingFeatureCode == site_code). \
            group_by(odm2_models.TimeSeriesResults.VariableID,
                     odm2_models.TimeSeriesResults.ResultID,
                     odm2_models.Results.ResultID).all()

        ids = [i.ResultID for i in r]

        edt_dict = _get_tsrv_enddatetimes(self.db_session, ids)

        r_arr = []
        aff = None
        for i in range(len(r)):
            if i == 0:
                aff = self.db_session.query(odm2_models.Affiliations). \
                    join(odm2_models.ActionBy). \
                    filter(odm2_models.ActionBy.ActionID == r[i].FeatureActionObj.ActionID).first()  # noqa

            r[i].tsrv_EndDateTime = edt_dict[r[i].ResultID]
            tsrv = self.db_session.query(odm2_models.TimeSeriesResultValues). \
                filter(odm2_models.TimeSeriesResults.ResultID == r[i].ResultID).first()
            w_r = model.Series(r[i], aff, tsrv)
            w_r.Variable.DataType = self.get_match('datatype', w_r.Variable.DataType)
            w_r.Variable.SampleMedium = self.get_match('samplemedium', w_r.Variable.SampleMedium)
            w_r.SampleMedium = w_r.Variable.SampleMedium
            r_arr.append(w_r)
        return r_arr

    def get_series_by_sitecode_and_varcode(self, site_code, var_code):
        """Get wof series from odm2 database by a site code and a variable code.

        :param site_code: Site Code Ex. 'USU-LBR-Mendon'
        :param var_code: Variable Code Ex. 'TEMP'
        :return: List of WOF Series
        """
        self.db_check()
        r = self.db_session.query(odm2_models.TimeSeriesResults). \
            join(odm2_models.FeatureActions). \
            join(odm2_models.SamplingFeatures). \
            join(odm2_models.Variables). \
            filter(
            odm2_models.TimeSeriesResults.FeatureActionID == odm2_models.FeatureActions.FeatureActionID,
            odm2_models.TimeSeriesResults.VariableID == odm2_models.Variables.VariableID,
            odm2_models.SamplingFeatures.SamplingFeatureCode == site_code,
            odm2_models.Variables.VariableCode == var_code). \
            group_by(odm2_models.TimeSeriesResults.VariableID,
                     odm2_models.TimeSeriesResults.ResultID,
                     odm2_models.Results.ResultID).all()

        ids = [i.ResultID for i in r]

        edt_dict = _get_tsrv_enddatetimes(self.db_session, ids)

        r_arr = []
        aff = None
        for i in range(len(r)):
            if i is 0:
                aff = self.db_session.query(odm2_models.Affiliations). \
                    join(odm2_models.ActionBy). \
                    filter(odm2_models.ActionBy.ActionID == r[i].FeatureActionObj.ActionID).first()

            r[i].tsrv_EndDateTime = edt_dict[r[i].ResultID]
            tsrv = self.db_session.query(odm2_models.TimeSeriesResultValues). \
                filter(odm2_models.TimeSeriesResults.ResultID == r[i].ResultID).first()
            w_r = model.Series(r[i], aff, tsrv)
            w_r.Variable.DataType = self.get_match('datatype', w_r.Variable.DataType)
            w_r.Variable.SampleMedium = self.get_match('samplemedium', w_r.Variable.SampleMedium)
            w_r.SampleMedium = w_r.Variable.SampleMedium
            r_arr.append(w_r)
        return r_arr

    def get_datavalues(self, site_code, var_code,
                       begin_date_time=None, end_date_time=None):
        """Get wof datavalues from odm2 database by site code,
        variable code, start datetime, and end datetime.

        :param site_code: Site Code Ex. 'USU-LBR-Mendon'
        :param var_code: Variable Code Ex. 'TEMP'
        :param begin_date_time: Start Time Ex. '2007-08-16 23:30:00.000'
        :param end_date_time: End Time Ex. '2008-03-27 19:30:00.000'
        :return: List of WOF DataValue
        """
        self.db_check()
        if not begin_date_time or not end_date_time:
            try:
                valueResultArr = self.db_session.query(odm2_models.TimeSeriesResultValues). \
                    join(odm2_models.TimeSeriesResults). \
                    join(odm2_models.FeatureActions). \
                    join(odm2_models.SamplingFeatures). \
                    join(odm2_models.Variables). \
                    filter(
                    odm2_models.TimeSeriesResults.FeatureActionID == odm2_models.FeatureActions.FeatureActionID,  # noqa
                    odm2_models.TimeSeriesResults.VariableID == odm2_models.Variables.VariableID,
                    odm2_models.SamplingFeatures.SamplingFeatureCode == site_code,
                    odm2_models.Variables.VariableCode == var_code). \
                    order_by(odm2_models.TimeSeriesResultValues.ValueDateTime).all()
            except:
                valueResultArr = []
        else:
            begin_date_time = parse(begin_date_time)
            end_date_time = parse(end_date_time)
            try:
                valueResultArr = self.db_session.query(odm2_models.TimeSeriesResultValues). \
                    join(odm2_models.TimeSeriesResults). \
                    join(odm2_models.FeatureActions). \
                    join(odm2_models.SamplingFeatures). \
                    join(odm2_models.Variables). \
                    filter(
                    odm2_models.TimeSeriesResults.FeatureActionID == odm2_models.FeatureActions.FeatureActionID,  # noqa
                    odm2_models.TimeSeriesResults.VariableID == odm2_models.Variables.VariableID,
                    odm2_models.SamplingFeatures.SamplingFeatureCode == site_code,
                    odm2_models.Variables.VariableCode == var_code,
                    odm2_models.TimeSeriesResultValues.ValueDateTime >= begin_date_time,
                    odm2_models.TimeSeriesResultValues.ValueDateTime <= end_date_time). \
                    order_by(odm2_models.TimeSeriesResultValues.ValueDateTime).all()
            except:
                valueResultArr = []
        v_arr = []
        if len(valueResultArr) is not 0:
            org_id = None
            aff = None
            first_flag = True
            for valueResult in valueResultArr:
                if first_flag:
                    first_flag = False
                    act_id = valueResult.ResultObj.FeatureActionObj.ActionID
                    aff = self.db_session.query(odm2_models.Affiliations). \
                        join(odm2_models.ActionBy). \
                        filter(odm2_models.ActionBy.ActionID == act_id).first()
                w_v = model.DataValue(valueResult, aff)
                w_v.CensorCode = self.get_match('censorcode', w_v.CensorCode)
                v_arr.append(w_v)
        return v_arr

    def get_method_by_id(self, method_id):
        """Get wof method from odm2 database by Method ID.

        :param method_id: Method ID. Ex. 'METHOD1'
        :return: A WOF Method
        """
        self.db_check()
        m = self.db_session.query(odm2_models.Methods). \
            filter(odm2_models.Methods.MethodID == method_id).first()
        w_m = model.Method(m)
        return w_m

    def get_methods_by_ids(self, method_id_arr):
        """Get wof method from odm2 database by list of Method ID's.

        :param method_id_arr: List of Method ID. Ex. ['METHOD1', 'METHOD2']
        :return: List of WOF Method
        """
        self.db_check()
        m = self.db_session.query(odm2_models.Methods). \
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


def _get_tsrv_enddatetimes(db_session, resultids):
    """Extracts Latest DateTime from Timeseries Result Values.

    :param db_session: SQLAlchemy Session Object
    :param resultids: List of result id. Ex. [1, 2, 3]
    :return: Dictionary of End Date Time
    """
    edt_dict = dict(db_session.query(odm2_models.TimeSeriesResultValues.ResultID,
                                               func.max(
                                                   odm2_models.TimeSeriesResultValues.ValueDateTime)).filter(  # noqa
        odm2_models.TimeSeriesResultValues.ResultID.in_(resultids)). \
                         group_by(odm2_models.TimeSeriesResultValues.ResultID).all())

    return edt_dict
