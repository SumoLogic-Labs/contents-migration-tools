from __future__ import unicode_literals

import pprint
import re
import string
from typing import List

from textx import *

from SFX.SFXParsingRules import *


class SFXTranslator():

    def __init__(self) -> None:
        self.mm = metamodel_from_file('SFX/SignalFX.tx', \
            classes=[Filter,FilterFunction,DataFunction,Expression, Term, \
                     Factor,SFXExpression,SingleInput,SFXFunction,SFXId,\
                      FilteringQuery, PlainTextQuery, UnSupportedSFXFunctions, RefrencedQuery],use_regexp_group=True)
        
    def cname(self,o):
        return o.__class__.__name__
    

    def get_stream_variables(self,obj, stream_vars:List):
        if obj and hasattr(obj, 'stream_variable_name') and obj.stream_variable_name:
            stream_vars.append(obj.stream_variable_name)
        elif obj and hasattr(obj, 'factors') and obj.factors:
            for factor in obj.factors:
                self.get_stream_variables(factor,stream_vars)
        elif obj and hasattr(obj, 'terms') and obj.terms:
            for term in obj.terms:
                self.get_stream_variables(term,stream_vars)
        elif obj and hasattr(obj, 'expr') and obj.expr:
            self.get_stream_variables(obj.expr,stream_vars)
        else:
            return


    def translate(self, current_logger, model_str, sumoLogicFiltersInjections:List, query_duration='1h'):
        SumoLogicDashboardDicts.reset_stream_var_labels_lookup()
        sumo_queries = {}
        model_str1 = re.sub("\\n+", r"\n", model_str)
        model_str2 = re.sub("\s*(?:\\n|\n)+\s*(and|not|filter|\()", r" \1", model_str1)
        model_strs = model_str2.split('\n')
        refrenced_stream_variables={}
        sfx_inputs=[]
        
        for mdl_str in model_strs:
            if not mdl_str:
                continue
            model = self.mm.model_from_str(mdl_str.strip())
            sfx_inputs += model.inputs
            
        for single_input in sfx_inputs:
            input_type_name = self.cname(single_input.input_type)
            stream_result_variable_name = ''
            if single_input and single_input.input_type:
                publish_options = single_input.get_publish_options()
                stream_result_variable_name = single_input.input_type.stream_result_variable_name
                if not stream_result_variable_name:
                    stream_result_variable_name = publish_options['label']
                    stream_result_variable_name = SumoLogicDashboardDicts.get_stream_var_labels_mapping(stream_result_variable_name, generate_key_if_none=True) if stream_result_variable_name not in SumoLogicDashboardDicts.stream_var_labels_mapping.keys() else stream_result_variable_name
                else:
                    stream_result_variable_name = stream_result_variable_name.name
                
                refrenced_stream_variables[stream_result_variable_name]=[]
                main_query = None
                expression_labels = None

                if isinstance(single_input.input_type.get_sumo_query(sumoLogicFiltersInjections=sumoLogicFiltersInjections), tuple):
                    main_query = single_input.input_type.get_sumo_query(sumoLogicFiltersInjections=sumoLogicFiltersInjections)[0]
                    if main_query and len(main_query) > 2750:
                        main_query = single_input.input_type.get_sumo_query( generate_fuzzy_filters=False, sumoLogicFiltersInjections=sumoLogicFiltersInjections)
                    hashed_expression_labels = single_input.input_type.get_sumo_query(sumoLogicFiltersInjections=sumoLogicFiltersInjections)[1]
                    expression_labels = [hashed_expression_label.replace("#","") for hashed_expression_label in hashed_expression_labels if hashed_expression_label]
                else:
                    main_query = single_input.input_type.get_sumo_query(sumoLogicFiltersInjections=sumoLogicFiltersInjections)
                    if main_query and len(main_query) > 2750:
                        main_query = single_input.input_type.get_sumo_query( generate_fuzzy_filters=False, sumoLogicFiltersInjections = sumoLogicFiltersInjections)


                query_tail = single_input.get_query_tail(query_duration=query_duration)
                final_query = f"{main_query} {query_tail}"
                sumo_queries[stream_result_variable_name] = {"expression_labels": expression_labels,"input_type_name": input_type_name, "query": final_query, "bys": single_input.get_grp_bys(), "publish": publish_options, "aggregationType": single_input.get_aggregation_type()}
            
            if single_input.input_type and self.cname(single_input.input_type)=="sfx_expression":
                for expr in single_input.input_type.exprs:
                    self.get_stream_variables(expr,refrenced_stream_variables[stream_result_variable_name])
        
        for stream_result_variable_name, sumo_query in sumo_queries.items():
            current_group_bys = []
            if sumo_query["expression_labels"]:
                for label in sumo_query["expression_labels"]:
                    if sumo_queries[label]["bys"]:
                        current_group_bys.append(sumo_queries[label]["bys"])
                    if "along" in sumo_queries[label]["query"]:
                        along_value_search = re.search('along\s(?P<along_value>\S+)', sumo_queries[label]["query"])
                        if along_value_search:
                            along_value = along_value_search.group('along_value')
                            if along_value:
                                current_group_bys.append(along_value)

                current_group_bys = flatten(current_group_bys)
                current_group_bys = list(set(current_group_bys))
                current_group_bys_stmt = f" along {','.join(current_group_bys)} " if current_group_bys and len(current_group_bys) > 0 else ""
                
                sumo_queries[stream_result_variable_name]["query"]= sumo_query["query"].replace("___ALONG___BYS___", f"{current_group_bys_stmt}")


        return sumo_queries
        
                