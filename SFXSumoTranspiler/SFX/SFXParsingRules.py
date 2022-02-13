import pytimeparse2
from typing import List
from SFX.Utils import *


class Factor(object):
    def __init__(self, parent, items:List) -> None:
        super().__init__()
        self.parent = parent
        self.items = items

    def get_factor(self):
        result =  self.get_factor_items(self.items, {"items":[], "labels":[]})
        return result["items"], result["labels"]

    def get_factor_items(self, items, result):
        if isinstance(items, list):
            for item in items:
                if isinstance(item, list):
                    if len(item) > 0:
                        self.get_factor_items(items, result)
                elif item:
                    if isinstance(item, Expression):
                        result["items"].append(item.get_expression()[0])
                        result["labels"].append(item.get_expression()[1])
                    elif isinstance(item, SFXId):
                        result["items"].append(SumoLogicDashboardDicts.get_hashed_stream_var_labels_mapping(item))
                        result["labels"].append(SumoLogicDashboardDicts.get_hashed_stream_var_labels_mapping(item))
                    else:
                        result["items"].append(item)
        elif items:
            if isinstance(items, Expression):
                result["items"].append(items.get_expression()[0])
                result["labels"].append(items.get_expression()[1])
            elif isinstance(items, SFXId):
                        result["items"].append(SumoLogicDashboardDicts.get_hashed_stream_var_labels_mapping(items))
                        result["labels"].append(SumoLogicDashboardDicts.get_hashed_stream_var_labels_mapping(items))
            else:
                result["items"].append(items)
        return result

class Term(object):
    def __init__(self, parent, items:List) -> None:
        super().__init__()
        self.parent = parent
        self.items = items

    def get_term(self):
        result = self.get_term_items(self.items, {"items":[], "labels":[]})
        return result["items"], result["labels"]

    def get_term_items(self, items, result):
        if isinstance(items, list):
            for item in items:
                if isinstance(item, list):
                    if len(item) > 0:
                        self.get_term_items(items, result)
                elif item:
                    if isinstance(item, Factor):
                        result["items"].append(item.get_factor()[0])
                        result["labels"].append(item.get_factor()[1])
                    else:
                        result["items"].append(item)
        elif items:
            if isinstance(items, Factor):
                result["items"].append(items.get_factor()[0])
                result["labels"].append(items.get_factor()[1])
            else:
                result["items"].append(items)
        return result

class Expression(object):
    def __init__(self, parent, items:List) -> None:
        super().__init__()
        self.parent = parent
        self.items = items

    def get_expression(self):
        result_all= self.get_expression_items(self.items, {"items":[], "labels":[]})
        result_items = flatten(result_all["items"])
        result_labels = flatten(result_all["labels"])
        return "".join(result_items), result_labels

    def get_expression_items(self, items, result):
        if isinstance(items, list):
            for item in items:
                if isinstance(item, list):
                    if len(item) > 0:
                        self.get_expression_items(items, result)
                elif item:
                    if isinstance(item, Term):
                        result["items"].append(item.get_term()[0])
                        result["labels"].append(item.get_term()[1])
                    else:
                        result["items"].append(item)
        elif items:
            if isinstance(items, Term):
                result["items"].append(items.get_term()[0])
                result["labels"].append(items.get_term()[1])            
            else:
                result["items"].append(items)
        return result

class FilterFunction(object):
    def __init__(self, parent, filter_key, queries:List, match_missing=False):
        self.parent = parent
        if filter_key:
            if isinstance(filter_key, SFXId):
                filter_key.name = SumoLogicDashboardDicts.get_hashed_stream_var_labels_mapping(filter_key)
            else:
                filter_key = SumoLogicDashboardDicts.get_stream_var_labels_mapping(filter_key)

        self.filter_key = filter_key
        self.queries = queries

class Filter(object):
    def __init__(self, parent, filter_funcs:List,filters:List, \
        and_kw=False, not_kw=False, or_kw=False):
        self.parent = parent
        self.filter_funcs = filter_funcs
        self.filters = filters
        self.and_kw = and_kw
        self.or_kw = or_kw
        self.not_kw = not_kw

class SingleInput(object):
    def __init__(self,parent,input_type,method:List) -> None:
        super().__init__()
        self.parent = parent
        self.input_type = input_type
        self.method = method

    def get_query_tail(self, query_duration='1d'):
        query_tail = ""
        sumo_method_mappings = \
            {"average":"avg", "count":"count", "max":"max", "min":"min", \
                "sum":"sum", "mean":"avg", "timeshift": "timeshift", \
                "scale":"eval","percentile":"pct", "above":"filter", "below":"filter", 
                "between":"filter","bottom":"filter","rateofchange":"rate",\
                "mean_plus_stddev":"outlier","fill":"fillmissing", "ceil":"eval", "bottom":"bottomk","top":"topk"
            }
        if isinstance(self.method, list):
            for mthd in self.method:
                current_method_name = ""
                if isinstance(mthd.name, SFXId):
                    current_method_name = mthd.name.name
                else:
                    current_method_name = mthd.name

                if not current_method_name in sumo_method_mappings.keys():
                    continue

                sumo_mth_name = sumo_method_mappings[current_method_name]
                sfx_mthd_name = "aggregation" if current_method_name in ("average", "count", "min", "max", "mean", "sum") else current_method_name
                keyvals_list = mthd.keyvals if "keyvals" in mthd.__dict__.keys() and mthd.keyvals else []
                keyvals = {}

                for keyval in keyvals_list:
                    current_val = None
                    if keyval.vals and isinstance(keyval.vals, list):
                        if len(keyval.vals) == 1:
                            current_val = keyval.vals[0]
                        elif len(keyval.vals) > 1:
                            current_val = ','.join(keyval.vals)
                    key = keyval.key.name if keyval.key and isinstance(keyval.key, SFXId) else keyval.key
                    keyvals[key] = current_val

                sfx_to_sumo_stmt_mappings = self.get_sfx_to_sumo_mappings(query_duration, mthd, sumo_mth_name, sfx_mthd_name, keyvals)
                query_tail += sfx_to_sumo_stmt_mappings[sfx_mthd_name]
        return query_tail

    def get_sfx_to_sumo_mappings(self, query_duration, mthd, sumo_mth_name, sfx_mthd_name, keyvals):
        sfx_to_sumo_stmt_mappings = \
            {
                        "above":        \
                                        f" | {sumo_mth_name} _value > {mthd.limit} all any {query_duration}" \
                                    if sfx_mthd_name=="above" and has_key(mthd,'limit') \
                                        else \
                                            '',
                        "bottom":        \
                                        f" | {sumo_mth_name}({keyvals.get('count')}, latest) by {keyvals.get('by')}" \
                                    if sfx_mthd_name=="bottom" and keyvals.get('count') and keyvals.get('by') \
                                        else \
                                        f" | {sumo_mth_name}({keyvals.get('count')}, latest)" \
                                    if sfx_mthd_name=="bottom" and keyvals.get('count') and not keyvals.get('by')\
                                        else \
                                            '',
                        "top":        \
                                        f" | {sumo_mth_name}({keyvals.get('count')}, latest) by {keyvals.get('by')}" \
                                    if sfx_mthd_name=="top" and keyvals.get('count') and keyvals.get('by') \
                                        else \
                                        f" | {sumo_mth_name}({keyvals.get('count')}, latest)" \
                                    if sfx_mthd_name=="top" and keyvals.get('count') and not keyvals.get('by') \
                                        else \
                                            ''
                                            ,
                        "scale":        \
                                        f" | {sumo_mth_name} _value * {mthd.value}" \
                                    if sfx_mthd_name=="scale" and has_key(mthd,'value') \
                                        else \
                                            '',
                        "ceil":        \
                                        f" | {sumo_mth_name} ceil" \
                                    if sfx_mthd_name=="ceil" \
                                        else \
                                            '',
                        "rateofchange":        \
                                        f" | {sumo_mth_name}" \
                                    if sfx_mthd_name=="rateofchange" \
                                        else \
                                            '',
                        "timeshift":        \
                                        f" | {sumo_mth_name} {str(pytimeparse2.parse(mthd.value))}s" \
                                    if sfx_mthd_name=="timeshift" and has_key(mthd,'value') \
                                        else \
                                            '',
                        "fill":        \
                                        f" | {sumo_mth_name} {mthd.value}" \
                                    if sfx_mthd_name=="fill" and has_key(mthd,'value') \
                                        else \
                                            '',
                        "mean_plus_stddev":        \
                                        f" | {sumo_mth_name} window={keyvals.get('over','1d')} threshold={keyvals.get('stddevs','1')}" \
                                    if sfx_mthd_name=="mean_plus_stddev" and (keyvals.get('over') or keyvals.get('stddevs')) \
                                        else \
                                            '',
                        "below":        
                                        \
                                        f" | {sumo_mth_name} _value < {mthd.limit} all any {query_duration}" \
                                    if sfx_mthd_name=="below" and has_key(mthd,'limit') \
                                        else \
                                            '',
                        "between":      \
                                        f" | {sumo_mth_name} _value > {mthd.low_limit} and _value < {mthd.high_limit}  all any {query_duration}" \
                                    if sfx_mthd_name=="between" and has_key(mthd,'low_limit') and has_key(mthd,'high_limit') \
                                        else \
                                            '',
                        "percentile":          
                                        \
                                        f" | {sumo_mth_name}({keyvals.get('pct')}) by {keyvals.get('by')}" \
                                        if sfx_mthd_name=="percentile" and ((keyvals.get('pct') or has_key(mthd, 'pct')) and keyvals.get('by')) \
                                        else \
                                            f" | {sumo_mth_name}({keyvals.get('pct', mthd.pct)})" \
                                        if sfx_mthd_name=="percentile" and ((keyvals.get('pct') or has_key(mthd, 'pct')) and not keyvals.get('by')) \
                                        else \
                                            f" | {sumo_mth_name}({keyvals.get('pct', mthd.pct)}) | quantize to {keyvals.get('over')}" \
                                        if sfx_mthd_name=="percentile" and ((keyvals.get('pct') or has_key(mthd, 'pct')) and keyvals.get('over') and not keyvals.get('by')) \
                                        else \
                                            '',
                        "aggregation":  \
                                         f" | quantize to {keyvals.get('over')} using {sumo_mth_name}" \
                                        if sfx_mthd_name=="aggregation" and keyvals.get('over')\
                                        else \
                                                f" | {sumo_mth_name} by {keyvals.get('by')}" 
                                        if keyvals.get('by') and sfx_mthd_name=="aggregation" \
                                        else \
                                                f" | {sumo_mth_name}" 
                                        if sfx_mthd_name=="aggregation" and not (keyvals.get('over') or keyvals.get('by')) \
                                        else \
                                            '',
                    }
            
        return sfx_to_sumo_stmt_mappings

    def get_grp_bys(self):
        grp_bys = []
        for mthd in self.method:           
            if "keyvals" in mthd.__dict__.keys() and mthd.keyvals:
                if mthd.keyvals:
                    grp_bys_set = set(flatten( [kv.vals for kv in mthd.keyvals if kv.key and ((isinstance(kv.key, SFXId) and kv.key.name=="by") or (isinstance(kv.key, str) and kv.key=="by")) and len(kv.vals)>0] ))
                    grp_bys += list(grp_bys_set)
        grp_bys_unique = set(grp_bys)
        return list(grp_bys_unique)

    def get_publish_options(self):
        publish_options = {"label":"","enable":True}
        for mthd in self.method: 
            if isinstance(mthd.name, SFXId):
                mthd.name = mthd.name
            if mthd.name == "publish" and "keyvals" in mthd.__dict__.keys() and mthd.keyvals:
                for kv in mthd.keyvals:
                    for v in kv.vals:
                        if kv.key=="label":
                            adjusted_label = SumoLogicDashboardDicts.get_stream_var_labels_mapping(v)
                            publish_options['label'] = adjusted_label if adjusted_label else publish_options['label']

                        if kv.key=="enable" and v == 'False':
                            publish_options['enable'] = False
                        else:
                            publish_options['enable'] = True
        return publish_options

    def get_sumo_query(self, generate_fuzzy_filters=True, sumoLogicFiltersInjections:List=[]):
        pass
    
    def get_aggregation_type(self):
        pass

class SFXFunction(SingleInput):
    def __init__(self, parent, stream_result_variable_name, name, params: List) -> None:
        self.parent = parent
        if stream_result_variable_name:
             stream_result_variable_name.name = SumoLogicDashboardDicts.get_stream_var_labels_mapping(stream_result_variable_name)
        self.stream_result_variable_name = stream_result_variable_name
        self.name = name
        self.params = params
    def get_sumo_query(self, generate_fuzzy_filters=True, sumoLogicFiltersInjections:List=[]):
        if self.name == 'min':
            modified_params = []
            for param in self.params:
                if isinstance(param.param, SFXId):
                    modified_params.append(SumoLogicDashboardDicts.get_stream_var_labels_mapping(param.param))
                elif isinstance(param.param, Expression):
                    modified_params.append(param.param.get_expression()[0])
                else:
                    modified_params.append(param.param)
            params_to_pass = ','.join(modified_params)

            return f"{self.name}({params_to_pass})".replace("'", "\"") if self.name and params_to_pass else ""

class FilteringQuery(SingleInput):
    def __init__(self,parent,stream_result_variable_name,stream_query_to_filter, filters:List) -> None:
        self.parent=parent
        if stream_result_variable_name:
             stream_result_variable_name.name = SumoLogicDashboardDicts.get_stream_var_labels_mapping(stream_result_variable_name)
        self.stream_result_variable_name = stream_result_variable_name
        self.filters=filters
        if stream_query_to_filter:
             stream_query_to_filter.name = SumoLogicDashboardDicts.get_hashed_stream_var_labels_mapping(stream_query_to_filter)
        self.stream_query_to_filter = stream_query_to_filter

    def get_sumo_query(self, generate_fuzzy_filters=True, sumoLogicFiltersInjections:List=[]):
        initial_scope = f"{self.stream_query_to_filter}" if self.stream_query_to_filter else ""
        sumo_query = get_sumo_query_scope(initial_scope, self.filters, generate_fuzzy_filters, sumoLogicFiltersInjections)
        return f"{sumo_query}".replace("'", "\"")

class RefrencedQuery(SingleInput):
    def __init__(self,parent,stream_result_variable_name,refrenced_stream_query) -> None:
        self.parent = parent
        if stream_result_variable_name:
             stream_result_variable_name.name = SumoLogicDashboardDicts.get_stream_var_labels_mapping(stream_result_variable_name)
        self.stream_result_variable_name = stream_result_variable_name
        if refrenced_stream_query:
            refrenced_stream_query = SumoLogicDashboardDicts.get_stream_var_labels_mapping(refrenced_stream_query)
   
    def get_sumo_query(self, generate_fuzzy_filters=True, sumoLogicFiltersInjections:List=[]):
        return f"#{self.refrenced_stream_query.name}".replace("'", "\"") if self.refrenced_stream_query else ""

class PlainTextQuery(SingleInput):
    def __init__(self,parent,stream_result_variable_name,metric_name) -> None:
        self.parent = parent
        if stream_result_variable_name:
             stream_result_variable_name.name = SumoLogicDashboardDicts.get_stream_var_labels_mapping(stream_result_variable_name)
        self.stream_result_variable_name = stream_result_variable_name
        self.metric_name = metric_name
   
    def get_sumo_query(self, generate_fuzzy_filters=True, sumoLogicFiltersInjections:List=[]):
        return f"metric = {self.metric_name}".replace("'", "\"") if self.metric_name else ""
        
class UnSupportedSFXFunctions(SingleInput):
    def __init__(self,parent,stream_result_variable_name,name) -> None:
        self.parent = parent
        if stream_result_variable_name:
             stream_result_variable_name.name = SumoLogicDashboardDicts.get_stream_var_labels_mapping(stream_result_variable_name)
        self.stream_result_variable_name = stream_result_variable_name
        self.name = name

    def get_sumo_query(self, generate_fuzzy_filters=True, sumoLogicFiltersInjections:List=[]):
        return f"//\"SignalFX Function ({self.name}) is not currently supported by Sumo\""

class SFXExpression(SingleInput):
    def __init__(self,parent,stream_result_variable_name,expr) -> None:
        self.parent = parent
        if stream_result_variable_name:
             stream_result_variable_name.name = SumoLogicDashboardDicts.get_stream_var_labels_mapping(stream_result_variable_name)
        self.stream_result_variable_name = stream_result_variable_name
        self.expr = expr

    def get_sumo_query(self, generate_fuzzy_filters=True, sumoLogicFiltersInjections:List=[]):
        exprs, labels = self.expr.get_expression()
        if labels and len(labels) > 0:
            labels = list(set(labels))

        along_ph = ""
        if exprs and not check_number(exprs):
            along_ph = "___ALONG___BYS___"
        return f"{exprs}{along_ph}".replace("'", "\"") if self.expr else "",labels

class DataFunction(SingleInput):
    def __init__(self,parent,stream_result_variable_name,metric_name,filters:List,rollup_type,extrapolation,maxex) -> None:
        self.parent=parent
        self.stream_result_variable_name = stream_result_variable_name
        if stream_result_variable_name:
             stream_result_variable_name.name = SumoLogicDashboardDicts.get_stream_var_labels_mapping(stream_result_variable_name)
        self.stream_result_variable_name = stream_result_variable_name
        self.metric_name=metric_name.name if isinstance(metric_name, SFXId) else metric_name
        self.filters=filters
        self.rollup_type=rollup_type
        self.extrapolation=extrapolation
        self.maxex=maxex

        # self.lableld_filters = lableld_filters
    def get_rollup_type(self):
        if not self.rollup_type:
            return ""
        rollup_types_mappings = {"delta":"delta","rate":"rate increasing", "lag":None}
        sumo_rollup = ""
        sumo_rollup_op= rollup_types_mappings.get(self.rollup_type)
        if sumo_rollup_op:
            sumo_rollup = f" | {sumo_rollup_op} "
        return sumo_rollup

    def get_aggregation_type(self):
        if not self.rollup_type:
            return "latest"
        rollup_types_mappings = {"average":"avg", "count":"count", "latest":"latest", "max":"max", "min":"min", "sum":"sum"}
        aggregation_type_setting= rollup_types_mappings.get(self.rollup_type)
        return aggregation_type_setting if aggregation_type_setting else "latest"
     
    def get_sumo_query(self, generate_fuzzy_filters=True, sumoLogicFiltersInjections:List=[]):
        initial_scope = f"metric={SumoLogicDashboardDicts.get_adjusted_metric_name(self.metric_name)}"
        sumo_query = get_sumo_query_scope(initial_scope, self.filters, generate_fuzzy_filters, sumoLogicFiltersInjections)
        sumo_query += self.get_rollup_type()
        return f"{sumo_query}".replace("'", "\"")
