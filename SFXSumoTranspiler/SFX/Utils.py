import logging
import os
from random import randint
import re
import string
from ast import Str
from types import SimpleNamespace

def path_escape(fpath):
    return fpath.replace('/','_')

def flatten(S):
    if S == []:
        return S
    if isinstance(S[0], list):
        return flatten(S[0]) + flatten(S[1:])
    return S[:1] +flatten(S[1:])

def rreplace(s, old, new, occurrence):
    li = s.rsplit(old, occurrence)
    return new.join(li)

def cname(o):
    return o.__class__.__name__

def has_key(o,k):
    return k in o.__dict__.keys()

def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text  # or whatever

def remove_postfix(text, postfix):
    if text.endswith(postfix):
        return text[:-len(postfix)]
    return text

def check_number(element):
    is_number=False
    partition = element.partition('.')
    if element.isnumeric():
        is_number=True

    if element.isdigit():
        is_number=True

    elif (partition[0].isdigit() and partition[1] == '.' and partition[2].isdigit()) or (partition[0] == '' and partition[1] == '.' and partition[2].isdigit()) or (partition[0].isdigit() and partition[1] == '.' and partition[2] == ''):
        is_number=True
    return is_number

def setup_logger(logger_name, log_file, level=logging.INFO):
    logging.basicConfig(force=True)
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fileHandler = logging.FileHandler(log_file, mode='w', encoding="utf-8")
    fileHandler.setFormatter(formatter)
    l.setLevel(level)
    l.addHandler(fileHandler)

def random_with_N_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)

def delete_folder_contents(folder_or_file_path):
    folder = folder_or_file_path
    if os.path.exists(folder_or_file_path):
        if os.path.isfile(folder_or_file_path):
            os.remove(folder_or_file_path)
            print(f"Deleted file: {folder_or_file_path}")
        elif os.path.isdir(folder_or_file_path):
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print('Failed to delete %s. Reason: %s' % (file_path, e))
    else:
        print(f"The file/folder {folder_or_file_path} does not exist")

def get_color_from_palette_index(paletteIndex):
    color = ""
    chart_colors_slice = [
	    ("gray", "#999999"),           # 0       
        ("blue", "#0077c2"),           # 1
        ("light_blue", "#00b9ff"),     # 2
        ("navy", "#6CA2B7"),           # 3
        ("dark_orange", "#b04600"),    # 4
        ("orange", "#f47e00"),         # 5
        ("dark_yellow", "#e5b312"),    # 6
        ("magenta", "#bd468d"),        # 7
        ("cerise", "#e9008a"),         # 8
        ("pink", "#ff8dd1"),           # 9
        ("violet", "#876ff3"),         # 10
        ("purple", "#a747ff"),         # 11
        ("gray_blue", "#ab99bc"),      # 12
        ("dark_green", "#007c1d"),     # 13
        ("green", "#05ce00"),          # 14
        ("aquamarine", "#0dba8f"),     # 15
        ("red", "#ea1849"),            # 16
        ("yellow", "#ea1849"),         # 17
        ("vivid_yellow", "#ea1849"),   # 18
        ("light_green", "#acef7f"),    # 19
        ("lime_green", "#6bd37e"),     #20
     ]

    if paletteIndex < len(chart_colors_slice):
        color = chart_colors_slice[paletteIndex][1]
    return color

def get_sumo_query_scope(initial_scope, filters, generate_fuzzy_filters, sumoLogicFiltersInjections) -> Str:
    sumo_scopes = []
    sumo_scope = ""

    if filters:
        for filter_obj in filters:
            sumo_scopes.append(processFilters([], [],filter_obj, generate_fuzzy_filters, sumoLogicFiltersInjections))
    else:
        global_sumo_filters = [f"{key} = {{{{{key}}}}}" for key in sumoLogicFiltersInjections]
        if global_sumo_filters and len(global_sumo_filters) > 0:
            global_sumo_filter_stmt =  " AND ".join(global_sumo_filters)
            sumo_scopes.append(global_sumo_filter_stmt)

    sumo_scopes_str = " ".join(sumo_scopes).replace("**","*").strip()
    sumo_scopes_str = f" AND {sumo_scopes_str}" if sumo_scopes_str else ""
    sumo_scope = initial_scope +  sumo_scopes_str
    return sumo_scope

def processFilters(sumo_filters,sumo_filters_processed,filter_obj, generate_fuzzy_filters, sumoLogicFiltersInjections):
    if has_key(filter_obj, "filters") and len(filter_obj.filters) > 0:
        for sub_filter in filter_obj.filters:
            processFilters(sumo_filters,sumo_filters_processed,sub_filter, generate_fuzzy_filters, sumoLogicFiltersInjections)

    if has_key(filter_obj, "filter_funcs") and len(filter_obj.filter_funcs) > 0:
        if sumo_filters:
            sumo_filters.append(" AND ")
        for filter_func_index, filter_func in enumerate(filter_obj.filter_funcs):
            
            current_filter = ""
            filter_key = ""

            if filter_func_index > 0:
                if filter_obj.and_kw:
                    sumo_filters.append(" AND ")
                
                if filter_obj.or_kw:
                    sumo_filters.append(" OR ")
            
            if filter_func.filter_key:
                filter_key = filter_func.filter_key
                if isinstance(filter_key, SFXId):
                    filter_key = filter_key.name
                if '#' in filter_key:
                    continue
            current_filter_global_override = f"{filter_key} = {{{{{filter_key}}}}}" if filter_key in sumoLogicFiltersInjections else None
            
            for qindex, query in enumerate(filter_func.queries):
                
                if qindex > 0:
                    current_filter += " OR "
            
                if generate_fuzzy_filters:
                    current_filter += f"{SumoLogicDashboardDicts.get_adjusted_filters_key(filter_key)}={query} OR (metric=*{filter_key}:{query}*)" 
                else:
                    current_filter += f"{SumoLogicDashboardDicts.get_adjusted_filters_key(filter_key)}={query}" 

            
            current_filter = f"({current_filter})"

            if filter_obj.not_kw:
                current_filter = f"!{current_filter}"
                current_filter = f"({current_filter_global_override} AND {current_filter})" if current_filter_global_override else current_filter
            else:
                current_filter = f"({current_filter_global_override})" if current_filter_global_override else current_filter

            sumo_filters.append(current_filter)
            sumo_filters_processed.append(filter_key)

    global_sumo_filters = [f"{key} = {{{{{key}}}}}" for key in sumoLogicFiltersInjections if key not in sumo_filters_processed]
    sumo_filters_stmt =  " ".join(sumo_filters)
    sumo_filters_stmt = f"({sumo_filters_stmt})"

        
    if global_sumo_filters:
        global_sumo_filter_stmt =  " AND ".join(global_sumo_filters)
        global_sumo_filter_stmt = f"({global_sumo_filter_stmt})"
        filters_scope = sumo_filters_stmt + " AND " +  f"({global_sumo_filter_stmt})"
        filters_scope = f"({filters_scope})"
    else:
        filters_scope = sumo_filters_stmt
    
    filters_scope = remove_prefix(filters_scope, "AND")
    filters_scope = remove_postfix(filters_scope, "AND")
    filters_scope = remove_prefix(filters_scope, "OR")
    filters_scope = remove_postfix(filters_scope, "OR")

    return filters_scope

class RecursiveNamespace(SimpleNamespace):
  @staticmethod
  def map_entry(entry):
    if isinstance(entry, dict):
      return RecursiveNamespace(**entry)

    return entry

  def __init__(self, **kwargs):
    super().__init__(**kwargs)
    for key, val in kwargs.items():
      if type(val) == dict:
        setattr(self, key, RecursiveNamespace(**val))
      elif type(val) == list:
        setattr(self, key, list(map(self.map_entry, val)))

class SFXId(object):
    def __init__(self, parent, name) -> None:
        super().__init__()
        self.parent = parent
        self.name = name

class SumoLogicDashboardDicts:

    alphabetical_stream_var_labels = []
    stream_var_labels_mapping = {}
    
    def get_stream_var_labels_mapping(key, generate_key_if_none=False):
        value = None
        if key:
            if isinstance(key, str):
                value = key
            else:
                if key._tx_fqn == 'SignalFX.SFXId':
                    value = SumoLogicDashboardDicts.stream_var_labels_mapping.get(key.name) 
                    if not value:
                        value = SumoLogicDashboardDicts.alphabetical_stream_var_labels.pop()
                        SumoLogicDashboardDicts.stream_var_labels_mapping[key.name] = value
        elif generate_key_if_none:
            value = SumoLogicDashboardDicts.alphabetical_stream_var_labels.pop()

        return value
    def get_hashed_stream_var_labels_mapping(key):
        _stream_var_labels_mapping = SumoLogicDashboardDicts.get_stream_var_labels_mapping(key)
        return f"#{_stream_var_labels_mapping}"

    def reset_stream_var_labels_lookup():
        SumoLogicDashboardDicts.alphabetical_stream_var_labels = []
        SumoLogicDashboardDicts.stream_var_labels_mapping = {}
        single_alphabets = list(string.ascii_uppercase)
        double_alphabets = [2*letter for letter in single_alphabets]
        alphabetical_stream_var_labels = single_alphabets + double_alphabets
        alphabetical_stream_var_labels.reverse()
        SumoLogicDashboardDicts.alphabetical_stream_var_labels = alphabetical_stream_var_labels

    def get_alphabetical_stream_var_labels():
        return SumoLogicDashboardDicts.alphabetical_stream_var_labels

    def get_next_stream_var_label():
        return SumoLogicDashboardDicts.alphabetical_stream_var_labels.pop()
        
    def get_adjusted_filters_key(property):
        key_to_adjust = {
            "kubernetes_namespace":"namespace",
            "kubernetes_cluster":"cluster",
            "kubernetes_name":"deployment",
            }
        adjusted_key = property
        if property in key_to_adjust.keys():
            adjusted_key = key_to_adjust[property]
        return adjusted_key

    def get_adjusted_metric_name(metric_name):
        metric_name_regex_pattern = metric_name.replace("*",".*")
        regex_pattern = r"\b(?=\w)" + metric_name_regex_pattern + r"\b(?!\w)"
        metrics_to_adjust = {
            "kubernetes.container_cpu_limit":"kube_pod_container_resource_limits resource=cpu",
            "kubernetes.container_cpu_request":"kube_pod_container_resource_requests resource=cpu",
            "kubernetes.container_ephemeral_storage_limit":"kube_pod_container_resource_limits  resource=ephemeral_storage",
            "kubernetes.container_ephemeral_storage_request":"kube_pod_container_resource_requests resource=ephemeral_storage",
            "kubernetes.container_memory_request":"kube_pod_container_resource_requests resource=memory",
            "kubernetes.container_ready":"kube_pod_container_status_ready", 
            "kubernetes.container_restart_count":"kube_pod_container_status_restarts_total", 
            "kubernetes.daemon_set.current_scheduled":"kube_daemonset_status_current_number_scheduled", 
            "kubernetes.daemon_set.desired_scheduled":"kube_daemonset_status_desired_number_scheduled", 
            "kubernetes.deployment.available":"kube_deployment_status_replicas_available", 
            "kubernetes.deployment.desired":"kube_deployment_spec_replicas", 
            "kubernetes.node_allocatable_cpu":"kube_node_status_allocatable resource=cpu",
            "kubernetes.node_allocatable_ephemeral_storage":"kube_node_status_allocatable resource=ephemeral_storage",
            "kubernetes.node_allocatable_memory":"kube_node_status_allocatable resource=memory",
            "kubernetes.node_ready":"kube_node_status_condition condition=ready",
            "kubernetes.pod_phase":"kube_pod_status_phase phase=running",
            "kubernetes.stateful_set.current":"kube_statefulset_status_replicas_current", 
            "kubernetes.stateful_set.desired":"kube_statefulset_replicas", 
            "kubernetes.stateful_set.ready":"kube_statefulset_status_replicas_ready", 
            "machine_memory_bytes ":"node_memory_bytes_total:sum", 
            "memory.total ":"container_memory_usage_bytes", 
            "plexus.heartbeat":"chs_plexus_worker_heartbeat", 
            "rollout_reconcile":"rollout_reconcile_sum", 
            "ta_client.prod.app.memory.heap_alloc":"memory_heap_alloc", 
            "ta_client.prod.app.memory.sys":"memory_sys",
            "ta_client.prod.app.memory.total_alloc":"memory_total_alloc", 
            "ta_client.prod.msg.received":"msg_received", 
            "ta_client.prod.msg.sent":"msg_sent", 
            "workqueue_queue_duration_seconds":"workqueue_queue_duration_seconds_sum", 
            "workqueue_work_duration_seconds":"workqueue_work_duration_seconds_sum"

        }
        
        matching_keys = [key for key in metrics_to_adjust.keys() if re.search(regex_pattern, key, re.IGNORECASE)]

        if not matching_keys:
            ds_name_value_might_be_present = re.search('(?P<metric>^.*?)(?:\.)(?P<ds_name_value>changed|compiled|config_retrieval|failed|failed_to_restart|free|in|io_time|last_run|longterm|majflt|midterm|minflt|out|out_of_sync|processes|read|restarted|rx|scheduled|shortterm|skipped|syst|threads|time|total|tx|used|user|weighted_io_time|write)', metric_name.replace("*",""))
            base_metric = None
            potential_ds_name = None
            metric_query_by_ds_name = None

            if ds_name_value_might_be_present:
                base_metric = ds_name_value_might_be_present.group('metric')
                potential_ds_name = ds_name_value_might_be_present.group('ds_name_value')

                metrics_with_valid_ds_name_dimension = (
                    "df.cache", "disk_io_time", "disk_merged", "disk_octets", "disk_ops", "disk_time", \
                    "if_dropped", "if_errors", "if_octets", "if_packets", "load", "memcached_octets", \
                    "ps_count", "ps_cputime", "ps_disk_octets", "ps_disk_ops", "ps_pagefaults", "puppet_run", \
                    "puppet_time", "vmpage_faults", "vmpage_io.memory", "vmpage_io.swap"
                    )

                if base_metric in metrics_with_valid_ds_name_dimension:
                    metric_query_by_ds_name = f"{base_metric} ds_name={potential_ds_name}"
                
                if metric_query_by_ds_name:
                    metric_name = metric_query_by_ds_name
        
            def wildcard_tailing_function(metric_names, func_name):
                if func_name in metric_names:
                    regex_pattern = func_name + r"(.*?)$"
                    replace= r"*" + func_name + r"\1"
                    metric_names = re.sub(regex_pattern, replace, metric_names)
                    return metric_names
            
            math_funcs = ["-average","-lower","-percentile-90","-sum","-upper","-average", "-count"]
            metric_names = [metric_name] * len(math_funcs)

            if any(math_func in metric_name for math_func in math_funcs):
                metric_name = next(final_metric_name for final_metric_name in list(map(wildcard_tailing_function,metric_names, math_funcs)) if final_metric_name)

        else:
            metric_name = metrics_to_adjust[matching_keys[0]]

        return metric_name

    def __init__(self) -> None:
        self.slVariable = {"id":None,"name":None,"displayName":None,"defaultValue":"*","sourceDefinition":{"variableSourceType":"MetadataVariableSourceDefinition","filter":"","key":None},"allowMultiSelect":True,"includeAllOption":True,"hideFromUI":False,"valueType":"Any"}
        self.dashboardJSON = {"type":"DashboardV2SyncDefinition","name":None,"description":None,"title":None,"rootPanel":None,"theme":None,"topologyLabelMap":{"data":{}},"refreshInterval":0,"timeRange":{"type":"BeginBoundedTimeRange","from":None,"to":None},"layout":{"layoutType":"Grid","layoutStructures":[]},"panels":[],"variables":[],"coloringRules":[]}
        self.slPanal = {"id":None,"key":"","title":None,"visualSettings":"","keepVisualSettingsConsistentWithParent":True,"panelType":"SumoSearchPanel","queries":[],"description":"","timeRange":None,"coloringRules":None,"linkedDashboards":[]}
        self.slQuery = {"queryString":None,"queryType":"Metrics","queryKey":None,"metricsQueryMode":"Advanced","metricsQueryData":None,"tracesQueryData":None,"parseMode":"Auto","timeSource":"Message"}
        self.layoutStructure = {"height":6,"width":12,"x":0,"y":12}

        self.visualSettingsGeneralTS =  {"mode":"timeSeries","type":"line","displayType":"default","markerSize":5,"lineDashType":"solid","markerType":"none","lineThickness":1, "aggregationType":"latest"}       
        self.displayOverride = {"series":[],"queries":[],"properties":{"name":None}}
        self.visualSettingsLegend = {"enabled":False,"verticalAlign":"bottom","fontSize":12,"maxHeight":50,"showAsTable":False,"wrap":True}
        self.visualSettingsColor = {"family":"Categorical Default"}
        self.visualSettingsAxes = {"axisX":{"title":None,"titleFontSize":12,"labelFontSize":12,"hideLabels":False},"axisY":{"title":None,"titleFontSize":12,"labelFontSize":12,"logarithmic":False,"unit":{"value":"","isCustom":False}}}
        
        self.visualSettingsThreshold = {"from":None,"to":None,"color":""}
       
        self.visualSettingsSVP = {"option":"Latest","label":"","useBackgroundColor":False,"useNoData":False,"noDataString":"No data","hideData":False,"hideLabel":False,"rounding":2,"valueFontSize":24,"labelFontSize":14,"sparkline":{"show":False,"color":"#222D3B"},"gauge":{"show":True}}
        self.visualSettingsGeneralSVP = {"mode":"singleValueMetrics","type":"svp","displayType":"default"}
        
        self.visualSettingsHC = {"honeyComb":{"shape":"hexagon","groupBy":[{"label":"clustername","value":"clustername"}],"aggregationType":"latest"}}
        self.visualSettingsGeneralHC = {"mode":"honeyComb","type":"honeyComb","displayType":"default","aggregationType":"avg"}
       
        self.visualSettings = {"title":{"fontSize":14},"series":{},"hiddenQueryKeys":[],"legend":{"enabled":False}}

        self.mdVisualSettings = {"title":{"fontSize":14},"text":{"format":"markdownV2"},"series":{}}
        self.dashboardVariable = {"id":None,"name":None,"displayName":None,"defaultValue":None,"sourceDefinition":{"variableSourceType":"MetadataVariableSourceDefinition","filter":"","key":None},"allowMultiSelect":False,"includeAllOption":False,"hideFromUI":False,"valueType":"Any"}
        self.timeRange = {"type":"BeginBoundedTimeRange","from":{"type": "RelativeTimeRangeBoundary","relativeTime": "-105m"},"to":None}
