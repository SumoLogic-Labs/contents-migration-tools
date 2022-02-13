from __future__ import print_function

import io
import json
import logging
import os
import os.path
from pathlib import Path
import re
import secrets
from pathlib import Path
from types import SimpleNamespace

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from SFX.SFXQueryTransPiler import SFXTranslator
from SFX.Utils import *

gdrive_folders_tobe_processed = {
       ############################################################
        "Atlas Classic":"18lxwt4o-lZ3OKSK-7E_y85LP93FKgKU1",    # 1
       ############################################################
        "Atlas: Core Infra":"1z6vdz1ibHOFQgQzxAFu4sWvI5C5Dd3C3",# 2
       ############################################################
        "CDE":"1HKMNmENM9CgA3EMlHByg5h_K0eLyq4J_",              # 3
       ############################################################
        "CH & Plexus":"1scoNXhx6upazMXf46DzOT-F1c1ZTlAc6",      # 4
       ############################################################
        "Cloud Data":"13xUhe3ZI-fmrwfthhGiLKuB6F0ZP7i0-",       # 5
       ############################################################
        "Cloud IDE":"1o45gLQ0H0RFmz3IU75ayIevI_tRicvpH",        # 6
       ############################################################
        "Messaging":"13RLwYmtyHALTstzU46tA_bHstHh63eOm",        # 7
       ############################################################
        "NGC Web":"1wzHqzL_V6GObFHra0yBlHS2lYqJ2GWUC",          # 8
       ############################################################
        "Ops/Support":"128Ne2FzIqDwfQHsfso8XXF4zo5PCt-Wy",      # 9
       ############################################################
        "Pipelines":"18hkh6gxSHXzFabCu1XzJ_ug_e80YaChA",       # 10
       ############################################################
        "Upgrades":"1f39wWxnSwKcOqnJMvi6L9jnSLd583O_w"         # 11
       ############################################################
}
 # Custom generation
usGdrive = False
specefic_file_processing = ""
# You may specify one or more indexes as CSVs
specefic_folders_processing_indexes = ""
add_dev_panel = True

home = str(Path.home())
outputPathBase = os.path.join(home,'SFXSumoTranspiler')
outputSLPathBase = os.path.join(outputPathBase, 'SumoLogic', 'Dashboards')
outputSLPathLocal = os.path.join(outputSLPathBase, 'Local')
outputSLPathGDrive = os.path.join(outputSLPathBase, 'GDrive')
sourcePathBase = os.path.join(home, 'SFXDashboards')
sourcePathLocal = os.path.join(sourcePathBase, 'Local')
sourcePathGDrive = os.path.join(sourcePathBase, 'GDrive')
folders_being_processed = []


def main():
    number_of_dashboards = 0
    number_of_panels = 0
    number_of_queries = 0
    max_number_of_panels=0
    max_number_of_queries=0
    longest_query=0
    dashboard_name_with_max_number_of_panels = ""
    dashboard_panel_path_with_max_number_of_panels = ""
    dashboard_panel_query_path_with_longest_query = ""

    if usGdrive:
        folders_being_processed = get_gdrive_folders_to_process(gdrive_folders_tobe_processed=gdrive_folders_tobe_processed , specefic_folders_processing_indexes=specefic_folders_processing_indexes, \
                                                         specefic_file_processing=specefic_file_processing, \
                                                         sourceBase=sourcePathGDrive ,\
                                                         outputBase=outputSLPathGDrive
                                                         )
    else:        
        folders_being_processed = traverse([], sourcePathLocal)

    current_logger = None


    for outputPath, folderName,items in folders_being_processed:
        
        if not len(items) > 0:
            continue
        
        lfpath = os.path.join(outputPath, 'parsing.log')
        setup_logger(folderName, lfpath)
        current_logger = logging.getLogger(folderName)

        if not items:
            print('No files found.')
        else:
            sfx_dashboard_document_name=''
            for item in items:
                specefic_file_processing_requested = False
                signalfx_json_name_path = os.path.join(item['src_folder_path'], item['name'])
                sumologic_json_name_path = os.path.join(item['oput_folder_path'], 'sumo_ready_' + item['name'])
                sfx_dashboard_document_name = item['doc_name']
                signal_fx_dash = open(signalfx_json_name_path)
                signal_fx_dash_obj = json.load(signal_fx_dash,  object_hook=lambda d: RecursiveNamespace(**d))
                dashboardTR = SimpleNamespace(**SumoLogicDashboardDicts().timeRange)
                sumoLogicDashVariables = []
                sumoLogicFiltersInjections = []
                query_duration = '1h'

                if signal_fx_dash_obj.dashboardExport.dashboard.filters.time:
                    dash_time = signal_fx_dash_obj.dashboardExport.dashboard.filters.time.start
                    query_duration = dash_time.replace("-","") if (isinstance(dash_time, str) and "-" in dash_time) else f"{dash_time/1000/60}m"
                    dashboardTR.__dict__["from"]["relativeTime"] = dash_time
                
                if signal_fx_dash_obj.dashboardExport.dashboard.filters.sources:
                    for source in signal_fx_dash_obj.dashboardExport.dashboard.filters.sources:
                        sumoLogicFiltersInjections.append(SumoLogicDashboardDicts.get_adjusted_filters_key(source.property))
                        sumoLogicDashVariable = RecursiveNamespace(**SumoLogicDashboardDicts().slVariable)
                        sumoLogicDashVariable.name = SumoLogicDashboardDicts.get_adjusted_filters_key(source.property)
                        sumoLogicDashVariable.displayName = SumoLogicDashboardDicts.get_adjusted_filters_key(source.property)
                        sumoLogicDashVariable.sourceDefinition.key = SumoLogicDashboardDicts.get_adjusted_filters_key(source.property)
                        sumoLogicDashVariables.append(sumoLogicDashVariable)

                sumo_dash_obj = RecursiveNamespace(**SumoLogicDashboardDicts().dashboardJSON)
                sumo_native = sumo_dash_obj
                sumo_native.timeRange = dashboardTR
                sumo_native.name = signal_fx_dash_obj.dashboardExport.dashboard.name 
                sumo_native.title = signal_fx_dash_obj.dashboardExport.dashboard.name
                sumo_native.description = signal_fx_dash_obj.dashboardExport.dashboard.name
                sumo_native.theme = "Light"
                errors_found = False
                total_errors = 0
                last_row = 0

                current_logger.info(f"\n\n{'='*100}\nOn File: {sfx_dashboard_document_name}\n{'='*100}\n")
                dev_notes_text = []

                number_of_dashboards += 1

                for chart in signal_fx_dash_obj.dashboardExport.dashboard.charts:
                    layoutStructure = {}
                    ls = SimpleNamespace(**SumoLogicDashboardDicts().layoutStructure)
                    layoutStructure["key"] = chart.chartId
                    ls.height = 6
                    ls.width = chart.width * 2
                    ls.x = chart.column * 2
                    ls.y = chart.row * 6
                    last_ls = max(last_row, ls.y)
                    layoutStructure["structure"] = json.dumps(ls, default=lambda obj: obj.__dict__)
                    sumo_native.layout.layoutStructures.append(layoutStructure)
                
                if add_dev_panel:
                # Dev panel
                    devPanel = RecursiveNamespace(**SumoLogicDashboardDicts().slPanal)
                    devVS = RecursiveNamespace(**SumoLogicDashboardDicts().mdVisualSettings)
                    devPanel.visualSettings = json.dumps(devVS, default=lambda obj: obj.__dict__)
                    sumo_dev_panel_key = "panelPANE-" + secrets.token_hex(8).upper()
                    devPanel.title = "PS Consults' Notes"
                    devPanel.key = sumo_dev_panel_key
                    devPanel.panelType = "TextPanel"
                    dev_notes_text.append("## **Below find the details of SignalFX Dashboard:** ({})\n- **Doc Link:**\t[{}]({})\n- **JSON Link:**\t[{}]({})".format(sfx_dashboard_document_name,item['doc_name'],item['doc_webViewLink'],item['name'],item['webViewLink']))
                    dev_notes_text.append("- **Sumo Transpiler Importing Report:**")
                    devLayoutStructure = RecursiveNamespace(**SumoLogicDashboardDicts().layoutStructure)
                    devPanelLayoutStructure = {}
                    devPanelLayoutStructure["key"] = sumo_dev_panel_key
                    devLayoutStructure.height = 16
                    devLayoutStructure.width = 24
                    devLayoutStructure.x = 0
                    devLayoutStructure.y = last_ls + 1
                    devPanelLayoutStructure["structure"] = json.dumps(devLayoutStructure, default=lambda obj: obj.__dict__)
                    sumo_native.layout.layoutStructures.append(devPanelLayoutStructure)

                queryLimit = 20

                sfx_to_sumo_units = {"Bit":"bi", "Byte":"Bi","Kibibyte":"KiB","Mebibyte":"MiB", "Day":"d","Hour":"h","Minute":"m","Second":"s","Millisecond":"ms","Microsecond":"us","Nanosecond":"ns"}

                max_number_of_panels = max(max_number_of_panels, len(signal_fx_dash_obj.chartExports))

                if len(signal_fx_dash_obj.chartExports) == max_number_of_panels:
                    dashboard_name_with_max_number_of_panels = sumo_native.name
                

                for chartIdx, chartExport in enumerate(signal_fx_dash_obj.chartExports):
                    number_of_panels += 1
                    sfQIdx = 1
                    errCtr = 0
                    last_stream_var = ""
                    deferred_queries = {}
                    sfxqt = SFXTranslator()
                    aggregationTypeIsSet = False
                    skipOverrides = False
                    panel = RecursiveNamespace(**SumoLogicDashboardDicts().slPanal)
                    currentPanelVS = RecursiveNamespace(**SumoLogicDashboardDicts().visualSettings)
                    plotTypeMap = {"AreaChart": "area", "ColumnChart": "column", "Histogram": "line", "LineChart": "line"}
                    sfx_chart_publish_label_options =[]
                    sfxChartType = ""
                    threasholds=[]

                    if chartExport.chart.options and "type" in chartExport.chart.options.__dict__.keys():
                        sfxChartType = chartExport.chart.options.type

                    if chartExport.chart.options and "colorScale2" in chartExport.chart.options.__dict__.keys() and \
                        chartExport.chart.options.colorScale2 and len(chartExport.chart.options.colorScale2) > 0:
                        sfxColorScale2entries = chartExport.chart.options.colorScale2
                        threasholds = []

                        for colorEntry in sfxColorScale2entries:
                            scaleStart = colorEntry.gt + (colorEntry.gt/1000) if colorEntry.gt else colorEntry.gte if colorEntry.gte else 0
                            scaleEnd = colorEntry.lt - (colorEntry.lt/1000) if colorEntry.lt else colorEntry.lte if colorEntry.lte else 10000
                            threashold = SumoLogicDashboardDicts().visualSettingsThreshold
                            threashold["from"] = scaleStart
                            threashold["to"] = scaleEnd
                            threashold["color"] = get_color_from_palette_index(colorEntry.paletteIndex)
                            threasholds.append(threashold)

                    if sfxChartType=="TimeSeriesChart" and chartExport.chart.options and "defaultPlotType" in chartExport.chart.options.__dict__.keys():
                        general = RecursiveNamespace(**SumoLogicDashboardDicts().visualSettingsGeneralTS)
                        legend = RecursiveNamespace(**SumoLogicDashboardDicts().visualSettingsLegend)
                        color = RecursiveNamespace(**SumoLogicDashboardDicts().visualSettingsColor)
                        general.type = plotTypeMap[chartExport.chart.options.__dict__["defaultPlotType"]]
                        axes = RecursiveNamespace(**SumoLogicDashboardDicts().visualSettingsAxes)
                        currentPanelVS.__dict__["general"] = general
                        currentPanelVS.__dict__["legend"] = legend
                        currentPanelVS.__dict__["color"] = color
                        currentPanelVS.__dict__["axes"] = axes

                        if chartExport.chart.options and "axes" in chartExport.chart.options.__dict__.keys() \
                            and chartExport.chart.options.axes and \
                                len(chartExport.chart.options.axes) > 0 and chartExport.chart.options.axes[0] \
                                    and chartExport.chart.options.axes[0].label:
                            axisY_label = chartExport.chart.options.axes[0].label
                            axes = RecursiveNamespace(**SumoLogicDashboardDicts().visualSettingsAxes)
                            axes.axisY.title = axisY_label

                            currentPanelVS.__dict__["axes"] = axes

                    if sfxChartType=="SingleValue" and chartExport.chart.options and "secondaryVisualization" in chartExport.chart.options.__dict__.keys():
                        skipOverrides = True
                        svpVS = RecursiveNamespace(**SumoLogicDashboardDicts().visualSettingsSVP)
                        secondaryVisualization=chartExport.chart.options.secondaryVisualization
                        visualSettingsGeneralSVP = RecursiveNamespace(**SumoLogicDashboardDicts().visualSettingsGeneralSVP)
                        shwoGuage = secondaryVisualization in ("Radial", "Linear")
                        svpVS.sparkline.show= not shwoGuage
                        svpVS.gauge.show= shwoGuage
                        svpVS.__dict__["threasholds"] = threasholds
                        currentPanelVS.__dict__["general"] = visualSettingsGeneralSVP

                        if threasholds:
                            svpVS.__dict__["threasholds"] = threasholds

                        currentPanelVS.__dict__["svp"] = svpVS

                    if sfxChartType in ("List","Heatmap") and chartExport.chart.options:
                        hcVS = RecursiveNamespace(**SumoLogicDashboardDicts().visualSettingsHC)

                        if threasholds:
                            hcVS.__dict__["threasholds"] = threasholds

                        visualSettingsGeneralHC = RecursiveNamespace(**SumoLogicDashboardDicts().visualSettingsGeneralHC)
                        currentPanelVS.__dict__["honeyComb"] = hcVS
                        currentPanelVS.__dict__["general"] = visualSettingsGeneralHC

                    if chartExport.chart.options and "publishLabelOptions" in chartExport.chart.options.__dict__.keys():
                        sfx_chart_publish_label_options = chartExport.chart.options.__dict__["publishLabelOptions"]
                        if sfx_chart_publish_label_options:
                            for plo in sfx_chart_publish_label_options:
                                if plo.valueUnit and sfx_to_sumo_units.get(plo.valueUnit) and (currentPanelVS.__dict__ and "axes" \
                                    in currentPanelVS.__dict__.keys()) and ("axisY" in currentPanelVS.__dict__["axes"].__dict__.keys()):
                                    currentPanelVS.__dict__["axes"].__dict__["axisY"].__dict__["unit"].__dict__["value"] = sfx_to_sumo_units.get(plo.valueUnit)
                                    break
                    
                    panel.timeRange = None
                    sumo_panel_key = chartExport.chart.id
                    panel.title = chartExport.chart.name
                    panel.key = sumo_panel_key
                    targetQueries = []
                    lastTargetQueries = []
                    hiddenQueryKeys = []
                    displayORS=[]
                    tab_char = "\t"

                    if chartExport.chart.options.type == "Text":
                        panel.panelType = "TextPanel"
                        panel.text = chartExport.chart.options.markdown
                    else:
                        sfx_input = chartExport.chart.programText
                        sfx_inputs = sfx_input.split('\n') 
                        current_panel_title = f"**Panel Title:** ({panel.title})"
                        current_panel_id = f"**Panel id:** ({sumo_panel_key})"
                        current_sfx_program = f"SFX Program Text:\n{sfx_input}"
                        current_logger.info("\n\n" + current_panel_title + "\n" + current_panel_id + "\n" + current_sfx_program + "\n\nSumo query translation details:\n")
                        
                        if add_dev_panel:
                            dev_notes_text.append(" - " + current_panel_title)
                            dev_notes_text.append(" - " + current_panel_id)
                            dev_notes_text.append(f"{tab_char*2}- **SFX Program Text:** {sfx_input}")
                            dev_notes_text.append(f"{tab_char*2}- **Sumo query translation details:**")
                        numerical_queries = {}

                        try:
                            query_duration_number = re.search('(\d+)',query_duration).group(0)
                            query_duration_unit = re.search('[s|m|h|d|w]',query_duration).group(0)

                            if query_duration_number and query_duration_unit:
                                if query_duration_unit == 'w':
                                    query_duration_number = int(query_duration_number) * 7
                                    query_duration_unit = 'd'
                                    query_duration = f"{str(query_duration_number)}{query_duration_unit}"

                            single_collection_sumo_queries = sfxqt.translate(current_logger, sfx_input, sumoLogicFiltersInjections,query_duration=query_duration)

                            max_number_of_queries = max(max_number_of_queries, len(single_collection_sumo_queries.items()))

                            if len(single_collection_sumo_queries.items()) == max_number_of_queries:
                                dashboard_panel_path_with_max_number_of_panels = f"{sumo_native.name} / {panel.title}"


                            for stream_var, sumo_query in single_collection_sumo_queries.items():

                                longest_query = max(longest_query, len(sumo_query['query']))

                                if len(sumo_query['query']) == longest_query:
                                    dashboard_panel_query_path_with_longest_query = f"{sumo_native.name}/{panel.title}/{stream_var}"
                                
                                if sumo_query and sumo_query['query']:
                                    sumo_query['query'] = ' '.join(sumo_query['query'].split()).strip()
                                
                                if sumo_query and sumo_query['query'] and  (check_number(sumo_query['query'])):
                                    numerical_queries[f"#{stream_var}"] = sumo_query['query']
                                    continue

                                for numerical_query_label,value in numerical_queries.items():
                                    sumo_query['query'] = sumo_query['query'].replace(numerical_query_label,value)
                                
                                queries_len = len(sfx_inputs)
                                number_of_queries += 1
                                if sfQIdx >= queryLimit and sfQIdx < queries_len:
                                    if sfQIdx==queryLimit:
                                        last_stream_var = stream_var
                                        lastTargetQueries.append(f"// Below are the queries where they exceded the current spported limit of ({queryLimit})\n")

                                    if sfQIdx >= queryLimit:
                                        sfQIdx += 1
                                        currentQuery = f"#{stream_var} = " + sumo_query["query"]
                                        lastTargetQueries.append(currentQuery)
                                        continue
                                elif sfQIdx >= queryLimit and sfQIdx==queries_len:
                                    stream_var = last_stream_var
                                    sumo_query["query"]= "\n".join(lastTargetQueries)

                                hide_query = False
                                displayOR = None
                                displayOR = RecursiveNamespace(**SumoLogicDashboardDicts().displayOverride)
                                grp_bys=[]
                                query_vsor = ""
                                
                                if sumo_query["bys"]:
                                    for by in sumo_query["bys"]:
                                        grp_bys.append(f"{{{{{by}}}}}")


                                if sumo_query["publish"]:
                                    query_vsor = sumo_query['publish']['label']
                                
                                if len(grp_bys) > 0:
                                    query_vsor += (" - " + " - ".join(grp_bys))
                                    
                                if not sumo_query['publish']['enable']:
                                    hide_query = True
                                    hiddenQueryKeys.append(stream_var)
                                elif not aggregationTypeIsSet:
                                    aggregationType = "latest"
                                    if sumo_query['aggregationType']:
                                        aggregationType = sumo_query['aggregationType']

                                        if sfxChartType=="TimeSeriesChart":
                                            currentPanelVS.__dict__["general"].__dict__["aggregationType"] = aggregationType
                                        
                                        if sfxChartType=="SingleValue":
                                            currentPanelVS.__dict__["svp"].__dict__["option"] = aggregationType
                                        
                                        if sfxChartType in ("List", "HeatMap"):
                                            currentPanelVS.__dict__["honeyComb"].__dict__["aggregationType"] = aggregationType
                                    aggregationTypeIsSet = True

                                if query_vsor:
                                    displayOR.queries.append(stream_var)
                                    displayOR.properties.name = query_vsor

                                current_query_translation_details_list = []
                                current_query_translation_details_list.append(f"**Lable:** {stream_var}") 
                                sumo_query_to_dev_text = sumo_query['query'].replace("*","\*").replace("-","\-")
                                current_query_translation_details_list.append(f"**Sumo Query:** {sumo_query_to_dev_text}") 
                                current_query_translation_details_list.append(f"**Group By:** {sumo_query['bys']}") 
                                current_query_translation_details_list.append(f"**Hidden query:** {hide_query}") 
                                current_query_translation_details_list.append(f"**Visual Overrides: Queries:** {displayOR.queries} **& Name:** {displayOR.properties.name}")
                                current_logger.info("\n\n" + "\n\t".join(current_query_translation_details_list).replace("\*","*").replace("\-","-") + "\n\n")
                               
                                if add_dev_panel:
                                    dev_notes_text.append(f"{tab_char*3}- " + f"\n{tab_char*3}- ".join(current_query_translation_details_list))
                                
                                displayORS.append(displayOR)
                                query = SimpleNamespace(**SumoLogicDashboardDicts().slQuery)
                                query.queryKey = stream_var
                                query.queryString = sumo_query["query"]
                                sfQIdx += 1
                                targetQueries.append(query)
                        except Exception as e:
                            errors_found = True
                            errCtr+=1
                            total_errors+=1
                            exception_to_dev_text = str(e.__str__()).replace("*","\*").replace("-","\-")
                            query_translation_error_details_dev_text = f"**Error of:** {exception_to_dev_text}"
                            query_translation_error_details = f"**Error of:** {e}"
                            error_report_dev_text = f"{tab_char*3}- **The query failed to import with the following error:** \n{tab_char*4}- {query_translation_error_details_dev_text}"
                            if add_dev_panel:
                                dev_notes_text.append(error_report_dev_text)
                            current_logger.exception("\n\n" + query_translation_error_details + "\n\n")
                        finally:
                            panel.queries = targetQueries
                            if not skipOverrides:
                                currentPanelVS.__dict__["overrides"] = displayORS
                            currentPanelVS.hiddenQueryKeys=hiddenQueryKeys
                            panel.visualSettings = json.dumps(currentPanelVS, default=lambda obj: obj.__dict__)
                
                        if errCtr:
                            panel.title +=f" - with {errCtr} Transpiler failures"
                    sumo_native.panels.append(panel)

                if add_dev_panel:
                    dev_notes_text_str = "\n".join(dev_notes_text)
                    devPanel.text = dev_notes_text_str

                if add_dev_panel:
                    sumo_native.panels.append(devPanel)

                sumo_native.variables = sumoLogicDashVariables
                sumo_native.name += "  - Imported By Sumo Transpiler" if not errors_found else f"  - Imported By Sumo Transpiler (With {total_errors} failures)"
                
                with open(sumologic_json_name_path, 'w') as outfile:
                    json.dump(sumo_native, outfile,indent=4, default=lambda obj: obj.__dict__)
                    signal_fx_dash.close()

                print(f"{sfx_dashboard_document_name} file processed\n\n")
                current_logger.info(f"{sfx_dashboard_document_name} file processed\n\n")
                
                if specefic_file_processing_requested:
                    break
        current_logger.info(f"{folderName} folder processing completed\n\n")
    print(f"Processing Stats:\n- Number of dashboards: {number_of_dashboards}\n- Number of panels: {number_of_panels}\n- Number of queries: {number_of_queries}\n- Max number of panels of: {max_number_of_panels} found in {dashboard_name_with_max_number_of_panels}\n- Max number of quries of: {max_number_of_queries} found in {dashboard_panel_path_with_max_number_of_panels}\n- Longest query of: {longest_query} chars, found in {dashboard_panel_query_path_with_longest_query}\n")

def get_gdrive_service():
    creds = None
    SCOPES = ['https://www.googleapis.com/auth/drive']

    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('drive', 'v3', credentials=creds)

    return service

def get_gdrive_folders_to_process(gdrive_folders_tobe_processed, specefic_folders_processing_indexes=None, specefic_file_processing=None, sourceBase=None , outputBase=None):
    
 
    specefic_folders_processing = [list(gdrive_folders_tobe_processed)[int(sfpidx)-1] for sfpidx in [sfpidx for sfpidx in specefic_folders_processing_indexes.split(',')]] if specefic_folders_processing_indexes else None

    def get_sfX_files_details_per_folder_from_gdrive(entries):
        gdriveFolderId, folderName = entries
        folder_processing_log = f"\n\nProcessing: {folderName} folder\n{'-'*50}\n"
        items = []
        query_string = "('" + f"{gdriveFolderId}" + "' in parents and name contains '" + f"{specefic_file_processing}" + "' and mimeType='application/json')"
        service = get_gdrive_service()
        results = service.files().list(q= query_string,
            pageSize=1000, fields="nextPageToken, files(id, name, webViewLink)",supportsAllDrives=True ,includeItemsFromAllDrives=True).execute()
        files = results.get('files', [])
        
        def update_item_name(item):
            item['name']=path_escape(item['name'])
            return item

        files = list(map(lambda item:update_item_name(item), files))
        if files and len(files) > 0:
            items = [item for item in files if specefic_file_processing in item['name']]

        def get_sfX_files_per_folder_from_gdrive(item):
            sourceBasePath = os.path.join(sourceBase,folderName)
            outputBasePath = os.path.join(outputBase,folderName)
            Path(sourceBasePath).mkdir(parents=True, exist_ok=True)
            Path(outputBasePath).mkdir(parents=True, exist_ok=True)
            download_file_path = os.path.join(sourceBasePath,item['name'])
            if not os.path.exists(download_file_path):
                fh = io.FileIO(download_file_path, "wb")
                request = service.files().get_media(fileId=item['id'])
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                print(f"Getting: {download_file_path}\nProgress: ")
                while done is False:
                    status, done = downloader.next_chunk()
                    progress = "Download %d%%." % int(status.progress() * 100)
                    print(f"\b{progress}..")
            else:
                print(f"Already found {download_file_path}\n")

            item['src_folder_path'] = sourceBasePath
            item['oput_folder_path'] = outputBasePath
            return item

        def get_sfx_docs_per_dahsboard(item):
            sfx_dashboard_document_name = ''
            regex = re.search('^([\S]+)', item['name'])
            if regex:
                sfx_dashboard_document_name = regex.group(1)
            docSearchResults = service.files().list(\
                q="('" + f"{gdriveFolderId}" + \
                "' in parents and mimeType='application/vnd.google-apps.document' and name contains '" + \
                    f"{sfx_dashboard_document_name}"+"')" ,
                pageSize=10, fields="nextPageToken, files(id, name, webViewLink)",supportsAllDrives=True ,\
                includeItemsFromAllDrives=True).execute()
            docSearchItems = docSearchResults.get('files', [])
            if docSearchItems and len(docSearchItems) > 0:
                for docSearchItem in docSearchItems:
                    item['doc_id'] = docSearchItem['id']
                    item['doc_name'] = docSearchItem['name']
                    item['doc_webViewLink'] = docSearchItem['webViewLink']
                    item = get_sfX_files_per_folder_from_gdrive(item)
            return item

        detailedItems = list(map(get_sfx_docs_per_dahsboard, items))
        return detailedItems[0]['oput_folder_path'], folderName,detailedItems
    
    folders_being_activly_processed = {gdriveFolderId:path_escape(folderName) for folderName, gdriveFolderId in gdrive_folders_tobe_processed.items() if not (specefic_folders_processing and len(specefic_folders_processing) > 0 and folderName not in specefic_folders_processing)}
    results = dict(zip(folders_being_activly_processed, map(get_sfX_files_details_per_folder_from_gdrive, folders_being_activly_processed.items())))
    matching_results = {id:files for id,files in results.items() if len(files) > 0}
    
    return matching_results.values()

def traverse(all_folders_being_processed, src):
        for folderName, _, fileList in os.walk(src):
            sourcePath = os.path.join(sourcePathLocal, os.path.relpath(folderName, src))
            outputPath = os.path.join(outputSLPathLocal, os.path.relpath(folderName, src))
            Path(outputPath).mkdir(parents=True, exist_ok=True)
            sfx_files = [file for file in fileList if file.endswith('.json') and not file.startswith('sumo_ready')]
            items = []
            for file in sfx_files:
                item = {}
                item['name'] = file
                item['webViewLink'] = ''
                item['doc_name'] = file
                item['doc_webViewLink'] = ''
                item['src_folder_path'] = sourcePath
                item['oput_folder_path'] = outputPath
                items.append(item)
            if len(items) > 0:
                all_folders_being_processed.append((outputPath, folderName, items))
        return all_folders_being_processed

if __name__ == '__main__':
    main()
