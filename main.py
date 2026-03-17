import shutil
import os, sys
import types
import json
import time 
import argparse 
import egdbToFGDB
from tableLogger import TableLogger
import logging
from arcgis import GIS
from arcgis.features import FeatureLayerCollection
import re
import datetime
import click
import threading
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib, ssl
import multiprocessing
import keyring
import gc


# Workflow map
# 1) Parse CLI + load config
# 2) Load layer spreadsheet into service map
# 3) Export SDE data to FGDB ZIPs (per workspace)
# 4) Swap workflow: overwrite inactive A/B and repoint view layers
# 5) Write dashboard logs + email on failures


scriptDir = (os.path.dirname(os.path.realpath(__file__)))

# Setup argument parsing
parser = argparse.ArgumentParser(description="Script for carrying out the SDE to AGOL process")
parser.add_argument('-c', '--config', type=str, required=True, help="Specify the path to the config")
parser.add_argument('-p', '--publish', action='store_true', help="Publish A and B copies of layers in the CSV")
parser.add_argument('-w', '--workspace', type=str, required=True, help="Specify the fgdb workspace directory name")
parser.add_argument('-i', '--interval', type=str, choices=['Nightly', 'Weekly', 'Monthly', 'Yearly', 'On Demand'], help="'Nightly', 'Weekly', 'Monthly', 'Yearly', or 'On Demand' for which layers the swap will be run on")


# Parse command-line arguments
args = parser.parse_args()

# Check for mutually exclusive arguments
if args.publish and args.interval:
    parser.error("The arguments -p/--publish and -i/--interval cannot be used together.")

    # Ensure --interval is required if --publish is not provided
if not args.publish and not args.interval:
    parser.error("The argument -i/--interval is required")

fgdbWorkspaceName = args.workspace
onlyPublish = args.publish
activeInterval = args.interval
configPath = args.config

def loadScriptConfig():
    """Load and normalize the script configuration JSON.

    Reads the config from the CLI path and supports legacy configs by converting
    a single `sdeWorkspace` entry into the `sdeWorkspaces` mapping format.

    Returns:
        dict: Parsed and normalized configuration.
    """
    with open(configPath, 'r') as f: 
        config = json.load(f)
    
    # Handle both old and new sdeWorkspace formats
    if 'sdeWorkspace' in config and 'sdeWorkspaces' not in config:
        config['sdeWorkspaces'] = {'default': config['sdeWorkspace']}
        logger.warning("Using legacy sdeWorkspace config - converted to sdeWorkspaces format")
    elif 'sdeWorkspaces' not in config:
        raise Exception("No sdeWorkspaces configuration found")
    
    return config
config = loadScriptConfig()

network_addr=f"{config['agol']['profile']}@arcgis_python_api_profile_passwords"
pwd = keyring.get_password(network_addr, config['agol']['profile'])
orgGIS = GIS(url=config['agol']['url'], username=config['agol']['username'], password=pwd)
if not hasattr(orgGIS.properties, 'user'):
    print('ERROR: Failed to connect to AGOL - please check profile in config')
    sys.exit('Failed to connect to AGOL - please check profile in config')
table_logger = TableLogger(orgGIS, config['agol'].get('logTableItemId'))
    
if onlyPublish:
    if not click.confirm(f'Are you sure you want to publish new copies for all layers in {config["layersSpreadsheet"]}?', default=False):
        quit()

curr_time = str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime()))

def sendEmail(allServicesInfo, config, logfile, subject):
    """Send a failure summary email with the run log attached.

    Builds an email from `config["email"]` settings and includes non-success
    service messages from `allServicesInfo`. Attaches the local logfile.
    """
    if "email" in config:
        emailInfo = config["email"]
        if "enabled" in emailInfo and emailInfo["enabled"]:
            emailServer = emailInfo["smtpServer"]
            emailPort = emailInfo["smtpPort"]
            emailFrom = emailInfo["from"]
            emailTo = emailInfo["to"]

            emailBody = subject

            for service in allServicesInfo:
                if allServicesInfo[service]['logs'] != "Success":
                    emailBody = f"{emailBody} \n Error: {allServicesInfo[service]['logs']}"

            mpMsg = MIMEMultipart()
            mpMsg['Subject'] = "AGOL to SDE Sync Report"
            mpMsg['From'] = emailFrom
            mpMsg['To'] = ", ".join(emailTo)
            mpMsg.attach(MIMEText(emailBody))

            # attach logfile
            with open(logfile, "r") as fd:
                attachment = MIMEText(fd.read())
                attachment.add_header("Content-Disposition", "attachment",
                                    filename=os.path.basename(logfile))
                mpMsg.attach(attachment)

            # send email
            try:
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                with smtplib.SMTP(emailServer, emailPort) as server:
                    server.starttls(context=context)
                    #server.login(emailFrom, emailInfo['password'])  # Ensure password is in config
                    server.sendmail(emailFrom, emailTo, mpMsg.as_string())
                    # self.logInfo("Successfully sent email")  # Remove or replace if needed
            except Exception as e:
                logger.error(f"Error sending email: {str(e)}")

def cleanOutputWorkspace(outputWorkspace):
    # output workspace where the file geodatabases will be exported to
    time.sleep(5)  # wait for any file locks to release
    if not os.path.exists(outputWorkspace):
        os.makedirs(outputWorkspace)
    else:
        outputDirectory = os.listdir(outputWorkspace)
        # remove old fgdbs
        for item in outputDirectory:
            try:
                if item.endswith(".zip") or item.endswith(".gdb"):
                    os.remove(os.path.join(outputWorkspace, item))
            except Exception as e:
                logger.error(f"Error removing file {item}: {str(e)}")
        # remove any empty directories
        for dirpath, dirnames, filenames in os.walk(outputWorkspace, topdown=False):
            for dirname in dirnames:
                try:
                    full_path = os.path.join(dirpath, dirname)
                    if not os.listdir(full_path): 
                        os.rmdir(full_path)
                except Exception as e:
                    logger.error(f"Error removing directory {dirname}: {str(e)}")

def printIDs(printDict, serviceNameList):
    for serviceName in serviceNameList:
        for service in printDict:
            if serviceName == service and len(printDict[service]) == 3:
                print(f"{service},{printDict[service][0]},{printDict[service][1]},{printDict[service][2]}")

def publishThreadFunc(newService, pubProps, outcomeContainer):
    try:
        newLayer = newService.publish(publish_parameters = pubProps, file_type = 'fileGeodatabase')
        outcomeContainer.append(newLayer)
    except Exception as e:
        outcomeContainer.append(e)

### Only used for publish workflow ###
def publishFGDBs(suffix, outputDirectory, printDict):
    for file in os.listdir(outputDirectory):
        try:
            filename = os.fsdecode(file)
            filePrefix = filename.split(".")[0]
            if not filename.endswith(".zip"):
                continue
            fgdb_base = os.path.join(outputWorkspace, filename)
            fgdb = shutil.copy(fgdb_base, os.path.join(outputWorkspace, f"ADMIN_{filePrefix}_{suffix}.zip"))
            os.remove(fgdb_base)
            serviceProp = {}
            serviceProp['type'] = 'File Geodatabase'
            serviceProp['itemType'] = "file"
            serviceProp['title'] = f"ADMIN_{filePrefix}_{suffix}"
            #serviceProp['tags'] = "sometag"

            pubProps = {}
            #pubProps["hasStaticData"] = 'true'
            pubProps["name"] = f"ADMIN_{filePrefix}_{suffix}"
            pubProps["layerInfo"] = {"capabilities":"Query"}
            #pubProps["editorTrackingInfo"] = {"enableEditorTracking":'true', "preserveEditUsersAndTimestamps":'true'}

            newService = orgGIS.content.add(item_properties=serviceProp, data = fgdb)
            print(f"new service: {newService}")
            logger.info(f"----- Added service {suffix} for {filePrefix} -----")

            outcomeContainer = []
            publishThread = threading.Thread(target=publishThreadFunc, args=(newService, pubProps, outcomeContainer))
            publishThread.start()
            publishThread.join(timeout=60*30) # Timeout after 30 minutes
            if publishThread.is_alive():
                raise Exception("Publish stalled")
            newLayer = outcomeContainer[0]
            if isinstance(newLayer, Exception):
                logger.info(f"Raising exception from inside thread")
                raise newLayer
            logger.info(f"----- Published layer {suffix} for {filePrefix} -----")

            if suffix == "A":
                sourceFLC = FeatureLayerCollection.fromitem(newLayer)
                newView = sourceFLC.manager.create_view(name = f"Pinellas_{filePrefix}")
                print(f"new view: {newView.id}")
                logger.info(f"----- Created view for {filePrefix} -----")
                if not printDict.get(filePrefix):
                    printDict[filePrefix] = []
                    printDict[filePrefix].append(newView.id)
                    printDict[filePrefix].append(newLayer.id)
            else:
                printDict[filePrefix].append(newLayer.id)
            del newService    # Remove the reference
            del newLayer      
            gc.collect()      

        except Exception as e:
            printDict[filePrefix] = []
            printDict[filePrefix].append("Error")
            logger.error(f"Error publishing {filename}: {str(e)}")
    return printDict

### Only used for publish workflow ###
def publishCopies(config, allServicesInfo, logger, outputWorkspace):
    """Publish A/B hosted service copies for all configured services.

    Groups services by SDE workspace, exports each group to FGDB ZIPs, and
    publishes A then B copies. Used for provisioning mode, not interval swap.
    """
    startTime = time.time()
    
    # Group services by their SDE workspace
    servicesByWorkspace = {}
    for serviceName, serviceInfo in allServicesInfo.items():
        workspace = serviceInfo.get('sdeWorkspace', 'default')
        if workspace not in servicesByWorkspace:
            servicesByWorkspace[workspace] = {}
        servicesByWorkspace[workspace][serviceName] = serviceInfo
    
    outputDirectory = os.fsencode(outputWorkspace)
    printDict = {}
    
    # Process each workspace separately
    for workspaceName, services in servicesByWorkspace.items():
        try:
            workspacePath = config['sdeWorkspaces'][workspaceName]
            logger.info(f"Publishing services from workspace '{workspaceName}': {workspacePath}")
            
            egdbExport = egdbToFGDB.egdbToFGDB(workspacePath, outputWorkspace, services, logger, table_logger)
            egdbExport.Execute()
            
            printDict = publishFGDBs("A", outputDirectory, printDict)
            
        except KeyError:
            logger.error(f"SDE workspace '{workspaceName}' not found in config. Skipping services: {list(services.keys())}")
            for serviceName in services.keys():
                printDict[serviceName] = ["Error - SDE workspace not found"]
            continue
        except Exception as e:
            logger.error(f"Error processing workspace '{workspaceName}': {str(e)}")
            for serviceName in services.keys():
                printDict[serviceName] = ["Error"]
            continue

    endTime = time.time()
    halfTime = endTime - startTime
    minutes, seconds = divmod(halfTime, 60)

    logger.info(f"############ DONE WITH ALL A COPIES IN {int(minutes)} MINUTES and {int(seconds)} SECONDS ############")

    cleanOutputWorkspace(outputWorkspace)

    # Repeat for B copies
    for workspaceName, services in servicesByWorkspace.items():
        try:
            workspacePath = config['sdeWorkspaces'][workspaceName]
            logger.info(f"Publishing B copies from workspace '{workspaceName}': {workspacePath}")
            
            egdbExport = egdbToFGDB.egdbToFGDB(workspacePath, outputWorkspace, services, logger, table_logger)
            egdbExport.Execute()
            
            printDict = publishFGDBs("B", outputDirectory, printDict)
            
        except KeyError:
            logger.error(f"SDE workspace '{workspaceName}' not found in config. Skipping B copies for services: {list(services.keys())}")
            continue
        except Exception as e:
            logger.error(f"Error processing B copies for workspace '{workspaceName}': {str(e)}")
            continue

    print(printDict)

    endTime = time.time()
    totalTime = endTime - startTime
    minutes, seconds = divmod(totalTime, 60)
    logger.info(f"############ DONE WITH ALL B COPIES IN {int(minutes)} MINUTES and {int(seconds)} SECONDS ############")

    cleanOutputWorkspace(outputWorkspace)
    logger.info(f"COMPLETE")
    return printDict


def reloadPackage(root_module):
    package_name = root_module.__name__

    # get a reference to each loaded module
    loaded_package_modules = dict([
        (key, value) for key, value in sys.modules.items() 
        if key.startswith(package_name) and isinstance(value, types.ModuleType)])

    # delete references to these loaded modules from sys.modules
    for key in loaded_package_modules:
        del sys.modules[key]

    # load each of the modules again; 
    # make old modules share state with new modules
    for key in loaded_package_modules:
        print('loading %s' % key)
        newmodule = __import__(key)
        oldmodule = loaded_package_modules[key]
        oldmodule.__dict__.clear()
        oldmodule.__dict__.update(newmodule.__dict__)


def getDashboardLayer(config, org_gis):
    '''Purpose: Get and return the output layer object
     Inputs:
          config: json config object
     Output: class instance layer object
     '''
    search_results = org_gis.content.get(config['agol']['dashboardTableItemId'])
    out_layer = search_results.tables[config['agol']['dashboardTableLayerIndex']]
    return out_layer

def writeDashboardLogs(dashboardLayer, datetimestamp, allServicesInfo, logger):
    for service in allServicesInfo:
        try:
            statusMessage = "Error" if allServicesInfo[service]['logs'] != "Success" else "Success"
            addRecord = {'attributes':
                            {'service': service,
                            'viewlink': allServicesInfo[service]['viewURL'],
                            'status': statusMessage,
                            'datetimestamp': datetimestamp,
                            'updateinterval': allServicesInfo[service]['updateinterval'],
                            'fgdbminutes': allServicesInfo[service]['egdbToFGDBMinutes'],
                            'swapminutes': allServicesInfo[service]['swapMinutes'],
                            'logs': allServicesInfo[service]['logs']
                            }
                        }
            success = dashboardLayer.edit_features(adds=[addRecord])
            logger.info(f'Adding dashboard logs for {service}: {success}')
        except Exception as e:
            logger.error(f"Error adding dashboard logs for {service}: {str(e)}")
    logger.info('Finished writing dashboard logs')

def loadLayersConfig(logger, config, onlyPublish):
    """Load and filter service definitions from the layer spreadsheet.

    Builds the `allServicesInfo` structure used by export/swap steps, applying
    skip and interval filters (unless running in publish mode).
    """
    logger.info('Reading layers config')
    table_logger.log('Reading layers config')
    # open input file and read it line by line
    fileLocation = config['layersSpreadsheet']
    FileList = open (fileLocation , 'r') # the w will overwrite the existing file (a would append)
    FileInput = FileList.readline()
    #this will cause the fist line to be skipped
    FileInput = FileList.readline()
    allServicesInfo = {}
    serviceNameList = []
    while FileInput:
        try:
            List = re.split(',',FileInput, maxsplit=0)
            serviceName = List[0]
            featureClass = List[1]
            sublayerID = List[3]
            updateinterval = List[4]
            skip = List[5]
            viewID = List[6]
            serviceID_A = List[7]
            serviceID_B = List[8].strip()
            sdeWorkspace = List[9].strip() if len(List) > 9 and List[9].strip() else 'default'
            
            if not (featureClass or serviceName):
                FileInput = FileList.readline()
                continue
            if skip == "Y":
                FileInput = FileList.readline()
                continue
            if not onlyPublish and updateinterval != activeInterval:
                FileInput = FileList.readline()
                continue
                
            # Validate SDE workspace exists in config
            if sdeWorkspace not in config['sdeWorkspaces']:
                logger.warning(f"SDE workspace '{sdeWorkspace}' not found in config for service '{serviceName}'. Available workspaces: {list(config['sdeWorkspaces'].keys())}. Skipping.")
                FileInput = FileList.readline()
                continue
                
            if not allServicesInfo.get(serviceName):
                allServicesInfo[serviceName] = {}
                allServicesInfo[serviceName]['layerList'] = []
                allServicesInfo[serviceName]['viewID'] = viewID
                allServicesInfo[serviceName]['viewURL'] = ""
                allServicesInfo[serviceName]['serviceID_A'] = serviceID_A
                allServicesInfo[serviceName]['serviceID_B'] = serviceID_B
                allServicesInfo[serviceName]['updateinterval'] = updateinterval
                allServicesInfo[serviceName]['logs'] = "Success"
                allServicesInfo[serviceName]['egdbToFGDBMinutes'] = -1
                allServicesInfo[serviceName]['swapMinutes'] = -1
                allServicesInfo[serviceName]['stalled'] = False
                allServicesInfo[serviceName]['sdeWorkspace'] = sdeWorkspace
                serviceNameList.append(serviceName)
            allServicesInfo[serviceName]['layerList'].append({
                "featureClass": featureClass,
                "sublayerID": sublayerID
            })
            FileInput = FileList.readline()
        except Exception as e:
            logger.error(f"Error processing layers config line {FileInput}: {str(e)}")
    logger.info('Finished reading layers config')
    return allServicesInfo, serviceNameList

def overwriteThreadFunc(OverwriteFS_working, SwapDataTest_VW, sourcefile, Service_Prop_Backup_Folder, outcomeContainer):
    """Run a single OverwriteFS swap call in a thread-safe wrapper.

    Appends either the swap outcome or an exception into `outcomeContainer` so
    the caller can enforce timeout/retry logic.
    """
    try:
        outcome = OverwriteFS_working.swapFeatureViewLayers(
            SwapDataTest_VW, updateFile=sourcefile, touchItems=False, verbose=True,
            touchTimeSeries=False, outcome=None, noIndexes=False, preserveProps=False,
            noWait=False, noProps=True, converter=None, outPath=Service_Prop_Backup_Folder,
            dryRun=False, noSwap=False, ignoreAge=False
        )
        outcomeContainer.append(outcome)
    except Exception as e:
        outcomeContainer.append(e)

def swap_worker(args):
    """Process one service ZIP through target selection and view swap.

    For a single service, this worker resolves the inactive A/B target, renames
    the ZIP to expected overwrite filename, executes swap with retries/timeouts,
    and returns status/timing details.
    """
    import os
    import sys
    import time
    import logging
    import keyring
    from arcgis import GIS
    from tableLogger import TableLogger
    config = args[1]
    overwrite_dir = os.path.abspath(config['overwriteFSDirectory'])
    if overwrite_dir not in sys.path:
        sys.path.insert(0, overwrite_dir)
    import OverwriteFS_working

    (filename, config, allServicesInfo_entry, Service_Prop_Backup_Folder, outputWorkspace, logLevel) = args
    result = {}
    try:
        startTime = time.time()
        serviceName = filename.split('.')[0]
        network_addr=f"{config['agol']['profile']}@arcgis_python_api_profile_passwords"
        pwd = keyring.get_password(network_addr, config['agol']['profile'])
        orgGIS = GIS(url=config['agol']['url'], username=config['agol']['username'], password=pwd)

        # Create table_logger instance for this worker process
        table_logger = TableLogger(orgGIS, config['agol'].get('logTableItemId'))
        table_logger.log(f"Starting swap for {serviceName}")
        SwapDataTest_VW = orgGIS.content.get(allServicesInfo_entry['viewID'])
        viewURL = orgGIS.url + f"/home/item.html?id={allServicesInfo_entry['viewID']}#overview"
        relitems = [allServicesInfo_entry['serviceID_A'], allServicesInfo_entry['serviceID_B']]

        # Update relationships and get target
        OverwriteFS_working.updateRelationships(SwapDataTest_VW, relateIds=relitems, unRelate=False, verbose=True, outcome=None, dryRun=False)
        outcome = OverwriteFS_working.getFeatureServiceTarget(SwapDataTest_VW, verbose=None, outcome=None, ignoreDataItemCheck=False)
        sd_fname = outcome["filename"]
        serviceZipName, extention = os.path.splitext(sd_fname)
        os.rename(os.path.join(outputWorkspace, filename), os.path.join(outputWorkspace, f"{serviceZipName}.zip"))
        sourcefile = os.path.join(outputWorkspace, f"{serviceZipName}.zip")

        # Perform overwrite with retries
        retries = 3
        for attempt in range(1, retries + 1):

            outcomeContainer = []
            overwriteThread = threading.Thread(
                target=overwriteThreadFunc,
                args=(OverwriteFS_working, SwapDataTest_VW, sourcefile, Service_Prop_Backup_Folder, outcomeContainer)
            )
            overwriteThread.start()
            overwriteThread.join(timeout=60*config.get('overwriteTimeoutMinutes', 60))  # Use config value or default to 60 minutes
            if overwriteThread.is_alive():
                result['stalled'] = True
                if attempt == retries:
                    raise Exception("Overwrite stalled")
                table_logger.log(f"Swap stalled for {serviceName}, attempt {attempt}/{retries}")
                continue
            
            outcome = outcomeContainer[0]
            if isinstance(outcome, Exception):
                if attempt == retries:
                    raise outcome
                table_logger.log(f"Exception during swap for {serviceName}, attempt {attempt}/{retries}: {str(outcome)}")
                continue
            
            if outcome["success"]:
                result['logs'] = "Success"
                table_logger.log(f"Successfully swapped {serviceName}")
                break
            elif outcome["success"] == False:
                if attempt == retries:
                    result['logs'] = str(outcome["items"][-1]["result"])
                    table_logger.log(f"Error swapping {serviceName}: {result['logs']}")
                else:
                    table_logger.log(f"Swap failed for {serviceName}, attempt {attempt}/{retries}: {str(outcome['items'][-1]['result'])}")
                    continue
        endTime = time.time()
        minutes = int((endTime - startTime)/60)
        result['swapMinutes'] = minutes
        result['viewURL'] = viewURL
    except Exception as e:
        result['logs'] = str(e) + "- try checking view and service IDs in the csv"
        result['swapMinutes'] = -1
        result['viewURL'] = ""
    return (serviceName, result)

if __name__ == '__main__':
    try:
        multiprocessing.set_start_method("spawn", force=True)
        logDir = os.path.join(scriptDir, "logs")
        if not os.path.exists(logDir):
            os.makedirs(logDir)

        logger = logging.getLogger()
        logLevel = logging.INFO
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
        logger.root.setLevel(logLevel)

        # log to file
        file_handler = logging.FileHandler(os.path.join(logDir, f"{curr_time}.log"))
        logfile = os.path.join(logDir, f"{curr_time}.log")
        file_handler.setLevel(logLevel)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # additionally log to command line
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logLevel)
        stdout_handler.setFormatter(formatter)
        logger.addHandler(stdout_handler)

        logger.info('Started')
        table_logger.log('Script started')
        config = loadScriptConfig()

        outputWorkspace = os.path.join(scriptDir, fgdbWorkspaceName)
        cleanOutputWorkspace(outputWorkspace)
    except Exception as e:
        logger.error(f"Error during startup: {str(e)}")
        sys.exit(1)

    try:
        allServicesInfo, serviceNameList = loadLayersConfig(logger, config, onlyPublish)
    except Exception as e:
        logger.error(f"Error during initialization: {str(e)}")
        sendEmail({}, config, logfile, "Error during initialization")
        sys.exit(1)
    #logger.info(f'Script config: {allServicesInfo}')
    if onlyPublish:
        printDict = publishCopies(config, allServicesInfo, logger, None, outputWorkspace)
        printIDs(printDict, serviceNameList)
        exit(0)

    try:
        # Group services by their SDE workspace for processing
        servicesByWorkspace = {}
        for serviceName, serviceInfo in allServicesInfo.items():
            workspace = serviceInfo.get('sdeWorkspace', 'default')
            if workspace not in servicesByWorkspace:
                servicesByWorkspace[workspace] = {}
            servicesByWorkspace[workspace][serviceName] = serviceInfo

        # Process each workspace separately
        for workspaceName, services in servicesByWorkspace.items():
            try:
                workspacePath = config['sdeWorkspaces'][workspaceName]
                logger.info(f"Processing services from workspace '{workspaceName}': {workspacePath}")
                
                egdbExport = egdbToFGDB.egdbToFGDB(workspacePath, outputWorkspace, services, logger, table_logger)
                # Update allServicesInfo with results from this workspace
                workspaceResults = egdbExport.Execute()
                for serviceName, serviceInfo in workspaceResults.items():
                    allServicesInfo[serviceName] = serviceInfo
                    
            except KeyError:
                logger.error(f"SDE workspace '{workspaceName}' not found in config. Skipping services: {list(services.keys())}")
                for serviceName in services.keys():
                    allServicesInfo[serviceName]['logs'] = f"Error - SDE workspace '{workspaceName}' not found in config"
                continue
            except Exception as e:
                logger.error(f"Error processing workspace '{workspaceName}': {str(e)}")
                for serviceName in services.keys():
                    allServicesInfo[serviceName]['logs'] = f"Error processing SDE workspace: {str(e)}"
                continue
                
    except Exception as e:
        logger.error(f"Error exporting SDE to FGDB: {str(e)}")
        sendEmail(allServicesInfo, config, logfile, "Error exporting SDE to FGDB")
        exit(1)


    logger.info('#################### START SWAP ####################')
    table_logger.log('Starting overwrite/swap process')
    #logger.info(f'allServicesInfo: {allServicesInfo}')


    #################### START SWAP ####################
    sys.path.insert(0, config['overwriteFSDirectory'])
    Service_Prop_Backup_Folder = config['swapZipBackupDirectory']

    import OverwriteFS_working
    reloadPackage(OverwriteFS_working)
    outputDirectory = os.fsencode(outputWorkspace)

    try:
        # Prepare arguments for each swap task
        swap_args = []
        for file in os.listdir(outputDirectory):
            filename = os.fsdecode(file)
            if not filename.endswith(".zip"):
                continue
            serviceName = filename.split('.')[0]
            swap_args.append((
                filename,
                config,
                allServicesInfo[serviceName],
                Service_Prop_Backup_Folder,
                outputWorkspace,
                logLevel
            ))

        # Use multiprocessing Pool for parallel swaps
        max_processes = config.get("maxProcesses", 4)  
        with multiprocessing.Pool(processes=max_processes) as pool:
            results = pool.map(swap_worker, swap_args)

        # Update allServicesInfo with results from workers
        logger.info('All swap processes completed')
        table_logger.log('All swap processes completed')

        for serviceName, result in results:
            allServicesInfo[serviceName]['logs'] = result.get('logs', '')
            allServicesInfo[serviceName]['swapMinutes'] = result.get('swapMinutes', -1)
            allServicesInfo[serviceName]['viewURL'] = result.get('viewURL', '')
            if 'stalled' in result:
                allServicesInfo[serviceName]['stalled'] = result['stalled']
    except Exception as e:
        logger.error(f"Uncaught error during swap: {str(e)}")
        sendEmail(allServicesInfo, config, logfile, "Uncaught error during swap")
        sys.exit(1)

    try:
        dashboardLayer = getDashboardLayer(config, orgGIS)
        writeDashboardLogs(dashboardLayer, datetime.datetime.now(), allServicesInfo, logger)
    except Exception as e:
        logger.error(f"Error getting dashboard layer: {str(e)}")
    
    # Only send email if any service has logs not equal to "Success"
    if any(info['logs'] != "Success" for info in allServicesInfo.values()):
        sendEmail(allServicesInfo, config, logfile, "Encountered Errors -- See Attached Log For More Information.")
    cleanOutputWorkspace(outputWorkspace)
    table_logger.clean_old_logs()
    sys.exit(0)
