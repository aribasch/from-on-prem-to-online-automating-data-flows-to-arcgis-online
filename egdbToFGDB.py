import arcpy
import os
import datetime
from pathlib import Path
import zipfile
import shutil
import time

class egdbToFGDB():

    def __init__(self, sdeWorkspace, outputWorkspace, layersToExport, logger, table_logger) -> None:
        self.logger = logger
        self.table_logger = table_logger
        self.sdeWorkspace = sdeWorkspace
        self.outputWorkspace = outputWorkspace
        self.layersToExport = layersToExport
        self.ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    def __existsInAnyDataset(self, layer, fgdb):
        datasets = arcpy.ListDatasets(feature_type='feature')   
        datasets = [''] + datasets if datasets is not None else []
        for ds in datasets:
            if arcpy.Exists(os.path.join(fgdb, ds, layer)):
                return True
        return False
    
    def __addUnrelatedFCs(self, layerList, fgdb, service):
        for layer in layerList:
            fc = os.path.join(self.sdeWorkspace, layer['featureClass'])
            if not (arcpy.Exists(layer['featureClass']) or self.__existsInAnyDataset(layer['featureClass'], fgdb)):
                fc = os.path.join(self.sdeWorkspace, layer['featureClass'])
                if not arcpy.Exists(fc):
                    self.logger.error(f"Featureclass Not Found in database: {fc}")
                    continue
                arcpy.Copy_management(fc, os.path.join(fgdb, layer['featureClass']))

    def __removeUnwantedFCs(self, layerList, fgdb):
        datasets = arcpy.ListDatasets(feature_type='feature')
        datasets = [''] + datasets if datasets is not None else []
        for ds in datasets:
            for layer in arcpy.ListFeatureClasses(feature_dataset=ds):
                if not any(d['featureClass'] == layer for d in layerList) and "_dir" in fgdb:
                    arcpy.Delete_management(os.path.join(fgdb, layer))
        for table in arcpy.ListTables():
            if not any(d['featureClass'] == table for d in layerList) and "_dir" in fgdb:
                arcpy.Delete_management(os.path.join(fgdb, table))
        
    # export featureclass and related tables to file geodatabase
    def __exportToGDB(self, layerList, fc, fgdb, service):
        self.logger.info(f"Exporting Featureclass: {fc} to {fgdb}")
        self.table_logger.log(f"Exporting Featureclass: {fc} to {fgdb}")
        try:
            arcpy.CreateFileGDB_management(os.path.join(self.outputWorkspace, f"{service}_dir"), Path(fgdb).stem)
            arcpy.Copy_management(fc, os.path.join(fgdb, layerList[0]['featureClass']))
  
            self.__addUnrelatedFCs(layerList[1:], fgdb, service)
            self.__removeUnwantedFCs(layerList, fgdb)
            return True
        except Exception as e:
            self.logger.error(f"Failed to export {layerList[0]['featureClass']} to {fgdb}")
            self.table_logger.log(f"Failed to export {layerList[0]['featureClass']} to {fgdb}. Error: {e}")
            self.logger.error(e)
            self.layersToExport[service]['logs'] = f"Failed to export {layerList[0]['featureClass']} to {fgdb}. Error: {e}"
        
        return False

    # rename field with retry logic
    def __renameField(self, dataset, oldFieldName, newFieldName, newFieldAlias):
        # occassionally fails due to lock, retry 5 times
        retries = 5
        for attempt in range(1, retries + 1):
            try:
                arcpy.management.AlterField(dataset, oldFieldName, newFieldName, newFieldAlias)
                return True
            except Exception as e:
                self.logger.warn(f"Rename Editor Tracking Field Failed (Retrying): {dataset} -- {oldFieldName} -- {attempt}")
                if attempt == retries:
                    return False

    # disable and standardizes editor tracking fields
    def __updateEditorTracking(self, dataset):
        desc = arcpy.Describe(dataset)
        self.logger.info(f"Update Editor Tracking: {dataset}")

        if desc.editorTrackingEnabled:
            creatorFieldName = desc.creatorFieldName
            createdAtFieldName = desc.createdAtFieldName
            editorFieldName = desc.editorFieldName
            editedAtFieldName = desc.editedAtFieldName

            # Disable Editor Tracking
            arcpy.management.DisableEditorTracking(dataset, True, True, True, True)

            # Rename Fields
            if creatorFieldName:
                self.__renameField(dataset, creatorFieldName, 'int_created_user', 'Internal Created User')

            if createdAtFieldName:
                self.__renameField(dataset, createdAtFieldName, 'int_created_date', 'Internal Created Date')

            if editorFieldName:
                self.__renameField(dataset, editorFieldName, 'int_edited_user', 'Internal Edited User')

            if editedAtFieldName:
                self.__renameField(dataset, editedAtFieldName, 'int_edited_date', 'Internal Edited Date')
    def __copyGeometryFields(self, dataset):
        self.logger.info('Copying Geometry Fields')
        fields = arcpy.ListFields(dataset)
        lengthPresent = False
        lengthField = None
        areaPresent = False
        areaField = None
        for field in fields:
            #self.logger.info(f"{field.name} has a type of {field.type} with a length of {field.length}")
            if field.name == "SHAPE_Length" or field.name == "Shape_Length":
                lengthPresent = True
                lengthField = field.name
            if field.name == "SHAPE_Area" or field.name == "Shape_Area":
                areaPresent = True
                areaField = field.name
            #self.logger.info(f"{field.name} has a type of {field.type} with a length of {field.length}")
        if lengthPresent:
            self.logger.info('Attempting to add length field')
            arcpy.management.AddField(dataset, "StatePlane_Length", "DOUBLE")
            arcpy.management.CalculateField(dataset, "StatePlane_Length", f"!{lengthField}!", "PYTHON3")
        if areaPresent:
            self.logger.info('Attempting to add area field')
            arcpy.management.AddField(dataset, "StatePlane_Area", "DOUBLE")
            arcpy.management.CalculateField(dataset, "StatePlane_Area", f"!{areaField}!", "PYTHON3")

    # remove attribute rules for dataset
    def __removeAttributeRules(self, dataset):
        self.logger.info(f"Removing Attribute Rules for: {dataset}")
        try:
            desc = arcpy.Describe(dataset).attributeRules
            for rule in desc:
                    self.logger.info("Deleting rule: {} for upload".format(rule.name))
                    arcpy.management.DeleteAttributeRule(dataset, rule.name)
        except Exception as e:
            self.logger.error(f"Failed to remove attribute rules for {dataset}: {e}")

    def __zipFGDB(self, inputFileGeodatabase, zipFilePath):
        zip_folder = os.path.dirname(zipFilePath)
        publish_fgdb = os.path.join(zip_folder, os.path.basename(zipFilePath).replace('.zip','.gdb'))
        if os.path.exists(publish_fgdb):
            shutil.rmtree(publish_fgdb)
        shutil.copytree(
            inputFileGeodatabase,
            publish_fgdb,
            ignore=shutil.ignore_patterns("*.lock")
        )

        with zipfile.ZipFile(zipFilePath, "w") as zipper:
            for root, dirs, files in os.walk(publish_fgdb):
                for file in files:
                    fpath = os.path.join(root, file)
                    zpath = os.path.relpath(
                                os.path.join(root, file),
                                os.path.join(publish_fgdb, '..')
                            )
                    zipper.write(
                        fpath,
                        zpath
                    )
        shutil.rmtree(publish_fgdb) 

    def Execute(self):
        """Export configured layers to FGDB ZIPs and update timing/log metadata."""

        for service in self.layersToExport:
            startTime = time.time()
            fc = os.path.join(self.sdeWorkspace, self.layersToExport[service]['layerList'][0]['featureClass'])
            os.makedirs(os.path.join(self.outputWorkspace, f"{service}_dir"))

            self.logger.info(f"Processing: {service}")
            
            if not arcpy.Exists(fc):
                self.logger.error(f"Featureclass Not Found: {fc}")
                continue

            fgdb = os.path.join(self.outputWorkspace, f"{service}_dir", f"{service}.gdb")
            arcpy.env.workspace = fgdb
            arcpy.env.overwriteOutput = True

            # export data
            if self.__exportToGDB(self.layersToExport[service]['layerList'], fc, fgdb, service):

                # disable editor tracking and rename fields
                arcpy.env.workspace = fgdb
                # build a list of datasets
                allDatasets = []
                for dirpath, dirnames, datasets in arcpy.da.Walk(fgdb,
                    datatype=['FeatureClass', 'Table']):
                    allDatasets.extend(datasets)

                # disable editor tracking, rename fields, and remove attribute rules
                for dataset in allDatasets:
                    try:
                        self.__removeAttributeRules(dataset)
                        self.__updateEditorTracking(dataset)
                        self.__copyGeometryFields(dataset)
                        self.__removeAttributeRules(dataset)
                    except Exception as e:
                        self.logger.error(f"Error updating Editor Tracking or Length/Area fields for {dataset} - {str(e)}")
                        self.layersToExport[service]['logs'] = f"Error updating Editor Tracking or Length/Area fields for {dataset} - {str(e)}"
            else:
                self.logger.error(f"Failed to export {fc} to {fgdb}")
                break
            inputFileGeodatabase = os.path.join(self.outputWorkspace, f"{service}_dir", f"{service}.gdb")
            zipFilePath = os.path.join(self.outputWorkspace, f"{service}.zip")
            try:
                self.__zipFGDB(inputFileGeodatabase, zipFilePath)
                arcpy.ClearWorkspaceCache_management(inputFileGeodatabase)
                arcpy.Delete_management(inputFileGeodatabase)
                os.rmdir(os.path.join(self.outputWorkspace, f"{service}_dir"))
            except Exception as e:
                self.logger.error(f"Failed to zip {inputFileGeodatabase} to {zipFilePath} or clean up workspace: {e}")
                self.layersToExport[service]['logs'] = f"Failed to zip {inputFileGeodatabase} to {zipFilePath}. Error: {e}"
            endTime = time.time()
            minutes = int((endTime - startTime)/60)
            self.layersToExport[service]['egdbToFGDBMinutes'] = minutes
        return self.layersToExport

# if __name__ == "__main__":
#     Execute()