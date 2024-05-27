# -*- coding: utf-8 -*-

import arcpy
import csv


class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Toolbox"
        self.alias = "toolbox"

        # List of tool classes associated with this toolbox
        self.tools = [Tool]


class Tool(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Add Fields From CSV"
        self.description = ""
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        csv_file_path = arcpy.Parameter(
            displayName="Input CSV File",
            name="csv_file_path",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")

        target_DB = arcpy.Parameter(
            displayName="Target Geodatabase",
            name="target_DB",
            datatype="DEWorkspace",
            parameterType="Required",
            direction="Input")

        return [csv_file_path, target_DB]

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        csv_file_path = parameters[0].valueAsText
        target_DB = parameters[1].valueAsText
        arcpy.env.workspace = target_DB

        if csv_file_path == target_DB:
            arcpy.AddError("Error: The CSV file and target database paths must be different.")
            raise arcpy.ExecuteError("CSV file path and target database path cannot be the same.")

        # Function to add fields is defined here as before
        def add_fields_to_feature_class(feature_class, fields_info):

            if not arcpy.Exists(feature_class):
                raise OSError(f"The feature class {feature_class} does not exist.")

            duplicate_fields = []
            for field in fields_info:
                field_list = arcpy.ListFields(feature_class, field['Field Name'])
                if len(field_list) > 0:   # Check if the field exists already
                    duplicate_fields.append(field['Field Name'])
                else:
                #WAY 1
                    # arcpy.AddField_management(feature_class,
                    #                           field['Field Name'],
                    #                           field['Data Type'],
                    #                           field_length=field['Field Length'])
                    # arcpy.AddMessage(f"Field {field['Field Name']} added to {feature_class}.")
                #WAY 2
                    # arcpy.AddField_management(feature_class,
                    #                           field['Field Name'],
                    #                           field['Data Type'],
                    #                           field_length=field['Field Length'] if field['Data Type'].upper() == 'TEXT' else None)
                    # arcpy.AddMessage(f"Field {field['Field Name']} added to {feature_class}.")
                #WAY 3
                    if field['Data Type'].upper() == 'TEXT':
                        arcpy.AddField_management(feature_class,
                                                  field['Field Name'],
                                                  field['Data Type'],
                                                  field_length=field['Field Length'])
                                                  #field_length=field.get('Field Length', ''))
                    else:
                        arcpy.AddField_management(feature_class,
                                                  field['Field Name'],
                                                  field['Data Type'])
                    arcpy.AddMessage(f"Field {field['Field Name']} added to {feature_class}.")

            if duplicate_fields:
                duplicate_fields_str = ", ".join(duplicate_fields)
                field_word = "field" if len(duplicate_fields) == 1 else "fields"
                exist_word = "exists" if len(duplicate_fields) == 1 else "exist"
                arcpy.AddWarning(f"Warning: The {field_word} {duplicate_fields_str} already {exist_word} in {feature_class}. {field_word.capitalize()} have not been overwritten.")
                #arcpy.AddWarning(f"Warning: The {field_word} {duplicate_fields_str} already {exist_word} in {feature_class}. "
                         #f"{duplicate_fields_str} will not be overwritten.")

        # Read the CSV and add fields to feature classes
        fields_to_add = {}
        with open(csv_file_path, mode='r') as csv_file:    #encoding='utf-8-sig'
            csv_reader = csv.DictReader(csv_file)          #convert CSV file to Python dictionary
            # Loop through each row in the CSV
            for row in csv_reader:
                class_name = row['Class']   # еxtracting the fc name from a row --> Bay': [{'Class': 'Bay', 'Field Name': 'MIG_VOLTAGE', 'Data Type': 'Text', 'Field Length': '20'},{'Class': 'Bay', 'Field Name': 'MIG_STATIONGUID', 'Data Type': 'Text', 'Field Length': '50'}]
                if class_name not in fields_to_add:
                    fields_to_add[class_name] = []
                fields_to_add[class_name].append(row)

        # Iterate over the classes and add the new fields
        for feature_class, fields_info in fields_to_add.items():
            add_fields_to_feature_class(feature_class, fields_info)   #Call function to add fields to the current feature class
            arcpy.AddMessage(f"Fields added to {feature_class} successfully.")

        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return
