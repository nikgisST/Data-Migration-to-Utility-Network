

# -*- coding: utf-8 -*-

import arcpy


class Toolbox:
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Toolbox"
        self.alias = "toolbox"

        # List of tool classes associated with this toolbox
        self.tools = [Tool]


class Tool:
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Copy_Field_Tool"
        self.description = ""

    def getParameterInfo(self):
        """Define the tool parameters."""
        params = []

        # Input parameters
        params.append(arcpy.Parameter(
            displayName="Template Database",
            name="template_db",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Input"))

        params.append(arcpy.Parameter(
            displayName="Target Database",
            name="target_db",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Input"))

        return params

    def isLicensed(self):
        """Set whether the tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        # Extract the template and target database paths from the parameters
        template_db_path = parameters[0].valueAsText
        target_db_path = parameters[1].valueAsText

        # Check if the user has input the same path for both the template and target databases
        if template_db_path == target_db_path:
            arcpy.AddError("Error: The template and target database paths must be different.")
            raise arcpy.ExecuteError("Template and target database paths cannot be the same.")

        # Split the template and target database paths to compare their components
        template_db_partial_path = template_db_path.split("\\")
        target_db_partial_path = target_db_path.split("\\")

        # Initialize common path list
        common_path = []

        # Check if the last element of both paths is the same
        if template_db_partial_path[-1] == target_db_partial_path[-1]:
            common_path.append(template_db_partial_path[-1])

        elif template_db_partial_path[-1] != target_db_partial_path[-1]:
        # If not, raise an error and stop execution
            arcpy.AddError("Error: The feature class in the template and target database must be the same.")
            raise arcpy.ExecuteError("Template and target database paths do not match in the last element of their paths.")

        # Get existing fields in the target database
        existing_fields = [field.name for field in arcpy.ListFields(target_db_path)]

        # List to keep track of duplicate fields
        duplicate_fields = []

        # Iterate through fields and store properties in a dictionary
        fields = arcpy.ListFields(template_db_path)
        for field in fields:
            if field.name not in ['OBJECTID', 'Shape']:
                if field.name in existing_fields:
                    # If field already exists in target, add to the list of duplicates
                    duplicate_fields.append(field.name)
                    continue

                field_name = field.name
                field_type = field.type
                field_length = field.length
                arcpy.AddField_management(target_db_path, field_name, field_type, field_length=field_length)

        # if duplicate_fields:
        #     duplicate_fields_str = ", ".join(duplicate_fields)
        #     arcpy.AddError(f"Error: The following field/s already exist/s in the target database: {duplicate_fields_str}.")
        #     raise arcpy.ExecuteError(f"Attempted to add existing field/s: {duplicate_fields_str}.")
        #
        #     arcpy.AddWarning(f"Warning: The field/s {duplicate_fields_str} already exist/s in the target database. Field/s will not be overwritten.")

        if duplicate_fields:
            duplicate_fields_str = ", ".join(duplicate_fields)
            field_word = "field" if len(duplicate_fields) == 1 else "fields"
            exist_word = "exists" if len(duplicate_fields) == 1 else "exist"
            arcpy.AddWarning(f"Warning: The {field_word} {duplicate_fields_str} already {exist_word} in the target database. {field_word.capitalize()} will not be overwritten.")

        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return





