# -*- coding: utf-8 -*-

import arcpy
import os

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
        self.label = "Update Tool"
        self.description = "Tool to update feature classes based on different rules."
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        params = []

        # Input parameter for Python script file
        python_script_param = arcpy.Parameter(
            displayName="Python Script File",
            name="python_script",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
        python_script_param.filter.list = ['py']  # allow .py files only
        params.append(python_script_param)

        # Input parameter for Target Database
        target_db_param = arcpy.Parameter(
            displayName="Target Database",
            name="target_db",
            datatype="DEWorkspace",
            parameterType="Required",
            direction="Input")
        params.append(target_db_param)

        return params

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
        python_script = parameters[0].valueAsText
        target_db = parameters[1].valueAsText

        # Log the input parameters
        arcpy.AddMessage(f"Python script: {python_script}")
        arcpy.AddMessage(f"Target database: {target_db}")

        # User confirmation prompt
        #if arcpy.GetActiveWindow():
           # response = arcpy.GetActiveWindow().MessageBox("Do you want to proceed with updating the data?", "Confirm", 4)
            #if response != 6:  # 6 means "Yes"
                #arcpy.AddMessage("Operation cancelled by user.")
                #return
            
        # Read the Python script file
        with open(python_script, 'r') as file:
            script_content = file.read()

        # Create a log file
        #log_file_path = os.path.join(os.path.dirname(python_script), 'update_log.txt')
        #with open(log_file_path, 'w') as log_file:
            #log_file.write("Log of updates:\n")
            #log_file.write("=================\n")

        # Execute the script content in the current namespace with the log file
        exec(script_content, {'arcpy': arcpy, 'target_db': target_db})

        arcpy.AddMessage(f"Script executed successfully.")

        return
