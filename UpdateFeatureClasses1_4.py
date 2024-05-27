import arcpy
#import re

#def check_field_existence(feature_class, field_name):
    #"""
    #Check if a specified field exists within a given feature class.
    #Parameters:
    #feature_class (str): Path to the feature class.
    #field_name (str): Name of the field to check.
    #"""
    #fields = [field.name for field in arcpy.ListFields(feature_class)]
    #if field_name not in fields:
       # print(f"Field {field_name} doesn't added to {feature_class}.")
    #else:
       # print(f"Field {field_name} already exists in {feature_class}.")

def update_fc_from_dict(source_fc, destination_fc, source_key_field, destination_key_field, field_pairs, where_clause):
    """
    Update fields in a destination feature class based on values from a source feature class.
    Parameters:
    source_fc (str): Path to the source feature class.
    destination_fc (str): Path to the destination feature class.
    source_key_field (str): Key field in the source feature class.
    destination_key_field (str): Key field in the destination feature class.
    field_pairs (list of tuples): List of field pairs (source field, destination field).
    where_clause (str): SQL where clause for filtering records.
    """
    source_fields = [pair[0] for pair in field_pairs]
    destination_fields = [pair[1] for pair in field_pairs]

    fields_to_retrieve = [source_key_field] + source_fields

    origin_fc_dict = {}

    with arcpy.da.SearchCursor(source_fc, fields_to_retrieve) as cursor:
        for row in cursor:
            key = row[0]
            origin_fc_dict[key] = {source_fields[i]: row[i + 1] for i in range(len(source_fields))}

    fields_to_update = [destination_key_field] + destination_fields

    with arcpy.da.UpdateCursor(destination_fc, fields_to_update, where_clause) as cursor:
        for row in cursor:
            common_guid = row[0]
            if common_guid in origin_fc_dict:
                related_data = origin_fc_dict[common_guid]
                for i in range(len(destination_fields)):
                    row[i+1] = related_data[field_pairs[i][0]]
                cursor.updateRow(row)
            else:
                arcpy.AddWarning(f"No related data found in feature class {source_fc} with key value {common_guid} from the destination feature class {destination_fc}")

def update_fc_within(inner_fc, outer_fc):
    """
    Update a field in the inner feature class to 'Station' if its polygon is within any polygon of the outer feature class.
    Parameters:
    inner_fc (str): Path to the inner feature class.
    outer_fc (str): Path to the outer feature class.
    """
    outer_polygons = [row[0] for row in arcpy.da.SearchCursor(outer_fc, 'SHAPE@')]

    with arcpy.da.UpdateCursor(inner_fc, ['SHAPE@', 'MIG_PARENTTYPE']) as cursor:
        for row in cursor:
            inner_polygon = row[0]
            within_outer = any(inner_polygon.within(outer) for outer in outer_polygons)
            if within_outer:
                row[1] = 'Station'
                cursor.updateRow(row)

def update_fc_self(source_fc, field_updates):
    """
    Updates fields within the same feature class based on a list of field pairs.
    Parameters:
    source_fc (str): Path to the feature class.
    field_updates (list of tuples): Each tuple contains the original field and the new fields to populate.
    """
    fields = [item for sublist in field_updates for item in sublist]
    fields_populated = {field: False for field in fields}
    with arcpy.da.UpdateCursor(source_fc, fields) as cursor:
        for row in cursor:
            for update_sub_tuple in field_updates:
                source_field = update_sub_tuple[0]
                source_index = fields.index(source_field)
                source_value = row[source_index]
                for each_target_field in update_sub_tuple[1:]:
                    target_index = fields.index(each_target_field)
                    if row[target_index] not in [None, '', 0]:
                        if not fields_populated[each_target_field]:
                            print(f"The field '{each_target_field}' in the feature class {source_fc} already contains data. Any existing data will be overwritten.")
                            fields_populated[each_target_field] = True
                    if 'TEXT' in each_target_field:
                        row[target_index] = str(source_value)
                    else:
                        row[target_index] = source_value
            cursor.updateRow(row)
    print("Self-update completed successfully.")


def update_voltage_from_multiple_sources(target_fc, source_layers, target_voltage_field, source_voltage_field):
    """
    Updates voltage field in a target feature class from multiple source layers based on spatial intersection.
    Parameters:
    target_fc (str): Path to the target feature class.
    source_layers (list): List of source feature class paths.
    target_voltage_field (str): Field in the target feature class to update.
    source_voltage_field (str): Field in the source feature classes containing voltage values.
    """
    target_layer = "TargetLayer"
    arcpy.management.MakeFeatureLayer(target_fc, target_layer)
    voltage_updates = {}
    for source_fc in source_layers:  # Process each source layer; the first in the list has the highest priority
        source_layer = "SourceLayer"
        arcpy.management.MakeFeatureLayer(source_fc, source_layer)
        arcpy.management.SelectLayerByLocation(target_layer, "INTERSECT", source_layer, selection_type="NEW_SELECTION")
        with arcpy.da.SearchCursor(target_layer, ["OBJECTID", target_voltage_field]) as target_cursor:  # Gather voltage values from intersecting source features
            intersecting_ids = {row[0]: row[1] for row in target_cursor}
        with arcpy.da.SearchCursor(source_layer, [source_voltage_field]) as source_cursor:
            for voltage, in source_cursor:
                for target_id in intersecting_ids:  # Update target features that intersect current source feature
                    if target_id not in voltage_updates:
                        voltage_updates[target_id] = voltage  # Overwrite voltage values regardless of previous updates
        arcpy.management.Delete(source_layer)  # Clean up source layer
    if voltage_updates:
        with arcpy.da.UpdateCursor(target_layer, ["OBJECTID", target_voltage_field]) as target_cursor:  # Update the target feature class with the collected voltage values
            for row in target_cursor:
                if row[0] in voltage_updates and (row[1] is None or row[1] == ''):
                    row[1] = voltage_updates[row[0]]  # Use the voltage value stored
                    target_cursor.updateRow(row)
    print("Voltage update completed.")
    arcpy.management.Delete(target_layer)

def update_field_based_on_whether_it_lies(target_fc, join_layers, value_field, value_map):
    """
    Update a field in the target feature class based on spatial relationships with multiple join layers.
    Parameters:
    target_fc (str): Path to the target feature class.
    join_layers (dict): Dictionary of join layers and the values to assign when a spatial relationship is met.
    value_field (str): Field in the target feature class to update.
    value_map (dict): Mapping of join layer keys to values to assign.
    """
    updated_features = set()
    for join_fc, value in join_layers.items():
        temp_el_junction_layer = "Temp_ElJunctionLayer"
        temp_join_layer = "Temp_JoinLayer"
        arcpy.management.MakeFeatureLayer(target_fc, temp_el_junction_layer)
        arcpy.management.MakeFeatureLayer(join_fc, temp_join_layer)
        arcpy.management.SelectLayerByLocation(temp_el_junction_layer, "INTERSECT", temp_join_layer)

        with arcpy.da.UpdateCursor(temp_el_junction_layer, [value_field, value_map]) as cursor:
            local_count = 0
            for row in cursor:
                if row[1] not in updated_features:
                    row[0] = value
                    cursor.updateRow(row)
                    updated_features.add(row[1])
                    local_count += 1
            print(f"Updated {local_count} features in '{target_fc}' with the value '{value}' for '{value_field}'.")

        arcpy.management.Delete(temp_el_junction_layer)
        arcpy.management.Delete(temp_join_layer)

    total_updated_features = len(updated_features)
    print(f"Total updated features in all categories: {total_updated_features}")

def update_line_fc_within_station_boundary(line_fc, station_fc, field_name='LINE_STATUS', field_type='TEXT', field_length=15):
    """
    Update line feature class based on whether lines are within or partially within the boundaries of station polygons.
    Parameters:
    line_fc (str): Path to the line feature class.
    station_fc (str): Path to the station feature class.
    field_name (str): The name of the field to add or check, default is 'LINE_STATUS'.
    field_type (str): The data type of the field, default is 'TEXT'.
    field_length (int): The length of the field if it is a 'TEXT' type, default is 15.
    """
    fields = [field.name for field in arcpy.ListFields(line_fc)]
    field_added = False
    if field_name not in fields:
        arcpy.AddField_management(line_fc, field_name, field_type, field_length=field_length)
        print(f"Field '{field_name}' was added in {line_fc}.")
        field_added = True
    else:
        print(f"Field '{field_name}' already exists in {line_fc}.")
    station_dict = {row[0]: row[1] for row in arcpy.da.SearchCursor(station_fc, ['GLOBALID', 'SHAPE@'])}
    with arcpy.da.UpdateCursor(line_fc, ['SHAPE@', 'MIG_STATIONGUID', 'LINE_STATUS']) as cursor:
        for row in cursor:
            line_geom = row[0]
            status_updated = False
            for station_global_id, station_polygon in station_dict.items():
                if line_geom.within(station_polygon):
                    row[1] = station_global_id
                    row[2] = 'Inside'
                    cursor.updateRow(row)
                    status_updated = True
                    break
                elif not line_geom.disjoint(station_polygon):
                    row[1] = station_global_id
                    row[2] = 'Partly Inside'
                    cursor.updateRow(row)
                    status_updated = True
                    break
            if not status_updated:
                row[1] = None
                row[2] = 'Outside'
                cursor.updateRow(row)

    if field_added:
        arcpy.DeleteField_management(line_fc, field_name)
        print(f"Field '{field_name}' was deleted from {line_fc}.")


def update_point_fc_within_station_boundary(point_fc, station_fc, field_name='POINT_STATUS', field_type='TEXT', field_length=15):
    """
    Updates point feature class based on spatial relationships with station boundaries.
    Points can be inside, on the boundary, or outside station polygons.
    Parameters:
    point_fc (str): Path to the point feature class.
    station_fc (str): Path to the station feature class.
    field_name (str): The name of the field to add or check, default is 'POINT_STATUS'.
    field_type (str): The data type of the field, default is 'TEXT'.
    field_length (int): The length of the field if it is a 'TEXT' type, default is 15.
    """
    fields = [field.name for field in arcpy.ListFields(point_fc)]
    field_added = False
    if field_name not in fields:
        arcpy.AddField_management(point_fc, field_name, field_type, field_length=field_length)
        print(f"Field '{field_name}' was added in {point_fc}.")
        field_added = True
    else:
        print(f"Field '{field_name}' already exists in {point_fc}.")
    station_dict = {row[0]: row[1] for row in arcpy.da.SearchCursor(station_fc, ['GLOBALID', 'SHAPE@'])}
    with arcpy.da.UpdateCursor(point_fc, ['SHAPE@', 'MIG_STATIONGUID', 'POINT_STATUS']) as cursor:
        for row in cursor:
            point_geom = row[0]
            status_updated = False
            for station_global_id, station_polygon in station_dict.items():
                if point_geom.within(station_polygon):
                    row[1] = station_global_id
                    row[2] = 'Inside'
                    cursor.updateRow(row)
                    status_updated = True
                    break
                elif point_geom.touches(station_polygon):
                    row[1] = station_global_id
                    row[2] = 'On Boundary'
                    cursor.updateRow(row)
                    status_updated = True
                    break
            if not status_updated:
                row[1] = None
                row[2] = 'Outside'
                cursor.updateRow(row)

    if field_added:
        arcpy.DeleteField_management(point_fc, field_name)
        print(f"Field '{field_name}' was deleted from {point_fc}.")


def check_relationship(source_path, global_id):
    """
    Check if a given GLOBALID exists in a specified field of a source feature class.
    Parameters:
    source_path (str): Path to the source feature class.
    global_id (str): GLOBALID to check for existence.
    """
    with arcpy.da.SearchCursor(source_path, ["CIRCUITBREAKER_GUID"]) as cursor:
        for row in cursor:
            if row[0] == global_id:
                return True
    return False

def update_mig_issource(circuit_breaker_fc, circuit_source_fc):
    """
    Updates the MIG_ISSOURCE field in the circuit breaker feature class based on voltage and relationships to a source feature class.
    Parameters:
    circuit_breaker_fc (str): Path to the circuit breaker feature class.
    circuit_source_fc (str): Path to the circuit source feature class.
    """
    fields = ["OPERATINGVOLTAGE", "SUBSOURCE", "MIG_ISSOURCE", "GLOBALID"]
    with arcpy.da.UpdateCursor(circuit_breaker_fc, fields) as cursor:
        for row in cursor:
            #voltage_str = row[0]
            #voltage_match = re.match(r'\d+', voltage_str)
            #voltage = int(voltage_match.group()) if voltage_match else 0
            subsource = row[1]
            global_id = row[3]
            relationship_exists = check_relationship(circuit_source_fc, global_id)
            if subsource == 1:  #voltage <= 1 and 
                row[2] = 2
            elif relationship_exists:  #1 < voltage <= 60 and 
                row[2] = 1
            else:    # high voltage 
                pass
            cursor.updateRow(row)

#print("Update completed successfully.")



def main():
    # Call your functions here
    switching_facility_path = r"D:\UN\set_DB\databases\GISRO_PILOT.gdb\SwitchingFacility"
    bay_path = r"D:\UN\set_DB\databases\GISRO_PILOT.gdb\Bay"
    field_pairs_bay = [("STATION_GUID", "MIG_STATIONGUID"), ("OPERATINGVOLTAGE", "MIG_VOLTAGE")]
    where_clause_bay1 = "SWITCHINGFACILITY_GUID IS NOT NULL"
    update_fc_from_dict(switching_facility_path, bay_path, "GLOBALID", "SWITCHINGFACILITY_GUID", field_pairs_bay, where_clause_bay1)

    bay_scheme_path = r"D:\UN\set_DB\databases\GISRO_PILOT.gdb\BayScheme"
    field_pairs_bayscheme = [("MIG_STATIONGUID", "MIG_STATIONGUID")]
    where_clause_bay_scheme = "BAY_GUID IS NOT NULL"
    update_fc_from_dict(bay_path, bay_scheme_path, "GLOBALID", "BAY_GUID", field_pairs_bayscheme, where_clause_bay_scheme)

    station_scheme_path = r"D:\UN\set_DB\databases\GISRO_PILOT.gdb\StationScheme"
    update_fc_within(bay_scheme_path, station_scheme_path)

    circuit_source_path = r"D:\UN\set_DB\databases\GISRO_PILOT.gdb\CircuitSource"
    field_pairs_circuit_source = [('OBJECTID', 'MIG_OID', 'MIG_OID_TEXT'), ('GLOBALID', 'MIG_GLOBALID')]
    update_fc_self(circuit_source_path, field_pairs_circuit_source)

    circuit_source_id_path = r"D:\UN\set_DB\databases\GISRO_PILOT.gdb\CircuitSourceID"
    field_pairs_circuit_source_id = [('OBJECTID', 'MIG_OID', 'MIG_OID_TEXT'), ('GLOBALID', 'MIG_GLOBALID')]
    update_fc_self(circuit_source_id_path, field_pairs_circuit_source_id)

    electric_net_junctions_path = r"D:\UN\set_DB\databases\GISRO_PILOT.gdb\Electric_Net_Junctions"
    station_boundary_fc_path = r"D:\UN\set_DB\databases\GISRO_PILOT.gdb\StationBoundary"
    update_point_fc_within_station_boundary(electric_net_junctions_path, station_boundary_fc_path)

    print("Update completed successfully.")

if __name__ == "__main__":
    main()