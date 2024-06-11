import arcpy

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
    edit = arcpy.da.Editor(arcpy.Describe(destination_fc).path)
    edit.startEditing(False, True)
    edit.startOperation()
    try:
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
                    pass
        edit.stopOperation()
        edit.stopEditing(True)  
    except Exception as e:
        edit.stopOperation()
        edit.stopEditing(False)
        print(f"Error during update: {e}")
        raise
    finally:
        arcpy.AddWarning(f"No related data found in feature class {source_fc} with key value {common_guid} from the destination feature class {destination_fc}")


def update_fc_within(inner_fc, outer_fc):
    """
    Update a field in the inner feature class to 'Station' if its polygon is within any polygon of the outer feature class.
    Parameters:
    inner_fc (str): Path to the inner feature class.
    outer_fc (str): Path to the outer feature class.
    """
    outer_polygons = [row[0] for row in arcpy.da.SearchCursor(outer_fc, 'SHAPE@')]
    edit = arcpy.da.Editor(arcpy.Describe(inner_fc).path)
    edit.startEditing(False, True)
    edit.startOperation()
    try:
        with arcpy.da.UpdateCursor(inner_fc, ['SHAPE@', 'MIG_PARENTTYPE']) as cursor:
            for row in cursor:
                inner_polygon = row[0]
                if inner_polygon is None:
                    print("Encountered a NoneType inner_polygon. Skipping this row.")
                    continue
                within_outer = any(inner_polygon.within(outer) for outer in outer_polygons if outer is not None)
                if within_outer:
                    row[1] = 'Station'
                    cursor.updateRow(row)
        edit.stopOperation()
        edit.stopEditing(True)  
    except Exception as e:
        edit.stopOperation()
        edit.stopEditing(False)
        print(f"Error during update: {e}")
        raise


def update_fc_self(source_fc, field_updates):
    """
    Updates fields within the same feature class based on a list of field pairs.
    Parameters:
    source_fc (str): Path to the feature class.
    field_updates (list of tuples): Each tuple contains the original field and the new fields to populate.
    """
    fields = [item for sublist in field_updates for item in sublist]
    fields_populated = {field: False for field in fields}
    edit = arcpy.da.Editor(arcpy.Describe(source_fc).path)
    edit.startEditing(False, True)
    edit.startOperation()
    try:
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
        edit.stopOperation()
        edit.stopEditing(True)  
    except Exception as e:
        edit.stopOperation()
        edit.stopEditing(False)
        print(f"Error during update: {e}")
        raise
    print("Self-update completed successfully.")


def update_mig_voltage(join_internal, join_conductor, join_busbar, dest_fc):
    """
    Updates the MIG_VOLTAGE field in the destination feature class based on spatial joins with InternalConnection, Conductor, and Busbar feature classes.
    Parameters:
    join_internal (str): Path to the feature layer resulting from the spatial join between Connector and InternalConnection.
    join_conductor (str): Path to the feature layer resulting from the spatial join between Connector and Conductor.
    join_busbar (str): Path to the feature layer resulting from the spatial join between Connector and Busbar.
    dest_fc (str): Path to the destination feature class where the MIG_VOLTAGE field will be updated.
    """
    def update_field(source_fc, destination_fc, source_globalid, operatingvoltage, dest_globalid, mig_voltage):
        """
        Updates the destination feature class field with values from the source feature class field.
        Parameters:
        source_fc (str): Path to the source feature class containing the original voltage data.
        destination_fc (str): Path to the destination feature class where the voltage data will be updated.
        source_globalid (str): Name of the GlobalID field in the source feature class.
        operatingvoltage (str): Name of the operating voltage field in the source feature class.
        dest_globalid (str): Name of the GlobalID field in the destination feature class.
        mig_voltage (str): Name of the field in the destination feature class to update with the migrated voltage.
        """
        fields_to_retrieve = [source_globalid, operatingvoltage]
        origin_fc_dict = {}
        edit = arcpy.da.Editor(arcpy.Describe(dest_fc).path)
        edit.startEditing(False, True)
        edit.startOperation()
        try:
            with arcpy.da.SearchCursor(source_fc, fields_to_retrieve) as cursor:
                for row in cursor:
                    key = row[0]
                    voltage = row[1]
                    if key and voltage:
                        origin_fc_dict[key] = voltage
            with arcpy.da.UpdateCursor(destination_fc, [dest_globalid, mig_voltage]) as cursor:
                for row in cursor:
                    common_guid = row[0]
                    if common_guid in origin_fc_dict:
                        row[1] = origin_fc_dict[common_guid]
                        cursor.updateRow(row)
        except Exception as e:
            edit.stopOperation()
            edit.stopEditing(False)
            print(f"Error during update: {e}")
            raise
        else:
            edit.stopOperation()
            edit.stopEditing(True)
        finally:
            print("MIG_VOLTAGE field updated successfully.")
    try:
        if join_internal:
            arcpy.analysis.SpatialJoin(dest_fc, "Свързваща линия", join_internal, "JOIN_ONE_TO_ONE", "KEEP_COMMON", None, "INTERSECT") #"Свързваща линия"=r"InternalConnection"
        if join_conductor:
            arcpy.analysis.SpatialJoin(dest_fc, "Проводник", join_conductor, "JOIN_ONE_TO_ONE", "KEEP_COMMON", None, "INTERSECT")  #"Проводник"="Conductor"
        if join_busbar:
            arcpy.analysis.SpatialJoin(dest_fc, "Шина", join_busbar, "JOIN_ONE_TO_ONE", "KEEP_COMMON", None, "INTERSECT")   #"Шина"=r"Busbar"
    except Exception as e:
        print(f"Error during spatial join: {e}")
        raise
    try:
        if join_internal:
            update_field(join_internal, dest_fc, "TARGET_FID", "OPERATINGVOLTAGE", "OBJECTID", "MIG_VOLTAGE")
        if join_conductor:
            update_field(join_conductor, dest_fc, "TARGET_FID", "OPERATINGVOLTAGE", "OBJECTID", "MIG_VOLTAGE")
        if join_busbar:
            update_field(join_busbar, dest_fc, "TARGET_FID", "OPERATINGVOLTAGE", "OBJECTID", "MIG_VOLTAGE")
        if join_internal:
            arcpy.management.Delete(join_internal)
        if join_conductor:
            arcpy.management.Delete(join_conductor)
        if join_busbar:
            arcpy.management.Delete(join_busbar)
        print("Temporary layers deleted.")
    except Exception as e:
        print(f"Error during field update: {e}")
        raise


def update_field_based_on_whether_it_lies(target_fc, mapping_layers, mig_parenttype, objectid):
    """
    Update a field in the target feature class based on spatial relationships with multiple join layers.
    Parameters:
    target_fc (str): Path to the target feature class.
    value_map (dict): Mapping of join layer keys to values to assign.
    objectid (str): Name of the OBJECTID field in the destination feature class.
    mig_parenttype (str): Name of the field in the destination feature class to update with the migrated parent.
    """
    updated_features = set()  # Set to track updated feature IDs
    for join_fc, value in mapping_layers.items():
        temp_join = "TempJoin"
        arcpy.analysis.SpatialJoin(target_features=target_fc, join_features=join_fc, out_feature_class=temp_join, join_operation="JOIN_ONE_TO_ONE", 
                                   join_type="KEEP_COMMON", field_mapping=None, match_option="INTERSECT", search_radius=None, distance_field_name=None)
        features_to_update = set()
        edit = arcpy.da.Editor(arcpy.Describe(target_fc).path)
        edit.startEditing(False, True)
        edit.startOperation()
        try:
            with arcpy.da.SearchCursor(temp_join, ['TARGET_FID']) as cursor:
                for row in cursor:
                    if row[0] not in updated_features:  
                        features_to_update.add(row[0])
            local_count = 0
            with arcpy.da.UpdateCursor(target_fc, [mig_parenttype, objectid]) as cursor:
                for row in cursor:
                    if row[1] in features_to_update:
                        row[0] = value
                        cursor.updateRow(row) 
                        updated_features.add(row[1])
                        local_count += 1
            edit.stopOperation()
            edit.stopEditing(True)  
        except Exception as e:
            edit.stopOperation()
            edit.stopEditing(False)
            print(f"Error during update: {e}")
            raise
        finally:
            print(f"Updated {local_count} features in '{target_fc}' with the value '{value}' for 'MIG_PARENTTYPE'.")
            if arcpy.Exists(temp_join):
                arcpy.management.Delete(temp_join)
    total_updated_features = len(updated_features)
    print(f"Total updated features in all categories: {total_updated_features}")

def classify_junction(subtypes, overhead_types, underground_types):
    if all(subtype in overhead_types for subtype in subtypes):
        return "Overhead Conductor"
    elif all(subtype in underground_types for subtype in subtypes):
        return "Underground Conductor"
    else:
        return "Overhead Conductor Underground Conductor"

def update_conductor_type(target_fc, conductor_fc, mig_parenttype, subtype_field):
    """
    Replace with 'Overhead Conductor', 'Underground Conductor', or 'Overhead Conductor Underground Conductor'based on intersecting values in the
    subtype_cd field of the Conductor feature class.
    Parameters:
    target_fc (str): Path to the target feature class.
    conductor_fc (str): Path to the Conductor feature class.
    mig_parenttype (str): Name of the field in the destination feature class to update.
    subtype_field (str): Name of the subtype field in the Conductor feature class.
    """
    temp_join = "TempJoinConductor"
    arcpy.analysis.SpatialJoin(target_features=target_fc, join_features=conductor_fc, out_feature_class=temp_join, join_operation="JOIN_ONE_TO_MANY", 
                               join_type="KEEP_COMMON", field_mapping=None, match_option="INTERSECT", search_radius=None, distance_field_name=None)
    junction_conductor_map = {}
    overhead_types = ["Въздушна линия ВН", "Въздушна линия СрН", "Въздушна линия НН", "Въздушна изолирана линия СрН", "Въздушна изолирана линия НН"]
    underground_types = ["Кабелна линия ВН", "Кабелна линия СрН", "Кабелна линия НН", "Земно въже ВН", "Земно въже СрН"]
    with arcpy.da.SearchCursor(temp_join, ['TARGET_FID', subtype_field]) as cursor:
        for row in cursor:
            target_fid = row[0]
            subtype_cd = row[1]
            if target_fid not in junction_conductor_map:
                junction_conductor_map[target_fid] = set()
            junction_conductor_map[target_fid].add(subtype_cd)
    updated_count = 0
    edit = arcpy.da.Editor(arcpy.Describe(target_fc).path)
    edit.startEditing(False, True)
    edit.startOperation()
    try:
        with arcpy.da.UpdateCursor(target_fc, [mig_parenttype, 'OBJECTID']) as cursor:
            for row in cursor:
                if row[0] == 'Conductor' and row[1] in junction_conductor_map:
                    subtype_set = junction_conductor_map[row[1]]
                    row[0] = classify_junction(subtype_set, overhead_types, underground_types)
                    cursor.updateRow(row)
                    updated_count += 1
        edit.stopOperation()
        edit.stopEditing(True)
    except Exception as e:
        edit.stopOperation()
        edit.stopEditing(False)
        print(f"Error during update: {e}")
        raise
    finally:
        print(f"Updated {updated_count} 'Conductor' features in '{target_fc}' with specific conductor types.")
        if arcpy.Exists(temp_join):
            arcpy.management.Delete(temp_join)


def update_line_fc_within_station_boundary(line_fc, station_fc, globalid_field, mig_stationguid_field, field_name='LINE_STATUS', field_type='TEXT', field_length=15):
    """
    Updates line feature class based on spatial relationships with station boundaries.Lines can be inside, on the boundary, or outside station polygons.
    Parameters:
    line_fc (str): Path to the line feature class.
    station_fc (str): Path to the station feature class.
    globalid_field (str): The field name for the station feature class global ID.
    mig_stationguid_field (str): The field name for the station GUID in the line feature class.
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
    station_dict = {row[0]: row[1] for row in arcpy.da.SearchCursor(station_fc, [globalid_field, 'SHAPE@'])}
    edit = arcpy.da.Editor(arcpy.Describe(line_fc).path)
    edit.startEditing(False, True)
    edit.startOperation()
    try:
        with arcpy.da.UpdateCursor(line_fc, ['SHAPE@', mig_stationguid_field, field_name]) as cursor:
            for row in cursor:
                line_geom = row[0]
                if line_geom is None:
                    print("Encountered a NoneType line geometry. Skipping this row.")
                    continue
                status_updated = False
                for station_global_id, station_polygon in station_dict.items():
                    if station_polygon is None:
                        continue
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
        edit.stopOperation()
        edit.stopEditing(True)  
    except Exception as e:
        edit.stopOperation()
        edit.stopEditing(False)
        print(f"Error during update: {e}")
        raise
    finally:
        if field_added:
            arcpy.DeleteField_management(line_fc, field_name)
            print(f"Field '{field_name}' was deleted from {line_fc}.")


def update_point_fc_within_station_boundary(point_fc, station_fc, globalid_field, mig_stationguid_field, field_name='POINT_STATUS', field_type='TEXT', field_length=15):
    """
    Updates point feature class based on spatial relationships with station boundaries.
    Points can be inside, on the boundary, or outside station polygons.
    
    Parameters:
    point_fc (str): Path to the point feature class.
    station_fc (str): Path to the station feature class.
    globalid_field (str): The field name for the station feature class global ID.
    mig_stationguid_field (str): The field name for the station GUID in the point feature class.
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
    station_dict = {row[0]: row[1] for row in arcpy.da.SearchCursor(station_fc, [globalid_field, 'SHAPE@'])}
    edit = arcpy.da.Editor(arcpy.Describe(point_fc).path)
    edit.startEditing(False, True)
    edit.startOperation()
    try:
        with arcpy.da.UpdateCursor(point_fc, ['SHAPE@', mig_stationguid_field, field_name]) as cursor:
            for row in cursor:
                point_geom = row[0]
                if point_geom is None:
                    print("Encountered a NoneType point geometry. Skipping this row.")
                    continue
                status_updated = False
                for station_global_id, station_polygon in station_dict.items():
                    if station_polygon is None:
                        continue
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
        edit.stopOperation()
        edit.stopEditing(True)  
    except Exception as e:
        edit.stopOperation()
        edit.stopEditing(False)
        print(f"Error during update: {e}")
        raise
    finally:
        if field_added:
            arcpy.DeleteField_management(point_fc, field_name)
            print(f"Field '{field_name}' was deleted from {point_fc}.")


def check_relationship(source_fc, global_id_column, global_id_value):
    """
    Parameters:
    Checks if a specific global ID value exists in a specified column of a feature class.
    source_fc (str): Path to the source feature class to search.
    global_id_column (str): Name of the column containing the global ID.
    global_id_value (str): The global ID value to search for.
    """
    with arcpy.da.SearchCursor(source_fc, [global_id_column]) as cursor:
        for row in cursor:
            if row[0] == global_id_value:
                return True
    return False

def update_mig_issource(dest_fc, source_fc=None, objectid_column=None, fields = None):
    """
    Parameters:
    Updates the MIG_ISSOURCE field in the destination feature class based on the value of SUBSOURCE
    and optionally checks a relationship with a source feature class.
    dest_fc (str): Path to the destination feature class to update.
    source_fc (str, optional): Path to the source feature class for relationship checking.
    global_id_column (str, optional): Name of the column containing the global ID in the source feature class.
    """
    warning_is_printed = False
    edit = arcpy.da.Editor(arcpy.Describe(dest_fc).path)
    edit.startEditing(False, True)
    edit.startOperation()
    try:
        with arcpy.da.UpdateCursor(dest_fc, fields) as cursor:
            for row in cursor:
                subsource = row[0]
                objectid_value = row[2]
                relationship_exists = False
                if source_fc and objectid_column:
                    try:
                        relationship_exists = check_relationship(source_fc, objectid_column, objectid_value)
                    except Exception as e:
                        if not warning_is_printed:
                            print(f"Warning: Could not check relationship for GLOBALID {objectid_value} in {dest_fc}. Error: {str(e)}")
                            warning_is_printed = True
                if relationship_exists:
                    row[1] = 1
                elif subsource == 1:
                    row[1] = 2
                else:
                    pass
                    #print(f"Processing without relationship check for GLOBALID {objectid_value} in {dest_fc}.")
                cursor.updateRow(row)
        edit.stopOperation()
        edit.stopEditing(True)
    except Exception as e:
        edit.stopOperation()
        edit.stopEditing(False)
        print(f"Error during update: {e}")
        raise
    print(f"Completion for {dest_fc}.")



def main():
# BAY CLASS calculated by SWITCHINGFACILITY CLASS
    switching_facility_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\SwitchingFacility"
    bay_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\Bay"
    field_pairs_bay = [("STATION_OID", "MIG_STATIONGUID"), ("OPERATINGVOLTAGE", "MIG_VOLTAGE")]
    where_clause_bay1 = "SWITCHINGFACILITY_OID IS NOT NULL"
    update_fc_from_dict(switching_facility_path, bay_path, "OBJECTID", "SWITCHINGFACILITY_OID", field_pairs_bay, where_clause_bay1)


# BAYSCHEME CLASS calculated by BAY CLASS
    bay_scheme_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\BayScheme"
    field_pairs_bayscheme = [("MIG_STATIONGUID", "MIG_STATIONGUID")]
    where_clause_bay_scheme = "BAY_OID IS NOT NULL"
    update_fc_from_dict(bay_path, bay_scheme_path, "OBJECTID", "BAY_OID", field_pairs_bayscheme, where_clause_bay_scheme)

# BAYSCHEME CLASS inside STATIONSCHEME CLASS
    station_scheme_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\StationScheme"
    update_fc_within(bay_scheme_path, station_scheme_path)


# CIRCUIT_SOURCE CLASS calculated by ITSELF
    circuit_source_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\CircuitSource"
    field_pairs_circuit_source = [('OBJECTID', 'MIG_OID', 'MIG_OID_TEXT'), ('GLOBALID', 'MIG_GLOBALID')]
    update_fc_self(circuit_source_path, field_pairs_circuit_source)


# CircuitSourceID CLASS calculated by ITSELF
    circuit_source_id_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\CircuitSourceID"
    field_pairs_circuit_source_id = [('OBJECTID', 'MIG_OID', 'MIG_OID_TEXT'), ('GLOBALID', 'MIG_GLOBALID')]
    update_fc_self(circuit_source_id_path, field_pairs_circuit_source_id)


# Electric_NET_Junctions CLASS calculated by StationBoundary CLASS
    electric_net_junctions_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\Electric_Net_Junctions"
    station_boundary_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\StationBoundary"
    station_oid = 'GLOBALID' 
    mig_stationguid_electric = 'MIG_STATIONGUID' 
    update_point_fc_within_station_boundary(electric_net_junctions_path, station_boundary_path, 
                                            station_oid, mig_stationguid_electric)

# Electric_NET_Junctions CLASS whether the junction is on a busbar, internal connecting line or a conductor
    busbar_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\Busbar"
    conductor_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\Conductor"
    internal_connection_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\InternalConnection"
    mapping_el_net_junction = {busbar_path: "Busbar", conductor_path: "Conductor", internal_connection_path: "Internal Connection"}
    mig_parenttype = 'MIG_PARENTTYPE'
    objectid = 'OBJECTID'
    update_field_based_on_whether_it_lies(electric_net_junctions_path, mapping_el_net_junction, mig_parenttype, objectid)
    update_conductor_type(electric_net_junctions_path, conductor_path, mig_parenttype, 'SUBTYPE_CD')  

# Electric_NET_Junctions CLASS Voltage calculated by Busbar/Conductor/InternalConnection CLASSES
    join_internal = r"D:\UN\set_DB\set_DB.gdb\join_internal_layer"
    join_conductor = r"D:\UN\set_DB\set_DB.gdb\join_conductor_layer"
    join_busbar = r"D:\UN\set_DB\set_DB.gdb\join_busbar_layer"
    update_mig_voltage(join_internal, join_conductor, join_busbar, electric_net_junctions_path)



# BUSBAR CLASS calculated by SWITCHINGFACILITY CLASS
    busbar_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\Busbar"
    field_pairs_busbar = [("STATION_OID", "MIG_STATIONGUID")]
    where_clause_busbar = "SWITCHINGFACILITY_OID IS NOT NULL"
    update_fc_from_dict(switching_facility_path, busbar_path, "OBJECTID", "SWITCHINGFACILITY_OID", field_pairs_busbar, where_clause_busbar)

# BUSBAR CLASS inside STATIONSCHEME CLASS
    station_scheme_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\StationScheme"
    update_fc_within(busbar_path, station_scheme_path)



# CIRCUITBREAKER CLASS calculated by itself (+Circuit_Source CLASS)
    circuit_breaker_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\CircuitBreaker"
    fields = ["SUBSOURCE", "MIG_ISSOURCE", "OBJECTID"]
    objectid_circiut_breaker = "CIRCUITBREAKER_OID"
    update_mig_issource(circuit_breaker_path, circuit_source_path, objectid_circiut_breaker, fields=fields)

# CIRCUITBREAKER calculated by CIRCUITSOURCEID CLASS
    field_pairs_circuit_breaker = [("FEEDERID", "MIG_FEEDERID")]
    where_clause = ""
    update_fc_from_dict(circuit_source_path, circuit_breaker_path, "MIG_OID",  "OBJECTID",  field_pairs_circuit_breaker, where_clause)

# CIRCUITBREAKER calculated by CIRCUITSOURCEID CLASS
    field_pairs_circuit_breaker2 = [("FEEDERNAME", "MIG_FEEDERNAME")]
    where_clause = ""
    update_fc_from_dict(circuit_source_id_path, circuit_breaker_path,
                        "MIG_OID",  # or SUBSTATIONID
                        "OBJECTID", field_pairs_circuit_breaker2, where_clause)

# CIRCUITBREAKER CLASS calculated by BAY CLASS
    field_pairs_circuit_breaker3 = [("STATION_OID", "MIG_STATIONGUID")]   #[("MIG_STATIONGUID", "MIG_STATIONGUID")]
    where_clause_circuit_breaker = "SWITCHINGFACILITY_OID IS NOT NULL"    #"BAY_GUID IS NOT NULL"
    update_fc_from_dict(switching_facility_path, circuit_breaker_path, "OBJECTID", "SWITCHINGFACILITY_OID", field_pairs_circuit_breaker3, where_clause_circuit_breaker) #(bay_path, circuit_breaker_path,"GLOBALID", "BAY_GUID",


# DISCONNECTOR CLASS calculated by itself (+Circuit_Source CLASS)
    disconnector_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\Disconnector"
    circuit_source_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\CircuitSource"
    objectid_disconnetor = "DISCONNECTOR_OID"
    fields = ["SUBSOURCE", "MIG_ISSOURCE", "OBJECTID"]
    update_mig_issource(disconnector_path, circuit_source_path, objectid_disconnetor, fields=fields)

# DISCONNECTOR CLASS calculated by CIRCUITSOURCE CLASS
    circuit_source = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\CircuitSource"
    field_pairs_disconnector1 = [("FEEDERID", "MIG_FEEDERID")]
    where_clause = ""
    update_fc_from_dict(circuit_source, disconnector_path, "MIG_OID",  "OBJECTID",  field_pairs_disconnector1, where_clause)

# DISCONNECTOR CLASS calculated by CIRCUITSOURCEID CLASS
    circuit_source_id = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\CircuitSourceID"
    field_pairs_disconnector2 = [("FEEDERNAME", "MIG_FEEDERNAME")]
    where_clause = ""
    update_fc_from_dict(circuit_source_id, disconnector_path,
                        "MIG_OID",  # or SUBSTATIONID
                        "OBJECTID", field_pairs_disconnector2, where_clause)
    
# DISCONNECTOR CLASS calculated by BAY CLASS
    field_pairs_disconnector3 = [("STATION_OID", "MIG_STATIONGUID")]   #[("MIG_STATIONGUID", "MIG_STATIONGUID")]
    where_clause_disconnector = "SWITCHINGFACILITY_OID IS NOT NULL"     #"BAY_OID IS NOT NULL"
    update_fc_from_dict(switching_facility_path,disconnector_path,"OBJECTID", "SWITCHINGFACILITY_OID", field_pairs_disconnector3, where_clause_disconnector) #"GLOBALID", "BAY_GUID",
    

# FaultIndicator CLASS calculated by CONDUCTOR CLASS
    join_internal_connection = None
    join_conductor = r"D:\UN\set_DB\set_DB.gdb\join_conductor_layer"
    join_busbar = None 
    fault_indicator_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\FaultIndicator"
    update_mig_voltage(join_internal_connection, join_conductor, join_busbar, fault_indicator_path)

# FAULTINDICATOR CLASS calculated by Station
    station_boundary_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\StationBoundary"
    station_oid = 'GLOBALID' 
    mig_stationguid_interal_con = 'MIG_STATIONGUID' 
    update_line_fc_within_station_boundary(fault_indicator_path, station_boundary_path, station_oid, mig_stationguid_interal_con)

# FUSE CLASS calculated by itself (+Circuit_Source CLASS)
    fuse_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\Fuse"
    #circuit_source_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\CircuitSource"
    #objectid_fuse = "FUSE_OID"
    #fields = ["SUBSOURCE", "MIG_ISSOURCE", "OBJECTID"]
    #update_mig_issource(fuse_path, circuit_source_path,objectid_fuse, fields=fields)    няма в БГ поле subsource

# FUSE CLASS calculated by CIRCUITSOURCE CLASS
    circuit_source_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\CircuitSource"
    field_pairs_fuse1 = [("FEEDERID", "MIG_FEEDERID")]
    where_clause = ""
    update_fc_from_dict(circuit_source_path, fuse_path,
                        "MIG_OID", "OBJECTID",  
                        field_pairs_fuse1, where_clause)

# FUSE CLASS calculated by CIRCUITSOURCEID CLASS
    circuit_source_id = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\CircuitSourceID"
    field_pairs_fuse2 = [("FEEDERNAME", "MIG_FEEDERNAME")]
    where_clause = ""
    update_fc_from_dict(circuit_source_id, fuse_path,
                    "MIG_OID",  # or SUBSTATIONID
                    "OBJECTID",  field_pairs_fuse2, where_clause)

# FUSE CLASS calculated by BAY CLASS
    field_pairs_fuse3 = [("STATION_OID", "MIG_STATIONGUID")]   #[("MIG_STATIONGUID", "MIG_STATIONGUID")]
    where_clause_fuse = "SWITCHINGFACILITY_OID IS NOT NULL"     #"BAY_OID IS NOT NULL"
    update_fc_from_dict(switching_facility_path,fuse_path,"OBJECTID", "SWITCHINGFACILITY_OID", field_pairs_fuse3, where_clause_fuse) #"GLOBALID", "BAY_GUID",


# INTERNALCONNECTION CLASS calculated by Station
    internal_connection_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\InternalConnection"
    station_boundary_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\StationBoundary"
    station_oid = 'GLOBALID' 
    mig_stationguid_interal_con = 'MIG_STATIONGUID' 
    update_line_fc_within_station_boundary(internal_connection_path, station_boundary_path, station_oid, mig_stationguid_interal_con)


# LOADBREAK_SWITCH CLASS calculated by itself (+Circuit_Source CLASS)
    loadbreak_switch_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\LoadBreakSwitch"
    objectid_load_break = "LOADBREAKSWTICH_OID"
    fields = ["SUBSOURCE", "MIG_ISSOURCE", "OBJECTID"]
    update_mig_issource(loadbreak_switch_path, circuit_source_path, objectid_load_break, fields=fields)

# LOADBREAK_SWITCH CLASS calculated by CIRCUITSOURCE CLASS
    field_pairs_loak_break1 = [("FEEDERID", "MIG_FEEDERID")]
    where_clause = ""
    update_fc_from_dict(circuit_source_path, loadbreak_switch_path, "MIG_OID",  "OBJECTID",  field_pairs_loak_break1, where_clause)

# LOADBREAK_SWITCH CLASS calculated by CIRCUITSOURCEID CLASS
    field_pairs_loak_break2 = [("FEEDERNAME", "MIG_FEEDERNAME")]
    where_clause = ""
    update_fc_from_dict(circuit_source_id_path, loadbreak_switch_path,
                        "MIG_OID",  # or SUBSTATIONID
                        "OBJECTID", field_pairs_loak_break2, where_clause)

# LOADBREAK_SWITCH CLASS calculated by BAY CLASS
    field_pairs_loak_break3 = [("STATION_OID", "MIG_STATIONGUID")]   #[("MIG_STATIONGUID", "MIG_STATIONGUID")]
    where_clause_load_break = "SWITCHINGFACILITY_OID IS NOT NULL"     #"BAY_OID IS NOT NULL"
    update_fc_from_dict(switching_facility_path,loadbreak_switch_path,"OBJECTID", "SWITCHINGFACILITY_OID", field_pairs_loak_break3, where_clause_load_break) #"GLOBALID", "BAY_GUID",
    

# MEASUREMENTTRANSFORMER CLASS calculated by Station
    measurement_transformer_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\MeasurementTransformer"
    station_oid = 'GLOBALID' 
    mig_stationguid_electric = 'MIG_STATIONGUID' 
    update_point_fc_within_station_boundary(measurement_transformer_path, station_boundary_path, station_oid, mig_stationguid_electric)



# STATION_EQUIPMENT CLASS calculated by STATION CLASS
    station_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\Station"
    station_equipment_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\StationEquipment"
    field_pairs_station = [("OPERATINGVOLTAGE", "MIG_VOLTAGE")]
    where_clause_station_equipment = "STATION_OID IS NOT NULL"
    update_fc_from_dict(station_path,station_equipment_path,"OBJECTID","STATION_OID",field_pairs_station,where_clause_station_equipment)


# TRANSFORMER CLASS calculated by BAY CLASS
    transformer_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\Transformer"
    field_pairs_transformer = [("OBJECTID", "MIG_STATIONGUID")]
    where_clause_transformer = "STATION_OID IS NOT NULL"
    update_fc_from_dict(station_path,transformer_path, "OBJECTID", "STATION_OID", field_pairs_transformer, where_clause_transformer)


# TRANSFORMER_UNIT CLASS calculated by TRANSFORMER CLASS
    transformer_unit_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\TransformerUnit"
    field_pairs_tran_unit = [("STATION_OID", "MIG_STATIONGUID")]
    where_clause_transformer_unit = "TRANSFORMER_OID IS NOT NULL"
    update_fc_from_dict(transformer_path,  transformer_unit_path,  "OBJECTID",  "TRANSFORMER_OID",   field_pairs_tran_unit,   where_clause_transformer_unit)

    
    print("Update completed successfully.")

if __name__ == "__main__":
    main()