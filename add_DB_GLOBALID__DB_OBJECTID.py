import arcpy


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
                        #if 'TEXT' in each_target_field:
                            #row[target_index] = str(source_value)
                        #else:
                            #row[target_index] = source_value
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


def main():
# BAY CLASS calculated by SWITCHINGFACILITY CLASS
    bay_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\Bay"
    field_pairs = [('OBJECTID', 'DB_OBJECTID'), ('GLOBALID', 'DB_GLOBALID')]
    #######update_fc_self(bay_path, field_pairs)

# BAYSCHEME CLASS calculated by BAY CLASS
    bay_scheme_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\BayScheme"
    field_pairs = [('OBJECTID', 'DB_OBJECTID'), ('GLOBALID', 'DB_GLOBALID')]
    #######update_fc_self(bay_scheme_path, field_pairs)

# Electric_NET_Junctions CLASS calculated by StationBoundary CLASS
    electric_net_junctions_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\Electric_Net_Junctions"
    field_pairs = [('OBJECTID', 'DB_OBJECTID'), ('GLOBALID', 'DB_GLOBALID')]
    #update_fc_self(electric_net_junctions_path, field_pairs)

# BUSBAR CLASS calculated by SWITCHINGFACILITY CLASS
    busbar_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\Busbar"
    field_pairs = [('OBJECTID', 'DB_OBJECTID'), ('GLOBALID', 'DB_GLOBALID')]
    update_fc_self(busbar_path, field_pairs)
    
# CIRCUITBREAKER CLASS calculated by itself (+Circuit_Source CLASS)
    circuit_breaker_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\CircuitBreaker"
    field_pairs = [('OBJECTID', 'DB_OBJECTID'), ('GLOBALID', 'DB_GLOBALID')]
    update_fc_self(circuit_breaker_path, field_pairs)
    
# DISCONNECTOR CLASS calculated by itself (+Circuit_Source CLASS)
    disconnector_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\Disconnector"
    field_pairs = [('OBJECTID', 'DB_OBJECTID'), ('GLOBALID', 'DB_GLOBALID')]
    update_fc_self(disconnector_path, field_pairs)
    
# FaultIndicator CLASS calculated by CONDUCTOR CLASS
    fault_indicator_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\FaultIndicator"
    field_pairs = [('OBJECTID', 'DB_OBJECTID'), ('GLOBALID', 'DB_GLOBALID')]
    update_fc_self(fault_indicator_path, field_pairs)
    
# FUSE CLASS calculated by itself (+Circuit_Source CLASS)
    fuse_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\Fuse"
    field_pairs = [('OBJECTID', 'DB_OBJECTID'), ('GLOBALID', 'DB_GLOBALID')]
    update_fc_self(fuse_path, field_pairs)
    
# INTERNALCONNECTION CLASS calculated by Station
    internal_connection_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\InternalConnection"
    field_pairs = [('OBJECTID', 'DB_OBJECTID'), ('GLOBALID', 'DB_GLOBALID')]
    update_fc_self(internal_connection_path, field_pairs)

# LOADBREAK_SWITCH CLASS calculated by itself (+Circuit_Source CLASS)
    loadbreak_switch_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\LoadBreakSwitch"
    field_pairs = [('OBJECTID', 'DB_OBJECTID'), ('GLOBALID', 'DB_GLOBALID')]
    update_fc_self(loadbreak_switch_path, field_pairs)
    
# MEASUREMENTTRANSFORMER CLASS calculated by Station
    measurement_transformer_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\MeasurementTransformer"
    field_pairs = [('OBJECTID', 'DB_OBJECTID'), ('GLOBALID', 'DB_GLOBALID')]
    update_fc_self(measurement_transformer_path, field_pairs)
    
# STATION_EQUIPMENT CLASS calculated by STATION CLASS
    station_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\StationEquipment"
    field_pairs = [('OBJECTID', 'DB_OBJECTID'), ('GLOBALID', 'DB_GLOBALID')]
    #####update_fc_self(station_path, field_pairs)
    
# TRANSFORMER CLASS calculated by BAY CLASS
    transformer_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\Transformer"
    field_pairs = [('OBJECTID', 'DB_OBJECTID'), ('GLOBALID', 'DB_GLOBALID')]
    update_fc_self(transformer_path, field_pairs)
    
# TRANSFORMER_UNIT CLASS calculated by TRANSFORMER CLASS
    transformer_unit_path = r"D:\UN\set_DB\databases\GISBG_PL_GULY.gdb\TransformerUnit"
    field_pairs = [('OBJECTID', 'DB_OBJECTID'), ('GLOBALID', 'DB_GLOBALID')]
    #####update_fc_self(transformer_unit_path, field_pairs)
    
    print("Update completed successfully.")

if __name__ == "__main__":
    main()