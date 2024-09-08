import os
import subprocess
from datetime import datetime
import datetime
from datetime import datetime
import zoneinfo
from pathlib import Path
from pycarlo.core import Client,Session, Query

#The below are used to set the temporary file name / path for the list of monitors to migrate.
OUTPUT_FILE = 'monitors.csv'
#MONITORS_FILE_WORKSPACE = '/dbfs/FileStore/temp/montecarlo'
MONITORS_FILE_WORKSPACE = '/Users/akitbalian/Desktop/Dev/SandP'

#This function enables the code to identify the data product tables associated with the target data product.
#The main reason we do this step is that it enabled S&P to easily tag tables. With the tags they then can identify the right tables.
def get_data_product_tables(data_product_name):
    # print("\ngetting list of data products")
    query = Query()
    query.get_data_products_v2().__fields__("name", "uuid")
    tables = {}
    for data_product in client(query).get_data_products_v2:
        # print(data_product.name)
        if data_product.name == data_product_name:
            # print('Found data product: %s' % data_product.name)
            offset = 0
            fetch_number = 10
            while True:
                count=0
                query = Query()
                query.get_data_product_v2(data_product_id=data_product.uuid).assets(first=fetch_number,offset=offset).__fields__("edges")
                results = client(query).get_data_product_v2
                for asset in results.assets.edges:
                    if asset.node.table_id not in tables:
                        tables[asset.node.table_id] = {"mcon": asset.node.mcon,
                                                    'monitors': [],
                                                    'full_id': asset.node.display_name}
                    # print(asset.node)
                    table_query = Query()
                    table_query.get_table(mcon=asset.node.mcon).warehouse.__fields__('name')
                    results2 = client(table_query).get_table
                    print("Warehouse: %s, Table: %s" % (results2.warehouse.name,asset.node.display_name))
                    count = count + 1
                if count < 10:
                    break
                else:
                    offset = offset + count
            break
    return tables

#The below function is used to identify the associated warehouse for the table - specifically this is used to differentiate QA vs. Prod
def get_table_warehouse_details(tables_list):
    warehouse_info = {}
    for table in tables_list:
        table_query = Query()
        table_query.get_table(mcon=tables_list[table]['mcon']).warehouse.__fields__('name', 'uuid')
        result = client(table_query).get_table
        tables_list[table]['warehouse'] = {'name': result.warehouse.name,
                                     'uuid': result.warehouse.uuid}
        warehouse_info = {'name': result.warehouse.name,
                    'uuid': result.warehouse.uuid}
    return tables_list, warehouse_info

#The below function gets a list of monitors for a specific data products from source (i.e. QA) to Target (i.e. Prod)
def get_monitors(client, source_data_product_name, destination_data_product_name):
    # Get list of tables that are tagged with tag_name
    print("\nGetting list of tables from %s" % source_data_product_name)
    source_tables = get_data_product_tables(source_data_product_name)
    print("\nGetting list of tables from %s" % destination_data_product_name)
    destination_tables = get_data_product_tables(destination_data_product_name)

    #Fill in table details
    source_tables, source_warehouse_info = get_table_warehouse_details(source_tables)
    destination_tables, destination_warehouse_info = get_table_warehouse_details(destination_tables)

    print('\nFound %s source tables, and %s destination tables' %(len(source_tables), len(destination_tables)))

    #Check each tables in the source and compare with target tables
    #This checks if data product in QA has the same tables in prod - if prod has different tables then dont migrate the monitors.
    for key in list(source_tables.keys()):
        if key in destination_tables:
            #Compares table names from QA to Prod
            if source_tables[key]['full_id'] == destination_tables[key]['full_id']:
                #This seems to hardcode a new parameter in the json called destination which is used later to identify tables that need migration
                source_tables[key]['destination'] = destination_tables[key]
                del destination_tables[key] #removes table from list if a match is found
            else:
                print('\nUnable to find matching destination table [table name matching but not its database], removing the below from source tables list')
                print('Source Table     : %s' % (source_tables[key]['full_id']))
                print('Destination Table: %s' % (destination_tables[key]['full_id']))
                del source_tables[key]
        else:
            print('\nUnable to find matching destination table, removing the below from source tables list')
            print('Source Table: %s' % (source_tables[key]['full_id']))
            del source_tables[key]

    #The below prints a list of tables that didnt meet the criteria of having the QA tables match Prod tables
    if len(destination_tables) > 0:
        print('\nFound %s destination tables that didnt match a source table' % (len(destination_tables)))
        for table in destination_tables:
            print(destination_tables[table]['full_id'])

    #Once tables are identified - then we identify the actual monitors on those tables
    # print("\nGetting the list of source monitors from each source table")
    for table in source_tables:
        print("\nGetting the list of source monitors from Source Table: ",source_tables[table]['full_id'])
        source_table_monitors = Query()
        #The below is pycarlo used to call MC APIs
        source_table_monitors.get_monitors(mcons=[source_tables[table]["mcon"]]).__fields__('uuid','namespace','description')
        response = client(source_table_monitors).get_monitors
        description_prefix = '%s |' % (source_data_product_name)
        for monitor in response:
            if monitor.namespace == 'ui': #checks that monitor is UI created - this indicates that the code does not migrate any MaC monitors from QA - this is as designed
                print("\nMonitor:",monitor.uuid," --- ",monitor.description)
                if monitor.description.startswith(description_prefix):
                    source_tables[table]["monitors"].append(monitor.uuid)
                    print("Marked for migration")
                else: #this validates naming standard for monitors
                    print("Not marked for migration as its description doesnt start with --- %s" % (description_prefix))

    return source_tables, source_warehouse_info, destination_tables, destination_warehouse_info

#The below generates a CSV file that will have a list of UUIDs for each monitor taht needs to be migrated
def write_csv_file(source_tables):

    #initialise variables
    print("\nWriting CSV file")
    monitors_to_write = []
    monitors_file_name = ''

    #check source table monitors and insert them into the monitor csv file
    if len(source_tables) > 0:
        for table in source_tables:
            for monitor in source_tables[table]['monitors']:
                if monitor not in monitors_to_write:
                    monitors_to_write.append(monitor)
    #if any monitors are due to being migrated - create a folder and csv file and copy the monitor UUIDs into it
    if monitors_to_write:
        print("Found %s monitors to write" % len(monitors_to_write))
        file_path = Path(os.path.abspath(MONITORS_FILE_WORKSPACE))
        file_path.mkdir(parents=True, exist_ok=True)
        monitors_file_name = file_path / OUTPUT_FILE
        print("Writing to file: %s" % monitors_file_name)
        with open(monitors_file_name, 'w') as csvfile:
            for monitor_id in monitors_to_write:
                csvfile.write(f"{monitor_id}\n")
    return monitors_file_name

#This functions downloads a yaml version for specified monitors using the CLI
def export_monitors(monitors_file_path, namespace, warehouse_id):
    print("\nExporting monitors")
    timestamp = datetime.now(timezone).strftime("%Y%m%d_%H%M%S")

    #populates variable with csv file folder
    mc_monitors_path = f"{MONITORS_FILE_WORKSPACE}/cli" # /cli_{timestamp}"
    #populates CLI variable to execute - the CLI will export all UI created monitors that are identified in the csv file
    #the dry run ensures that QA monitors are not deleted - but the monitor configurations are simply exported into yaml
    cmd_args = ["montecarlo", "monitors", "convert-to-mac",
             "--namespace", namespace, "--project-dir", mc_monitors_path,
             "--monitors-file", monitors_file_path, "--dry-run"]
    print(cmd_args)
    #Execute the CLI
    cmd = subprocess.run(cmd_args, capture_output=True, text=True)
    print(cmd.stderr)
    print(cmd.stdout)

    print("\nAdding default_resource")

    #I think this modifies the main configuration yaml file with the Target warehouse ID
    with open(mc_monitors_path + '/montecarlo.yml', 'r') as montecarlo_yml:
        file_data = montecarlo_yml.read()
        file_data = file_data + 'default_resource: %s' % warehouse_id

    #Once the above change is made - it writes the changes into a file NOTE: this isnt the yaml with all monitors - but the main configuration file montecarlo.yaml
    with open(mc_monitors_path + '/montecarlo.yml', 'w') as new_montecarlo_yml:
        new_montecarlo_yml.write(file_data)

    print("file_data:\n",file_data)

    montecarlo_yml.close()
    new_montecarlo_yml.close()
    print("Wrote the file")
    return mc_monitors_path

#This function modifies the actual yaml file with the monitor configurations to prepare it for execution
def modify_monitors_file_ids(monitor_path, source_tables, source_warehouse_info, destination_warehouse_info, source_data_product_name, destination_data_product_name):
    print("\nModifying the monitors file")
    monitors_file_yml = monitor_path + '/montecarlo/monitors.yml'

    with open(monitors_file_yml, 'r') as monitors_yml:
        file_data = monitors_yml.read()
        print("file_data:\n",file_data)
        #The below changes the warehouse ID from QA to Prod
        file_data = file_data.replace(source_warehouse_info['name'], destination_warehouse_info['name'])
        #The below changes the data product name from QA to Prod. The actual change seems to happen in the description field of the monitor.
        file_data = file_data.replace(source_data_product_name, destination_data_product_name)
        # print(file_data)

        #This seems to use the previously hardcoded variable destination to identify which values to replace in the file with new prod paths.
        #This will replace the QA table path with a Prod table path
        for table in source_tables:
            if "destination" in source_tables[table]:
                file_data = file_data.replace(source_tables[table]['full_id'], source_tables[table]['destination']['full_id'])
                file_data = file_data.replace(source_tables[table]['full_id'].replace(':','.'), source_tables[table]['destination']['full_id'].replace(':','.'))

    #once changes are made write them into the yaml file
    with open(monitors_file_yml, 'w') as new_monitors_yml:
        new_monitors_yml.write(file_data)
        print("file_data:\n",file_data)
    monitors_yml.close()
    new_monitors_yml.close()
    print("\nCompleted modifying the monitors file")

#As one of the last steps - this function then takes the new montecarlo.yaml config as well as the new monitors yaml file and applies it to the Monte Carlo env via the CLI
def move_monitors(namespace, monitors_workspace_dir):
    print("Moving the monitors")

    #--aut-yes simply skips any manual approvals the CLI may require
    cmd_args = ["montecarlo", "monitors", "apply", "--namespace", namespace, "--project-dir", monitors_workspace_dir, "--auto-yes"]
    cmd = subprocess.run(cmd_args, capture_output=True, text=True)
    print(cmd.stdout)
    print(cmd.stderr)
    print("Movement complete")

#This function deletes all working files - I would suggest we modify this to keep a audit trail of changes instead.
def clean_up_files():
    print("\nCleaning up files")
    #these are bash script commands to force delete a path recursively.
    cmd_args = ['rm', '-rf', MONITORS_FILE_WORKSPACE]
    cmd = subprocess.run(cmd_args, capture_output=True, text=True)

if __name__ == '__main__':
    print("\nProcess Start")
    timezone = zoneinfo.ZoneInfo('Asia/Kolkata')
    current_time = datetime.now(timezone)
    print(current_time.strftime('%Y-%m-%d %H:%M:%S %Z%z'))
    #-------------------INPUT VARIABLES---------------------
    mcdId = 'af93c871703843849f04df8c1355ce68'
    mcdToken = 'CSF7CXV_rNrfUyu0nOEEABllEAEbhG2cVQmCL7ZpPPWZoeoWCi_PWdPh'
    source_data_product_name = 'TestMigrateMonitors (QA)'
    destination_data_product_name = 'TestMigrateMonitors (Prod)'
    #-------------------------------------------------------
    print("ID: %s, Token: %s" % (mcdId, mcdToken))
    # Environment setup
    os.environ['MCD_DEFAULT_API_ID'] = mcdId
    os.environ['MCD_DEFAULT_API_TOKEN'] = mcdToken
    client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
    namespace = destination_data_product_name.replace(' ', '_')

    #Get monitors to move
    source_tables, source_warehouse_info, destination_tables, destination_warehouse_info = get_monitors(client=client, source_data_product_name=source_data_product_name, destination_data_product_name=destination_data_product_name)
    print("\nsource_tables:\n",source_tables)
    print("\nsource_warehouse_info:\n",source_warehouse_info)
    print("\ndestination_tables:\n",destination_tables)
    print("\ndestination_warehouse_info:\n",destination_warehouse_info)
    print("\nnamespace:\n",namespace)

    if source_tables:
        # Write UUIDs to csv file
        csv_file_name = write_csv_file(source_tables=source_tables)

        # Validate CLI
        # Export using the csv file from above
        monitors_path = export_monitors(monitors_file_path=csv_file_name, namespace=namespace, warehouse_id=destination_warehouse_info['uuid'])

        # Modify exported files contents to new paths
        modify_monitors_file_ids(monitors_path, source_tables, source_warehouse_info=source_warehouse_info, destination_warehouse_info=destination_warehouse_info, source_data_product_name=source_data_product_name,destination_data_product_name=destination_data_product_name)

        # Re import the file
        move_monitors(namespace=namespace, monitors_workspace_dir=monitors_path)
    else:
        print('\nNo monitors found to move')
    #Clean up files on system
    clean_up_files()
    print("\nProcess Complete")
    current_time = datetime.now(timezone)
    print(current_time.strftime('%Y-%m-%d %H:%M:%S %Z%z'))
