from google.cloud import bigquery
import ConfigParser
import csv


def get_bq_table_metadata(project_list, dataset_list):

    table_metadata = []
    table_metadata.append(['project', 'dataset_id', 'dataset_description', 'table_id', 'table_description',
                           'date_created', 'date_modified', 'num_bytes', 'num_rows', 'partition_type', 'table_type'
                           ])
    for project in project_list:
        client = bigquery.Client(project=project)

        for dataset_id in dataset_list:
            dataset_ref = client.get_dataset(dataset_id)
            tables = client.list_tables(dataset_id)

            for table in tables:
                full_table_id = table.full_table_id.replace(':', '.')
                table_ref = client.get_table(full_table_id)
                table_metadata.append([project, dataset_id, dataset_ref.description, table_ref.table_id,
                             table_ref.description, table_ref.modified.strftime("%Y-%m-%d %H:%M:%S"),
                             table_ref.created.strftime("%Y-%m-%d %H:%M:%S"), table_ref.num_bytes,
                             table_ref.num_rows, table_ref.partitioning_type, table_ref.table_type])
    return table_metadata


def get_bq_attribute_data(project_list, dataset_list):

    attribute_metadata = []
    table_metadata.append(['project', 'dataset_id', 'table_id', 'attribute_name', 'description', 'data_type', 'mode'])

    for project in project_list:
        client = bigquery.Client(project=project)

        for dataset_id in dataset_list:
            tables = client.list_tables(dataset_id)
            for table in tables:

                full_table_id = table.full_table_id.replace(':', '.')
                table_ref = client.get_table(full_table_id)
                schemafields = table_ref.schema

                for schemafield in schemafields:
                    attribute_metadata.append([project, dataset_id, table_ref.table_id, schemafield.name,
                                     schemafield.description, schemafield.field_type, schemafield.mode])
    return attribute_metadata


if __name__ == "__main__":

    configFile = r'properties.conf'
    configParser = ConfigParser.RawConfigParser()
    configParser.read(configFile)

    # Read variables from config file (properties.conf)
    projects = [e.strip() for e in configParser.get('global', 'projects').split(',')]
    datasets = [e.strip() for e in configParser.get('global', 'datasets').split(',')]
    tableFile = open(configParser.get('global', 'tables_metadata_file_name'), 'w')
    attributeFile = open(configParser.get('global', 'attributes_metadata_file_name'), 'w')

    tableWriter = csv.writer(tableFile)
    attributeWriter = csv.writer(attributeFile)

    table_metadata = get_bq_table_metadata(projects, datasets)
    for row in table_metadata:
        tableWriter.writerow(row)

    attribute_metadata = get_bq_attribute_data(projects, datasets)
    for row in attribute_metadata:
        attributeWriter.writerow(row)

    tableFile.close()
    attributeFile.close()
    print ("done")
