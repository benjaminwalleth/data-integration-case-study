import logging
import uuid
import pandas as pd
from sqlalchemy import create_engine, text, String, Date, Integer
from read_file import csv_to_dataframe, xlsx_to_dataframe

HOST = "<HOST>"
PORT = "<PORT>"
DATABASE = "<DATABASE>"
USER = "<USER>"
PASSWORD = "<PASSWORD>"


def determine_entity_type(row: pd.Series):
    """ This function returns the type of the entity.

    :param row: An entity row
    :return:
    """
    if row.isnull()['first_name'] and row.isnull()['birthday']:
        return 'PM'
    else:
        return 'PF'


def insert_in_table(connection, df: pd.DataFrame, table_name: str, data_type=None):
    """ This function is designed to insert a DataFrame into a specified database table.

    :param connection: The connection to the DB
    :param df: The DataFrame to insert
    :param table_name: The name of DB table
    :param data_type: A dict for data types
    :return:
    """

    if data_type is None:
        data_type = {}

    df.to_sql(name=table_name, con=connection, if_exists='replace', index=False, dtype=data_type)
    logging.info(f"Data transferred successfully in table [{table_name}].")


def import_to_mySQL():
    # Read files
    contacts = csv_to_dataframe('./data/contacts.csv')
    relations = xlsx_to_dataframe('./data/relations.xlsx')
    contracts = csv_to_dataframe('./data/contrats.csv')

    # Create the connexion to the DB
    url_schema = f'mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'
    engine = create_engine(url_schema)

    logging.info('Connected to MySQL.')

    # Rename columns
    contacts.columns = ['name', 'first_name', 'birthday', 'civility', 'entity_type', 'address', 'zip_code',
                        'city', 'country', 'phone_number']
    contracts.columns = ['name', 'first_name', 'birthday', 'contract_number', 'open_at', 'isin', 'count',
                         'unit_price', 'date_price', 'value']
    relations.columns = ['name_s', 'first_name_s', 'birthday_s', 'name_d', 'first_name_d', 'birthday_d',
                         'relation_type']

    # Transform dates
    contacts['birthday'] = pd.to_datetime(contacts['birthday'], format="%d/%m/%Y")
    contracts['birthday'] = pd.to_datetime(contracts['birthday'], format="%d/%m/%Y")
    contracts['open_at'] = pd.to_datetime(contracts['open_at'], format="%d/%m/%Y")
    contracts['date_price'] = pd.to_datetime(contracts['date_price'], format="%d/%m/%Y")
    relations['birthday_s'] = pd.to_datetime(relations['birthday_s'], format="%Y-%m-%d")
    relations['birthday_d'] = pd.to_datetime(relations['birthday_d'], format="%Y-%m-%d")

    # Give a unique id for each contact and client
    contacts['entity_id'] = contacts.apply(lambda x: uuid.uuid4(), axis=1)
    contracts['entity_id'] = contracts.apply(lambda x: uuid.uuid4(), axis=1)

    # For each contract determine the entity type (PM or PF)
    contracts['entity_type'] = contracts.apply(determine_entity_type, axis=1)

    # Create a DataFrame entities
    entities = pd.concat([contacts[['entity_id', 'name', 'first_name', 'birthday', 'entity_type']],
                          contracts[['entity_id', 'name', 'first_name', 'birthday', 'entity_type']]])

    # Build the DataFrame relations for the DB
    entities_mapper = {}
    relations_bd = pd.DataFrame(columns=['entity_id_source', 'entity_id_destination', 'relation_type'])
    for index, row in relations.iterrows():
        if row.isnull()['first_name_s'] or row.isnull()['first_name_d']:
            continue

        entity_source = (row['name_s'], row['first_name_s'], row['birthday_s'], 'PF')
        entity_destination = (row['name_d'], row['first_name_d'], row['birthday_d'], 'PF')

        entity_id_source = uuid.uuid4()
        entity_id_destination = uuid.uuid4()

        # Ensure that the entity doesn't already exist
        if entity_source in entities_mapper:
            entity_id_source = entities_mapper[entity_source]
        else:
            entities_mapper[entity_source] = entity_id_source

        if entity_id_destination in entities_mapper:
            entity_id_destination = entities_mapper[entity_source]
        else:
            entities_mapper[entity_destination] = entity_id_destination

        relation_type_mapper = {'espoux (e) de': 'SPOUSE_OF',
                                'parent (e) de': 'PARENT_OF',
                                'enfant (e) de': 'CHILD_OF'}
        relation_type = relation_type_mapper.get(row['relation_type'])

        relations_bd.loc[len(relations_bd)] = [entity_id_source, entity_id_destination, relation_type]

    for key, row in entities_mapper.items():
        entities.loc[len(entities)] = [row, key[0], key[1], key[2], key[3]]

    # --> insert this DataFrame in DB
    data_type = {'entity_id': String(36)}
    insert_in_table(engine, entities, 'entities', data_type)

    # Reshape the DataFrame contacts and contracts
    contacts.drop(columns=['name', 'first_name', 'birthday', 'entity_type'], inplace=True)
    contracts.drop(columns=['name', 'first_name', 'birthday', 'entity_type', 'value'], inplace=True)

    # --> insert them in DB
    insert_in_table(engine, contacts, 'contacts', data_type)
    data_type = {'entity_id': String(36),
                 'contract_number': String(64)}
    insert_in_table(engine, contracts, 'contracts', data_type)

    # Insert the DataFrame relations
    data_type = {'entity_id_source': String(36),
                 'entity_id_destination': String(36)}
    insert_in_table(engine, relations_bd, 'relations', data_type)

    # Add primary keys and constraints
    with engine.connect() as connexion:
        connexion.execute(text("ALTER TABLE `entities` ADD PRIMARY KEY (`entity_id`);"))
        connexion.execute(text("ALTER TABLE `contacts` ADD PRIMARY KEY (`entity_id`);"))
        connexion.execute(text("ALTER TABLE `contracts` ADD PRIMARY KEY (`contract_number`);"))


import_to_mySQL()
