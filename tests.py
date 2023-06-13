import unittest
from datetime import datetime
from read_file import csv_to_dataframe, xlsx_to_dataframe
from sqlalchemy import create_engine, text


HOST = "<HOST>"
PORT = "<PORT>"
DATABASE = "<DATABASE>"
USER = "<USER>"
PASSWORD = "<PASSWORD>"


class DataBaseTestCase(unittest.TestCase):
    def test_contact_insertion_in_db(self):
        url_schema = f'mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'
        self.engine = create_engine(url_schema)

        # We pick a random contact in the input file
        contacts = csv_to_dataframe("./data/contacts.csv")
        selected = False
        random_contact = None
        while not selected:
            random_contact = contacts.sample(n=1)
            # We do the test only for "natural person"
            if random_contact.values[0][4] == 'PF':
                selected = True

        name = random_contact.values[0][0]
        first_name = random_contact.values[0][1]
        birthday = random_contact.values[0][2]
        address = random_contact.values[0][5]

        date_object = datetime.strptime(birthday, '%d/%m/%Y')
        formatted_date = date_object.strftime('%Y-%m-%d')

        # We fetch this contact in DB
        with self.engine.connect() as connexion:
            query = text("SELECT address FROM entities INNER JOIN contacts ON entities.entity_id = contacts.entity_id"
                         " WHERE name=:name AND first_name=:first_name AND DATE(birthday)=:birthday")
            result = connexion.execute(query, {'name': name, 'first_name': first_name, 'birthday': formatted_date})
            address_from_db = result.first()[0]

        self.assertEqual(address, address_from_db)
