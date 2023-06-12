import pandas as pd
from read_file import xlsx_to_dataframe, csv_to_dataframe


def check_nullity():
    """ This function verifies the presence of null values in fields.

    :return:
    """

    # Contacts
    print("--- Contacts ---")
    contacts = csv_to_dataframe("./data/contacts.csv")
    print(contacts.isna().sum())

    # Contracts
    print("\n--- Contracts ---")
    contracts = csv_to_dataframe("./data/contrats.csv")
    print(contracts.isna().sum())

    # Relations
    print("\n--- Relations ---")
    relations = xlsx_to_dataframe("./data/relations.xlsx")
    print(relations.isna().sum())


def check_duplicate():
    """ This function performs two tasks: it checks for duplicate data
    and identifies the possible unique values for a field.

    :return:
    """

    def is_unique(series: pd.Series):
        """ Check uniqueness of a boolean series.

        :param series: a boolean series
        :return:
        """
        count = series.sum()
        return count == 0, count

    # Contacts
    print("--- Contacts ---")
    contacts = csv_to_dataframe("./data/contacts.csv")

    print('contact type:', contacts['type de contact'].unique())

    # Two types of contact
    contacts_PF = contacts[contacts['type de contact'] == 'PF']
    contacts_PM = contacts[contacts['type de contact'] == 'PM']

    print("# of rows (PF):", contacts_PF.shape[0])
    print("# of rows (PM):", contacts_PM.shape[0])

    print("is unique: ", is_unique(contacts_PF.duplicated(subset=['nom', 'prenom', 'date de naissance'], keep=False)))
    print("is unique: ", is_unique(contacts_PM.duplicated(subset=['nom'], keep=False)))
    print('civilities:', contacts_PF['civilité'].unique())

    # Contracts
    print("\n--- Contracts ---")
    contracts = csv_to_dataframe("./data/contrats.csv")

    print("# of rows:", contracts.shape[0])

    print("is unique: ", is_unique(contracts.duplicated()))
    print("is unique: ", is_unique(contracts.duplicated(subset=['nom', 'prenom'], keep=False)))
    print("is unique: ", is_unique(contracts.duplicated(subset=['nom', 'prenom', 'date de naissance'], keep=False)))
    print("is unique: ", is_unique(contracts.duplicated(subset=['numero du contrat'], keep=False)))

    # Relations
    print("\n--- Relations ---")
    relations = xlsx_to_dataframe("./data/relations.xlsx")

    print("# of rows:", relations.shape[0])

    print("Is unique: ", is_unique(relations.duplicated(subset=['nom source', 'prenom source',
                                                                'date de naissance source', 'nom destination',
                                                                'prenom destination', 'date de naissance destination'])))

    print(relations['type de relation'].unique())  # Get unique relation type

    # Check relations between files
    print("\n --- ---")

    count = 0
    for index, row in contacts.iterrows():
        query_result = contracts[(contracts['nom'] == row['nom'])
                                 & (contracts['prenom'] == row['prenom'])
                                 & (contracts['date de naissance'] == row['date de naissance'])]
        if len(query_result) > 0:
            count += 1

    print("Common people in files:", count)


check_nullity()
check_duplicate()
