import pandas as pd
from read_file import xlsx_to_dataframe, csv_to_dataframe


def check_data():

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

    print("Is unique: ", is_unique(contacts.duplicated()))
    print("Is unique: ", is_unique(contacts.duplicated(subset=['nom', 'prenom', 'date de naissance', 'adresse'])))
    print("Is unique: ", is_unique(contacts.duplicated(subset=['nom', 'prenom', 'date de naissance'])))

    print("\n--> nullity")
    print(contacts.isna().sum())

    # Contracts
    print("\n--- Contracts ---")
    contracts = csv_to_dataframe("./data/contrats.csv")

    print("Is unique: ", is_unique(contracts.duplicated()))
    print("Is unique: ", is_unique(contracts.duplicated(subset=['numero du contrat'])))

    print("\n--> nullity")
    print(contracts.isna().sum())

    # Relations
    print("\n--- Relations ---")
    relations = xlsx_to_dataframe("./data/relations.xlsx")
    print("Is unique: ", is_unique(relations.duplicated()))

    print("\n--> nullity")
    print(relations.isna().sum())


check_data()