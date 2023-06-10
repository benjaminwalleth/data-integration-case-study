import pandas as pd
import openpyxl


def csv_to_dataframe(path):
    return pd.read_csv(path, sep=';')


def xlsx_to_dataframe(path):
    return pd.read_excel(path)


xlsx_to_dataframe("./data/relations.xlsx")
