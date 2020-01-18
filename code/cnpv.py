from zipfile import ZipFile
import io
from pathlib import Path
from pycspro import DictionaryParser
import pandas as pd

data_folder = Path.cwd() / "data"
dict_path = Path.cwd() / "dict/Diccionario_Datos_CNPV_2018.zip"


def CreateProcessedDataframe(folder_path, dict_path):
    """Creates a processed Pandas dataframe reading the folder with the raw data and the file with the data dictionary and returns a processed Pandas dataframe
    
    Parameters:
    folder_path (string): The folder path that includes the data archives
    dict_path (string): The file path that includes the data dictionary archive

    Returns:
    dict: A dictionary of Pandas dataframes
    """
    dfs = ReadDataFolder(folder_path)
    dictionary_parser = ReadCSProDct(dict_path)
    for name, df in dfs.items():
        if name != "marco_georreferenciacion":
            dfs[name] = df.replace(GetValueLabels(dictionary_parser, name))
    return dfs


def ReadDataFolder(folder_path):
    """Reads the folder with the census data archives and returns a dictionary of Pandas dataframes containing the census data of all the files in the given folder path

    Parameters:
    folder_path (string): The folder path that includes the data archives

    Returns:
    dict: A dictionary of Pandas dataframes
    """
    dfs = {
        "viviendas": pd.DataFrame(),
        "hogares": pd.DataFrame(),
        "fallecidos": pd.DataFrame(),
        "personas": pd.DataFrame(),
        "marco_georreferenciacion": pd.DataFrame(),
    }
    pathlist = Path(folder_path).glob("**/[0-9][0-9]_*")
    for path in pathlist:
        current_dfs = ReadZippedStataData(path)
        for key in dfs:
            dfs[key] = pd.concat([current_dfs[key], dfs[key]])
    return dfs


def ReadZippedStataData(file_path):
    """Reads the compressed file containing the census data and returns a dictionary of Pandas dataframes containing the census data of the given file
    
    Parameters:
    file_path (string): The file path for the archive with the census data

    Returns:
    dict: A dictionary of Pandas dataframes
    """
    df_dictionary = {}
    with ZipFile(file_path, "r") as data_archive:
        dta_archive_list = [
            filename
            for filename in data_archive.namelist()
            if filename.lower().endswith("dta.zip")
        ]
        for filename in dta_archive_list:
            with data_archive.open(filename) as nested_data_archive:
                dta_archive_filedata = io.BytesIO(nested_data_archive.read())
                with ZipFile(dta_archive_filedata) as dta_archive:
                    dta_file_list = dta_archive.namelist()
                    for filename in dta_file_list:
                        dataframe_name = MapDataframeName(filename)
                        dta_file = dta_archive.open(filename)
                        df = ReadStataData(dta_file)
                        df_dictionary[dataframe_name] = df
        return df_dictionary


def CleanDataframe(df):
    """Cleans the dataframe (converts datatypes and converts the column names to uppercase as defined in the data dictionary)

    Parameters:
    df (Pandas.DataFrame): Dataframe to be cleaned

    Returns:
    df (Pandas.DataFrame): Cleaned dataframe
    """
    clean_df = df.select_dtypes(include=["float64"]).astype("Int64")
    df.update(clean_df)
    df.columns = map(str.upper, df.columns)
    return df


def ReadStataData(data):
    """Reads a Stata data file and converts it to a Pandas dataframe and the cleans the data
    
    Parameters:
    data (filepath_or_buffer): Data in Stata format to be read

    Returns:
    df (Pandas.DataFrame): Cleaned Pandas dataframe

    """
    df = pd.read_stata(data)
    df = CleanDataframe(df)
    return df


def ReadCSProDct(dict_path):
    """Reads a compressed archive containing the census CSPro dictionary file and parses it useing the pycspro library
    
    Parameters:
    dict_path (string): The file path to the dictionary archive

    Returns:
    dictionary_parser (pycspro.DictionaryParser): A DictionaryParser object containing the parsed data dictionary
    """
    with ZipFile(dict_path, "r") as dict_archive:
        with dict_archive.open("Diccionario_datosCNPV.dcf", mode="r") as dict_file:
            raw_dictionary = dict_file.read().decode("utf-8")
            dictionary_parser = DictionaryParser(raw_dictionary)
            dictionary_parser.parse()
    return dictionary_parser


def GetColumnNames(dictionary_parser, table_name):
    """Gets a dictionary containing the column names for a particular dataframe name
    
    Parameters:
    dictionary_parser (pycspro.DictionaryParser): A DictionaryParser object containing the parsed data dictionary
    table_name (string): The name of the table to get the column names from

    Returns:
    column_labels (dict): A dictionary containing a mapping of column names to column labels
    """
    column_labels = dictionary_parser.get_column_labels(MapRecordName(table_name))
    return column_labels


def GetValueLabels(dictionary_parser, table_name):
    """Gets a dictionary containing the column names for a particular dataframe name
    
    Parameters:
    dictionary_parser (pycspro.DictionaryParser): A DictionaryParser object containing the parsed data dictionary
    table_name (string): The name of the table to get the column names from

    Returns:
    column_labels (dict): A dictionary containing a mapping of column names to column labels
    """
    value_labels = dictionary_parser.get_value_labels(MapRecordName(table_name))
    return value_labels


def MapRecordName(record_name):
    name_dict = {
        "viviendas": "REGVIV",
        "hogares": "REGHOG",
        "fallecidos": "REGFALL",
        "personas": "REGPER",
    }
    return name_dict[record_name]


def MapDataframeName(filename):
    key_string = filename.split("_")[1]
    name_dict = {
        "1VIV": "viviendas",
        "2HOG": "hogares",
        "3FALL": "fallecidos",
        "5PER": "personas",
        "MGN": "marco_georreferenciacion",
    }
    return name_dict[key_string]


def main():
    CreateProcessedDataframe(data_folder, dict_path)


if __name__ == "__main__":
    main()
