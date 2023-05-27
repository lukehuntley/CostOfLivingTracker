from os import walk
import pandas as pd

class Extract():

    def __init__(self, path:str): 
        self.path = path

    def _get_csv_file_paths(self, path)->list:
        """
        Gets all the csv file names in a directory        
        - `path`: the path to the seed folder

        Returns a list of csv file paths
        """

        # Get list of file names
        list_of_csv_file_names = []
        for (dirpath, dirnames, filenames) in walk(path):
            
            list_of_csv_file_names.extend(filenames)

        # Get list of csv file paths
        list_of_csv_file_paths = []
        for file in list_of_csv_file_names:
            file_path = path + file
            list_of_csv_file_paths.append(file_path)

        return list_of_csv_file_paths

    def _get_transactions(self, list_of_csv_file_paths:list)->list:
        """
        Gets all the csv file names in a directory        
        - `list_of_csv_file_paths`: a list of paths to the csv files

        Returns a list of df
        """

        list_of_df = []

        for file_path in list_of_csv_file_paths:          

            df_name = file_path.split('/')[2].split('_')[0] + '_' + file_path.split('/')[2].split('_')[1] + '_' + file_path.split('/')[2].split('_')[2]
            
            df = pd.read_csv(file_path, sep=',')
            
            df.attrs['name'] = df_name

            list_of_df.append(df)
            
        return list_of_df
    
    def run(self)->pd.DataFrame:
        
        list_of_csv_file_paths = self._get_csv_file_paths(path=self.path)

        list_of_df = self._get_transactions(list_of_csv_file_paths=list_of_csv_file_paths)

        return list_of_df