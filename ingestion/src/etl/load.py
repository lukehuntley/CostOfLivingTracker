import pandas as pd

class Load():

    def __init__(self, df:pd.DataFrame, path_to_output_folder:str, output_filename:str): 
        self.df = df
        self.path_to_output_folder = path_to_output_folder
        self.output_filename = output_filename

    def run(self):

        self.df.to_excel(f'{self.path_to_output_folder}{self.output_filename}.xlsx', index=False)