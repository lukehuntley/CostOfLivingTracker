from datetime import datetime as dt, timedelta
import pandas as pd
import numpy as np
import logging

class Transform():

    def __init__(self, list_of_df:list): 
        self.list_of_df = list_of_df

    def _get_list_of_unique_dates(self, list_of_df:list )->list:

        dates = []
        
        for df in list_of_df:

            dates.append(df.attrs['name'].split('_')[1])

        list_of_unique_dates = np.unique(dates)    
        
        return list_of_unique_dates  
     
    def _combine_dfs_by_date(self, list_of_df:list)->list:
        """
        Combine the df's     
        - `list_of_df`: a list of df's of transactions

        Returns a df with all the transactions
        """

        dates = self._get_list_of_unique_dates(list_of_df=list_of_df)

        list_of_combined_dfs = []

        for date in dates:

            combined_df = pd.DataFrame()

            for df in list_of_df:

                df_bank_name  = df.attrs['name'].split('_')[0]
                df_date = df.attrs['name'].split('_')[1]
                df_user = df.attrs['name'].split('_')[2].split('.')[0]

                if df_date == date:

                    if df_bank_name == 'cdc':

                        # Replace nan with blank string
                        df = df.replace(np.nan,'')

                        # Drop uncessary columns
                        df = df[['Timestamp (UTC)', 'Transaction Description', 'Amount']]

                        # Rename these columns
                        df = df.rename(columns={'Timestamp (UTC)':'Date', 'Transaction Description': 'Description' })

                        # Add user column
                        df['User'] = df_user

                        # Convert date column from string to datetime
                        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M:%S')

                    elif df_bank_name == 'ing':
                        
                        # Replace nan with blank string
                        df = df.replace(np.nan,'') 

                        # Combine Debit and Credit columns
                        df["Amount"] = (df['Credit'].astype(str) + df["Debit"].astype(str)).astype(float)

                        # Drop unecessary columns
                        df = df.drop(columns=['Credit', 'Debit', 'Balance']) 

                        # Add user column
                        df['User'] = df_user

                        # Convert date column from string to datetime
                        df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d %H:%M:%S')

                    # Union both df
                    combined_df = pd.concat((combined_df, df), axis=0)

                    # Convert date 
                    combined_df['Date'] = pd.to_datetime(combined_df['Date'], format='%Y-%m-%d %H:%M:%S')
                    
                    combined_df.attrs['name'] = df_date

                    # combined_df.to_csv(f'datecheck{df_date}')

                    list_of_combined_dfs.append(combined_df)

        return list_of_combined_dfs
       
    def _transform_df(self, df:pd.DataFrame)->pd.DataFrame:
        """
        Transform the df     
        - `df`: a df witha list of all transactions

        Returns a transformed df
        """

        # Filter for the month that is wanted
        start_date = str(pd.to_datetime(df.attrs['name'], format='%Y%m')) 
        end_date = str(pd.to_datetime(start_date, format='%Y-%m-%d') + pd.DateOffset(months=1))

        # df.to_csv(f"beforedatefilter{df.attrs['name']}.csv")

        df = df[(df['Date'] >= start_date) & (df['Date'] < end_date)]

        # df.to_csv(f"datefiler{df.attrs['name']}.csv")

        # Copy df to avoid warning error when filering
        df = df.copy()

        # Convert date column to string for excel
        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Sort by date ascending
        df = df.sort_values(by='Date', ascending=True)

        # Add new columns category
        df['Friendly Description'] = ''
        df['Category'] = ''
        df['Sub Category'] = ''

        # Populate Friendly Description column
        # Utilities        
        df.loc[df['Description'].str.contains('netflix', case=False), 'Friendly Description'] = 'Netflix'
        df.loc[df['Description'].str.contains('real debrid', case=False), 'Friendly Description'] = 'Real Debrid'
        df.loc[df['Description'].str.contains('spotify', case=False), 'Friendly Description'] = 'Spotify'
        df.loc[df['Description'].str.contains('synergy', case=False), 'Friendly Description'] = 'Synergy'
        df.loc[df['Description'].str.contains('tangerine', case=False), 'Friendly Description'] = 'Tangerine Internet'        
        df.loc[df['Description'].str.contains('telco', case=False), 'Friendly Description'] = 'Woolworths Mobile'
        df.loc[df['Description'].str.contains('youtube', case=False), 'Friendly Description'] = 'Youtube'       
                
        # Shopping
        df.loc[df['Description'].str.contains('aldi', case=False), 'Friendly Description'] = 'Aldi'
        df.loc[df['Description'].str.contains('amazon|amazon mktplc', case=False), 'Friendly Description'] = 'Amazon'     
        df.loc[df['Description'].str.contains('ams', case=False), 'Friendly Description'] = 'AMS'
        df.loc[df['Description'].str.contains('big w', case=False), 'Friendly Description'] = 'Big W'
        df.loc[df['Description'].str.contains('bunnings', case=False), 'Friendly Description'] = 'Bunnings'
        df.loc[df['Description'].str.contains('coles', case=False), 'Friendly Description'] = 'Coles'
        df.loc[df['Description'].str.contains('ebay', case=False), 'Friendly Description'] = 'eBay'
        df.loc[df['Description'].str.contains('five seasons fresh', case=False), 'Friendly Description'] = 'Five Seasons Fresh'
        df.loc[df['Description'].str.contains('family fresh market', case=False), 'Friendly Description'] = 'Family Fresh Market'
        df.loc[df['Description'].str.contains('iga', case=False), 'Friendly Description'] = 'IGA'        
        df.loc[df['Description'].str.contains('kmart', case=False), 'Friendly Description'] = 'KMart'
        df.loc[df['Description'].str.contains('ple computers', case=False), 'Friendly Description'] = 'PLE Computers'
        df.loc[df['Description'].str.contains('red dot', case=False), 'Friendly Description'] = 'Red Dot'        
        df.loc[df['Description'].str.contains('rise minimart', case=False), 'Friendly Description'] = 'Rise Minimart'
        df.loc[df['Description'].str.contains('rise supermar', case=False), 'Friendly Description'] = 'Rise Supermarket'
        df.loc[df['Description'].str.contains('subway', case=False), 'Friendly Description'] = 'Subway'
        df.loc[df['Description'].str.contains('target', case=False), 'Friendly Description'] = 'Target'
        df.loc[df['Description'].str.contains('the reject shop', case=False), 'Friendly Description'] = 'The Reject Shop'
        df.loc[df['Description'].str.contains('thingz gifts', case=False), 'Friendly Description'] = 'Thingz Gifts'
        df.loc[df['Description'].str.contains('woolworths', case=False), 'Friendly Description'] = 'Woolworths'    

        # Recreation & Entertainment
        df.loc[df['Description'].str.contains('adventure world|adventureworld', case=False), 'Friendly Description'] = 'Adventure World'
        df.loc[df['Description'].str.contains('ben & jerrys', case=False), 'Friendly Description'] = 'Ben & Jerrys'
        df.loc[df['Description'].str.contains('boost juice', case=False), 'Friendly Description'] = 'Boost Juice'
        df.loc[df['Description'].str.contains('Cafe Zamia', case=False), 'Friendly Description'] = 'Cafe Zamia'
        df.loc[df['Description'].str.contains('captain cook', case=False), 'Friendly Description'] = 'Captain Cook'
        df.loc[df['Description'].str.contains('chu bakery', case=False), 'Friendly Description'] = 'Chu Bakery'        
        df.loc[df['Description'].str.contains('cockburn ice arena', case=False), 'Friendly Description'] = 'Cockburn Ice Arena'
        df.loc[df['Description'].str.contains('cracovia club', case=False), 'Friendly Description'] = 'Cracovia Club'
        df.loc[df['Description'].str.contains('crown', case=False), 'Friendly Description'] = 'Crown'
        df.loc[df['Description'].str.contains('dominos', case=False), 'Friendly Description'] = 'Dominos'
        df.loc[df['Description'].str.contains('fry?d', case=False), 'Friendly Description'] = 'Fry?d'
        df.loc[df['Description'].str.contains('grilld', case=False), 'Friendly Description'] = 'Grilld'
        df.loc[df['Description'].str.contains('kfc', case=False), 'Friendly Description'] = 'KFC'        
        df.loc[df['Description'].str.contains('infinity care', case=False), 'Friendly Description'] = 'Kinky Lizard'
        df.loc[df['Description'].str.contains('nandos', case=False), 'Friendly Description'] = 'Nandos'
        df.loc[df['Description'].str.contains('maruzzella', case=False), 'Friendly Description'] = 'Maruzzella'
        df.loc[df['Description'].str.contains('menulog', case=False), 'Friendly Description'] = 'Menulog'
        df.loc[df['Description'].str.contains('muffin|muffin break', case=False), 'Friendly Description'] = 'Muffin Break'
        df.loc[df['Description'].str.contains('pinnacles dese', case=False), 'Friendly Description'] = 'Pinnacles Desert'
        df.loc[df['Description'].str.contains('polish club sikorski', case=False), 'Friendly Description'] = 'Polish Club Sikorski'
        df.loc[df['Description'].str.contains('rise pizza', case=False), 'Friendly Description'] = 'Rise Pizza'
        df.loc[df['Description'].str.contains('san churro', case=False), 'Friendly Description'] = 'San Churro'
        df.loc[df['Description'].str.contains('six senses', case=False), 'Friendly Description'] = 'Six Senses' 
        df.loc[df['Description'].str.contains('spur', case=False), 'Friendly Description'] = 'Spur' 
        df.loc[df['Description'].str.contains('tao', case=False), 'Friendly Description'] = 'Tao Cafe'
        df.loc[df['Description'].str.contains('tempayan', case=False), 'Friendly Description'] = 'Tempayan'
        df.loc[df['Description'].str.contains('the coffee club', case=False), 'Friendly Description'] = 'The Coffee Club'
        df.loc[df['Description'].str.contains('the local shack', case=False), 'Friendly Description'] = 'The Local Shack'
        df.loc[df['Description'].str.contains('ticketmaster|ticket master', case=False), 'Friendly Description'] = 'Ticketmaster'               

        # Transport
        df.loc[df['Description'].str.contains('ampol', case=False), 'Friendly Description'] = 'Ampol'
        df.loc[df['Description'].str.contains('atlas fuel', case=False), 'Friendly Description'] = 'Atlas'
        df.loc[df['Description'].str.contains('burswood car rentals', case=False), 'Friendly Description'] = 'Burswood Car Rentals'
        df.loc[df['Description'].str.contains('caltex', case=False), 'Friendly Description'] = 'Caltex'
        df.loc[df['Description'].str.contains('city of perth parking|cpp', case=False), 'Friendly Description'] = 'City of Perth Parking'
        df.loc[df['Description'].str.contains('on the run', case=False), 'Friendly Description'] = 'On The Run'
        df.loc[df['Description'].str.contains('puma energy', case=False), 'Friendly Description'] = 'Puma energy'
        df.loc[df['Description'].str.contains('uber', case=False), 'Friendly Description'] = 'Uber'
        df.loc[df['Description'].str.contains('united', case=False), 'Friendly Description'] = 'United'
        df.loc[df['Description'].str.contains('vibe', case=False), 'Friendly Description'] = 'Vibe'
        df.loc[df['Description'].str.contains('wilson parking', case=False), 'Friendly Description'] = 'Wilson Parking'

        # Accommodation
        df.loc[df['Description'].str.contains('deft rent', case=False), 'Friendly Description'] = 'M Property'

        # Healthcare
        df.loc[df['Description'].str.contains('chemistwarehouse|chemist warehouse', case=False), 'Friendly Description'] = 'Chemist Warehouse'
        df.loc[df['Description'].str.contains('hcfhealth', case=False), 'Friendly Description'] = 'HCF'
        df.loc[df['Description'].str.contains('pline ph', case=False), 'Friendly Description'] = 'Priceline Pharmacy'
        df.loc[df['Description'].str.contains('wan chang', case=False), 'Friendly Description'] = 'Wan Chang'
        df.loc[df['Description'].str.contains('wizard pharmacy', case=False), 'Friendly Description'] = 'Wizard Pharmacy'  
              
        
        # Populate categories column
        utilities = df['Friendly Description'].str.contains(
            'Netflix|'  
            'Real Debrid|'
            'Spotify|'
            'Synergy|'
            'Tangerine Internet|'                      
            'Woolworths Mobile|'
            'Youtube'
            , case=False)

        shopping = df['Friendly Description'].str.contains(            
            'Aldi|'
            'Amazon|'
            'AMS|'
            'Big W|'
            'Bunnings|'
            'Coles|'
            'eBay|'
            'Family Fresh Market|'
            'Five Seasons Fresh|'
            'IGA|'
            'KMart|'
            'PLE Computers|'
            'Red Dot|'
            'Rise Minimart|'
            'Rise Supermarket|'
            'Subway|'
            'Target|'
            'The Reject Shop|'            
            'Thingz Gifts|'
            'Woolworths'
            , case=False)
        
        recreationentertainment = df['Friendly Description'].str.contains(
            'Adventure World|'
            'Ben & Jerrys|'
            'Boost Juice|'
            'Cafe Zamia|'
            'Captain Cook|'
            'Chu Bakery|'
            'Cockburn Ice Arena|'
            'Cracovia Club|'
            'Crown|'
            'Dominos|'
            'Fry?d|'  
            'Grilld|'
            'KFC|'
            'Kinky Lizard|'
            'Maruzzella|'
            'Menulog|'
            'Muffin Break|'     
            'Nandos|'
            'Pinnacles Desert|'
            'Rise Pizza|' 
            'San Churro|'
            'Six Senses|'
            'Spur|'    
            'Tao Cafe|'
            'Tempayan|'                  
            'The Local Shack|'
            'The Coffee Club|'
            'Ticketmaster'
            , case=False)
        
        transport = df['Friendly Description'].str.contains(            
            'Ampol|'
            'Atlas|'
            'Burswood Car Rentals|'
            'Caltex|'
            'City of Perth Parking|'
            'On The Run|'
            'Puma Energy|'
            'Uber|'
            'United|'    
            'Vibe|'         
            'Wilson Parking'         
            , case=False)
        
        accommodation = df['Friendly Description'].str.contains(
            'M Property'
            , case=False)
        
        healthcare = df['Friendly Description'].str.contains(
            'Chemist Warehouse|'
            'HCF|'
            'Priceline Pharmacy|'
            'Wan Chang|'
            'Wizard Pharmacy'
            , case=False)
            
        df.loc[utilities, 'Category'] = 'Utilities'
        df.loc[shopping, 'Category'] = 'Shopping'
        df.loc[recreationentertainment, 'Category'] = 'Recreation & Entertainment'
        df.loc[transport, 'Category'] = 'Transport'
        df.loc[accommodation, 'Category'] = 'Accommodation'
        df.loc[healthcare, 'Category'] = 'Healthcare'

        # Populate Sub Category column
        df.loc[utilities, 'Sub Category'] = 'Phone|Electricity|Internet|Video|Audio'
        df.loc[shopping, 'Sub Category'] = 'Groceries|House & Home'
        df.loc[recreationentertainment, 'Sub Category'] = 'Food|Entertainment|Travel|Accomodation'
        df.loc[transport, 'Sub Category'] = 'Car Hire|Fuel|Parking|Car Wash'
        df.loc[accommodation, 'Sub Category'] = 'Rent'
        df.loc[healthcare, 'Sub Category'] = 'Health Insurance|Medication|Dentist|Doctor'

        return df
    
    def run(self)->list:
        
        list_of_transformed_dfs = []

        list_of_combined_dfs = self._combine_dfs_by_date(list_of_df=self.list_of_df)

        for combined_df in list_of_combined_dfs:
            transformed_df = self._transform_df(df=combined_df)
            list_of_transformed_dfs.append(transformed_df)
        
        return list_of_transformed_dfs
