from datetime import datetime as dt, timedelta
import pandas as pd
import numpy as np
# pd.options.mode.copy_on_write = True

class Transform():

    def __init__(self, list_of_df:list, date_filter:str): 
        self.list_of_df = list_of_df
        self.date_filter = date_filter

    def _append_dfs(self, list_of_df:list)->pd.DataFrame:
        """
        Append the df's     
        - `list_of_df`: a list of df's of transactions

        Returns a df with all the transactions
        """

        combined_df = pd.DataFrame()

        for df in list_of_df:

            if df.attrs['name'] == 'cdc':

                # Replace nan with blank string
                df = df.replace(np.nan,'')

                # Drop uncessary columns
                df = df[['Timestamp (UTC)', 'Transaction Description', 'Amount']]

                # Rename these columns
                df = df.rename(columns={'Timestamp (UTC)':'Date', 'Transaction Description': 'Description' })

                # Convert date column from string to datetime
                df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M:%S')

            elif df.attrs['name'] == 'ing':
                
                # Replace nan with blank string
                df = df.replace(np.nan,'') 

                # Combine Debit and Credit columns
                df["Amount"] = (df['Credit'].astype(str) + df["Debit"].astype(str)).astype(float)

                # Drop unecessary columns
                df = df.drop(columns=['Credit', 'Debit', 'Balance']) 

                # Convert date column from string to datetime
                df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d %H:%M:%S')

            # Union both df
            combined_df = pd.concat((combined_df, df), axis=0)

            # Convert date 
            combined_df['Date'] = pd.to_datetime(combined_df['Date'], format='%Y-%m-%d %H:%M:%S')
        
        return combined_df
    
    def _transform_df(self, df:pd.DataFrame, date_filter:str)->pd.DataFrame:
        """
        Transform the df     
        - `df`: a df witha list of all transactions
        - `date_filer': a date to filter for

        Returns a transformed df
        """

        # Filter for the month that is wanted
        start_date = date_filter
        end_date = str(pd.to_datetime(start_date, format='%Y-%m-%d') + pd.DateOffset(months=1))
        df = df[(df['Date'] > start_date) & (df['Date'] < end_date)]

        # Copy df to avoid warning error when filering
        df = df.copy()

        # Convert date column to string for excel
        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Sort by date ascending
        df = df.sort_values(by='Date', ascending=True)

        # Add new columns category
        df['FriendlyDescription'] = ''
        df['Category'] = ''
        df['Sub Category'] = ''

        # Populate FriendlyDescription column
        # Utilities        
        df.loc[df['Description'].str.contains('netflix', case=False), 'FriendlyDescription'] = 'Netflix'
        df.loc[df['Description'].str.contains('real debrid', case=False), 'FriendlyDescription'] = 'Real Debrid'
        df.loc[df['Description'].str.contains('spotify', case=False), 'FriendlyDescription'] = 'Spotify'
        df.loc[df['Description'].str.contains('synergy', case=False), 'FriendlyDescription'] = 'Synergy'
        df.loc[df['Description'].str.contains('tangerine', case=False), 'FriendlyDescription'] = 'Tangerine Internet'        
        df.loc[df['Description'].str.contains('telco', case=False), 'FriendlyDescription'] = 'Woolworths Mobile'
        df.loc[df['Description'].str.contains('youtube', case=False), 'FriendlyDescription'] = 'Youtube'       
                
        # Shopping
        df.loc[df['Description'].str.contains('aldi', case=False), 'FriendlyDescription'] = 'Aldi'
        df.loc[df['Description'].str.contains('amazon mktplc', case=False), 'FriendlyDescription'] = 'Amazon'     
        df.loc[df['Description'].str.contains('ams', case=False), 'FriendlyDescription'] = 'AMS'
        df.loc[df['Description'].str.contains('big w', case=False), 'FriendlyDescription'] = 'Big W'
        df.loc[df['Description'].str.contains('coles', case=False), 'FriendlyDescription'] = 'Coles'
        df.loc[df['Description'].str.contains('ebay', case=False), 'FriendlyDescription'] = 'eBay'
        df.loc[df['Description'].str.contains('five seasons fresh', case=False), 'FriendlyDescription'] = 'Five Seasons Fresh'
        df.loc[df['Description'].str.contains('family fresh market', case=False), 'FriendlyDescription'] = 'Family Fresh Market'
        df.loc[df['Description'].str.contains('iga', case=False), 'FriendlyDescription'] = 'IGA'        
        df.loc[df['Description'].str.contains('kmart', case=False), 'FriendlyDescription'] = 'KMart'
        df.loc[df['Description'].str.contains('ple computers', case=False), 'FriendlyDescription'] = 'PLE Computers'
        df.loc[df['Description'].str.contains('red dot', case=False), 'FriendlyDescription'] = 'Red Dot'        
        df.loc[df['Description'].str.contains('rise minimart', case=False), 'FriendlyDescription'] = 'Rise Minimart'
        df.loc[df['Description'].str.contains('rise supermar', case=False), 'FriendlyDescription'] = 'Rise Supermarket'
        df.loc[df['Description'].str.contains('subway', case=False), 'FriendlyDescription'] = 'Subway'
        df.loc[df['Description'].str.contains('target', case=False), 'FriendlyDescription'] = 'Target'
        df.loc[df['Description'].str.contains('the reject shop', case=False), 'FriendlyDescription'] = 'The Reject Shop'
        df.loc[df['Description'].str.contains('thingz gifts', case=False), 'FriendlyDescription'] = 'Thingz Gifts'
        df.loc[df['Description'].str.contains('woolworths', case=False), 'FriendlyDescription'] = 'Woolworths'    

        # Recreation & Entertainment
        df.loc[df['Description'].str.contains('adventure world|adventureworld', case=False), 'FriendlyDescription'] = 'Adventure World'
        df.loc[df['Description'].str.contains('ben & jerrys', case=False), 'FriendlyDescription'] = 'Ben & Jerrys'
        df.loc[df['Description'].str.contains('Cafe Zamia', case=False), 'FriendlyDescription'] = 'Cafe Zamia'
        df.loc[df['Description'].str.contains('captain cook', case=False), 'FriendlyDescription'] = 'Captain Cook'
        df.loc[df['Description'].str.contains('chu bakery', case=False), 'FriendlyDescription'] = 'Chu Bakery'        
        df.loc[df['Description'].str.contains('cockburn ice arena', case=False), 'FriendlyDescription'] = 'Cockburn Ice Arena'
        df.loc[df['Description'].str.contains('cracovia club', case=False), 'FriendlyDescription'] = 'Cracovia Club'
        df.loc[df['Description'].str.contains('crown', case=False), 'FriendlyDescription'] = 'Crown'
        df.loc[df['Description'].str.contains('dominos', case=False), 'FriendlyDescription'] = 'Dominos'
        df.loc[df['Description'].str.contains('fry?d', case=False), 'FriendlyDescription'] = 'Fry?d'
        df.loc[df['Description'].str.contains('grilld', case=False), 'FriendlyDescription'] = 'Grilld'
        df.loc[df['Description'].str.contains('kfc', case=False), 'FriendlyDescription'] = 'KFC'        
        df.loc[df['Description'].str.contains('infinity care', case=False), 'FriendlyDescription'] = 'Kinky Lizard'
        df.loc[df['Description'].str.contains('nandos', case=False), 'FriendlyDescription'] = 'Nandos'
        df.loc[df['Description'].str.contains('maruzzella', case=False), 'FriendlyDescription'] = 'Maruzzella'
        df.loc[df['Description'].str.contains('menulog', case=False), 'FriendlyDescription'] = 'Menulog'
        df.loc[df['Description'].str.contains('muffin|muffin break', case=False), 'FriendlyDescription'] = 'Muffin Break'
        df.loc[df['Description'].str.contains('pinnacles dese', case=False), 'FriendlyDescription'] = 'Pinnacles Desert'
        df.loc[df['Description'].str.contains('rise pizza', case=False), 'FriendlyDescription'] = 'Rise Pizza'
        df.loc[df['Description'].str.contains('san churro', case=False), 'FriendlyDescription'] = 'San Churro'
        df.loc[df['Description'].str.contains('six senses', case=False), 'FriendlyDescription'] = 'Six Senses' 
        df.loc[df['Description'].str.contains('spur', case=False), 'FriendlyDescription'] = 'Spur' 
        df.loc[df['Description'].str.contains('tao', case=False), 'FriendlyDescription'] = 'Tao Cafe'
        df.loc[df['Description'].str.contains('tempayan', case=False), 'FriendlyDescription'] = 'Tempayan'
        df.loc[df['Description'].str.contains('the coffee club', case=False), 'FriendlyDescription'] = 'The Coffee Club'
        df.loc[df['Description'].str.contains('the local shack', case=False), 'FriendlyDescription'] = 'The Local Shack'
        df.loc[df['Description'].str.contains('ticketmaster|ticket master', case=False), 'FriendlyDescription'] = 'Ticketmaster'               

        # Transport
        df.loc[df['Description'].str.contains('ampol', case=False), 'FriendlyDescription'] = 'Ampol'
        df.loc[df['Description'].str.contains('burswood car rentals', case=False), 'FriendlyDescription'] = 'Burswood Car Rentals'
        df.loc[df['Description'].str.contains('caltex', case=False), 'FriendlyDescription'] = 'Caltex'
        df.loc[df['Description'].str.contains('city of perth parking|cpp', case=False), 'FriendlyDescription'] = 'City of Perth Parking'
        df.loc[df['Description'].str.contains('on the run', case=False), 'FriendlyDescription'] = 'On The Run'
        df.loc[df['Description'].str.contains('puma energy', case=False), 'FriendlyDescription'] = 'Puma energy'
        df.loc[df['Description'].str.contains('united', case=False), 'FriendlyDescription'] = 'United'
        df.loc[df['Description'].str.contains('vibe', case=False), 'FriendlyDescription'] = 'Vibe'
        df.loc[df['Description'].str.contains('wilson parking', case=False), 'FriendlyDescription'] = 'Wilson Parking'

        # Accommodation
        df.loc[df['Description'].str.contains('deft rent', case=False), 'FriendlyDescription'] = 'M Property'

        # Healthcare
        df.loc[df['Description'].str.contains('chemistwarehouse', case=False), 'FriendlyDescription'] = 'Chemist Warehouse'
        df.loc[df['Description'].str.contains('hcfhealth', case=False), 'FriendlyDescription'] = 'HCF'
        df.loc[df['Description'].str.contains('pline ph', case=False), 'FriendlyDescription'] = 'Priceline Pharmacy'
        df.loc[df['Description'].str.contains('wizard pharmacy', case=False), 'FriendlyDescription'] = 'Wizard Pharmacy'        
        
        # Populate categories column
        utilities = df['FriendlyDescription'].str.contains(
            'Netflix|'  
            'Real Debrid|'
            'Spotify|'
            'Synergy|'
            'Tangerine Internet|'                      
            'Woolworths Mobile|'
            'Youtube'
            , case=False)

        shopping = df['FriendlyDescription'].str.contains(            
            'Aldi|'
            'Amazon|'
            'AMS|'
            'Big W|'
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
        
        recreationentertainment = df['FriendlyDescription'].str.contains(
            'Adventure World|'
            'Ben & Jerrys|'
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
        
        transport = df['FriendlyDescription'].str.contains(            
            'Ampol|'
            'Burswood Car Rentals|'
            'Caltex|'
            'City of Perth Parking|'
            'On The Run|'
            'Puma Energy|'
            'United|'    
            'Vibe|'         
            'Wilson Parking'         
            , case=False)
        
        accommodation = df['FriendlyDescription'].str.contains(
            'M Property'
            , case=False)
        
        healthcare = df['FriendlyDescription'].str.contains(
            'Chemist Warehouse|'
            'HCF|'
            'Priceline Pharmacy|'
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
    
    def run(self)->pd.DataFrame:
        
        combined_df = self._append_dfs(list_of_df=self.list_of_df)

        df = self._transform_df(df=combined_df, date_filter=self.date_filter)

        return df
