# Owner: nehal.kumar@mydeal.com.au

# Import required packages

import configurations as config
import pandas as pd
import numpy as np
import os
import json
import requests

import datetime 
from datetime import datetime, timedelta
from pytz import timezone
import dateutil.relativedelta
import time

from google.cloud import bigquery

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.campaign import Campaign

import logging
if not logging.getLogger().handlers:
    logging.basicConfig(level = logging.INFO)

PROJECT_ID = config.PROJECT_ID
DATASET_ID = config.DATASET_ID
TABLE_ID = config.TABLE_ID
app_id = config.APP_ID
app_secret = config.APP_SECRET
access_token = os.environ['ACCESS_TOKEN']
ad_account_id = config.ACCOUNT_ID


client = bigquery.Client()

class LibFacebook:
    """Initializes a new Facebook object

    Args:
        app_id (str): Unique ID for the selected App
        app_secret (int): Secret value for the selected App
        access_token (str): Access token for Marketing API with read permissions
        account_id (str): Facebook business manager account id
 
    """
    def __init__(self,app_id,app_secret,access_token,ad_account_id):
        FacebookAdsApi.init(app_id=app_id, app_secret=app_secret, access_token=access_token, api_version ='v16.0')
        self.account = AdAccount(ad_account_id)  
    def get_ads_insights(self,start_date,end_date):
        logging.info("get_ads_insights is called") 
        fields_ads=[
        AdsInsights.Field.date_start, # The start date for your data. This is controlled by the date range you've selected for your reporting view.
        AdsInsights.Field.campaign_name, # The name of the ad campaign you're viewing in reporting. Your campaign contains ad sets and ads
        AdsInsights.Field.campaign_id, # The unique ID number of the ad campaign you're viewing in reporting. Your campaign contains ad sets and ads
        AdsInsights.Field.adset_id,   #The unique ID of the ad set you're viewing in reporting. An ad set is a group of ads that share the same budget, schedule, delivery optimization and targeting
        AdsInsights.Field.attribution_setting, #The default attribution window to be used when attribution result is calculated. Each ad set has its own attribution setting value. The attribution setting for campaign or account is calculated based on existing ad sets
        AdsInsights.Field.adset_name, #	The name of the ad set you're viewing in reporting. An ad set is a group of ads that share the same budget, schedule, delivery optimization and targeting.
        AdsInsights.Field.ad_id, # The unique ID of the ad you're viewing in reporting
        AdsInsights.Field.ad_name, # The name of the ad you're viewing in reporting
        AdsInsights.Field.clicks, # The number of clicks on your ads
        AdsInsights.Field.cpc, # The average cost for each click (all)
        AdsInsights.Field.cpm, # The average cost for 1,000 impressions
        AdsInsights.Field.frequency,# The average number of times each person saw your ad. This metric is estimated
        AdsInsights.Field.impressions,# The number of times your ads were on screen
        AdsInsights.Field.objective, # The objective reflecting the goal you want to achieve with your advertising. It may be different from the selected objective of the campaign in some cases
        AdsInsights.Field.reach, # The number of people who saw your ads at least once. Reach is different from impressions, which may include multiple views of your ads by the same people. This metric is estimated
        AdsInsights.Field.spend, # The estimated total amount of money you've spent on your campaign, ad set or ad during its schedule. This metric is estimated               
        AdsInsights.Field.website_purchase_roas, # The total return on ad spend (ROAS) from website purchases. This is based on the value of all conversions recorded by the Facebook pixel on your website and attributed to your ads
        AdsInsights.Field.action_values, # The total value of all conversions attributed to your ads
        AdsInsights.Field.actions  #The total number of actions people took that are attributed to your ads. Actions may include engagement, clicks or conversions                     
        ]
        params_ads = {'level': 'ad', # enum {ad, adset, campaign, account} : we have requested the lowest level
                    'time_range': {'since':str(start_date), 'until':str(end_date)}, # {'since':YYYY-MM-DD,'until':YYYY-MM-DD}
                    'time_increment' : 1, # If it is an integer, it is the number of days from 1 to 90. After you pick a reporting period by using time_range or date_preset, you may choose to have the results for the whole period, or have results for smaller time slices. If "all_days" is used, it means one result set for the whole period. If "monthly" is used, you get one result set for each calendar month in the given period. Or you can have one result set for each N-day period specified by this param. This param is ignored if time_ranges is specified
                    'use_unified_attribution_setting':True #use_unified_attribution_setting - When this parameter is set to true, your ads results will be shown using unified attribution settings defined at ad set level and parameter   
                        }
            
        async_job = self.account.get_insights(fields=fields_ads,params=params_ads, is_async = True)
        async_job.api_get()
        async_job_status = async_job[AdReportRun.Field.async_status]
        aysnc_job_completion = async_job[AdReportRun.Field.async_percent_completion]
        while async_job_status != "Job Completed" or aysnc_job_completion < 100:
            state = async_job.api_get()
            time.sleep(1)
            async_job_status = dict(state)['async_status']
            aysnc_job_completion = dict(state)['async_percent_completion'] 
        ads_insights = async_job.get_result()
        obj = ads_insights  # obj is a Facebook cursor object with its own set of methods and attributes
        result_arr = []
        for i in obj:
            datadict = {}
            datadict["date"] = i.get("date_start")
            datadict["campaign_id"] = i.get("campaign_id")
            datadict["campaign_name"] = i.get("campaign_name")
            datadict["adset_id"] = i.get("adset_id")
            datadict["adset_name"] = i.get("adset_name")
            datadict["attribution_setting"] = i.get("attribution_setting")
            datadict["ad_id"] = i.get("ad_id")
            datadict["ad_name"] = i.get("ad_name")
            datadict["clicks"] = i.get("clicks")
            datadict["cpc"] = i.get("cpc")
            datadict["cpm"] = i.get("cpm")
            datadict["frequency"] = i.get("frequency")
            datadict["impressions"] = i.get("impressions")
            datadict["objective"] = i.get("objective")
            datadict["reach"] = i.get("reach")
            datadict["spend"] = i.get("spend")
            actions = i.get("actions")

            purchase = False
            if actions is not None:
                for index,line in enumerate(actions):
                    if "offsite_conversion.fb_pixel_view_content" in line['action_type'].lower():
                        datadict["website_content_views"] = float(line['value'])
                        break
                    else:
                        datadict["website_content_views"] = np.nan
                
                for index,line in enumerate(actions):       
                    if "offsite_conversion.fb_pixel_add_to_cart" in line['action_type'].lower():
                        datadict["adds_to_cart"] = float(line['value'])
                        break
                    else:
                        datadict["adds_to_cart"] = np.nan
                        
                for index,line in enumerate(actions):        
                    if "offsite_conversion.fb_pixel_purchase" in line['action_type'].lower():
                        datadict["purchases"] = float(line['value'])
                        purchase = True 
                        break
                    else:
                        datadict["purchases"] = np.nan

            else:
                datadict["website_content_views"] = np.nan
                datadict["adds_to_cart"] = np.nan
                datadict["purchases"] = np.nan

            if(purchase):
                action_values = i.get("action_values")
                if action_values is not None:
                    for index,line in enumerate(action_values):
                        if "offsite_conversion.fb_pixel_purchase" in line['action_type'].lower():
                            datadict["purchase_conversion_value"] = float(line['value'])
                            break
                        else:
                            datadict["purchase_conversion_value"] = np.nan
            else:
                datadict["purchase_conversion_value"] = np.nan
                    
            if(purchase):
                website_purchase_roas = i.get("website_purchase_roas")
                if website_purchase_roas is not None:
                    for index,line in enumerate(website_purchase_roas):
                        if "offsite_conversion.fb_pixel_purchase" in line['action_type'].lower():
                            datadict["purchase_roas"]=float(line['value'])
                            break
            else:
                datadict["purchase_roas"] = np.nan
                                                

            result_arr.append(datadict)
        return result_arr
    
def facebook_ads_data(NewAccount,start_date,end_date):
    """
    This function is used to convert extracted Facebook Ads level data into a pandas dataframe

    Args: 
    NewAccount(Facebook Object) : Facebook class initialized
    start_date(date): The start of reporting period
    end_date(date): The end of reporting period

    Output:
    df_ads(dataframe):  Dataframe containing Facebook Ads Stats
    
    """
    logging.info("Function: facebook_ads_data")
    logging.info(f"Dates received {start_date}  AND {end_date}")
    try:
        result_arr = NewAccount.get_ads_insights(start_date,end_date)
        df_ads = pd.DataFrame() 
        df_ads=pd.DataFrame(result_arr)
        logging.info("get_ads_insights SUCCESS")
        return(df_ads)
    except Exception as e:
        logging.error(f"Failed in get_ads_insights call:{e}")
    

def write_data_to_BQ(df):    
    """
    This function is used to write data into BQ tables

    Args: 
    df(dataframe): Dataframe containing Facebook Ads Stats

    Output: 
    True | False 
    """
    my_df = df
    table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    logging.info(f"writing data to:{table}")
    try:
        job = client.load_table_from_dataframe(my_df,table)
        job.result()
        logging.info("records written to BQ table")
        return True
    except Exception as e:
        logging.error(f"Failed to write data to table:{e}")
        return False
        

def check_latest_ads_date():
    """
    This function is used to check the latest data available in prod BQ table 

    Args: 

    Output: 
    df_bq_ads_date(date) : The latest available date
    """
    sql_str = "select distinct date from `{project_id}.{dataset_id}.{table_id}` order by date desc limit 1"
    sql_str = sql_str.format(project_id = PROJECT_ID, dataset_id = DATASET_ID, table_id = TABLE_ID)
    df_bq_ads = client.query(sql_str ).to_dataframe()
    df_bq_ads_date = df_bq_ads['date'][0]
    return(df_bq_ads_date)

def social_media_fb(event, context):

    try:
        logging.info("Function: social_media_fb")
        #the below timezone and time setting is needed when we use time_range rather than date_preset 
        fmt = f"%Y-%m-%d"
        timezonelist = ['Australia/Melbourne']
        for zone in timezonelist:
            now_time = datetime.now(timezone(zone))
            start_date = now_time - timedelta(7) # 7 days back
            end_date = now_time - timedelta(0)#till today

        NewAccount = LibFacebook(app_id, app_secret, access_token, ad_account_id)
        
        #Check latest ads date from BQ Table
        df_bq_ads_date = check_latest_ads_date()
        logging.info(f"df_bq_ads_date:{df_bq_ads_date}")
        
            
        #Get the Facebook ADS data in batches 
        chunks=[]
        while start_date <= end_date:
            new_start_date = start_date 
            new_end_date = start_date + timedelta(3)
            data = facebook_ads_data(NewAccount,datetime.strftime(new_start_date,fmt),datetime.strftime(new_end_date,fmt))
            chunks.append(data)
            df_ads = pd.concat(chunks)
            start_date = start_date +  timedelta(4)
            logging.info(f"new starting date,{start_date}")
        
        df_ads[['ad_id','adset_id','campaign_id']] = df_ads[['ad_id','adset_id','campaign_id']].astype(int)
        df_ads[['frequency','impressions','reach','spend','clicks','cpc','cpm']]=df_ads[['frequency','impressions','reach','spend','clicks','cpc','cpm']].astype(float)

        # re-arrange column names 
        df_ads =df_ads[['date','ad_id','ad_name','campaign_id','campaign_name','adset_id','adset_name','attribution_setting',
                    'frequency','impressions','reach','spend','objective','clicks','cpc','cpm',
                    'website_content_views','adds_to_cart','purchases','purchase_conversion_value','purchase_roas']]
        
        
        df_ads["cost_per_purchase"]= df_ads["spend"]/df_ads["purchases"]
        df_ads_starting_date = df_ads['date'].iloc[0] # First row date
        df_ads_ending_date = df_ads['date'].iloc[-1] # Last row date
        logging.info(f"Total Ads items returned by API-  {len(df_ads)}")


        date_1 = df_ads_starting_date
        date_2 = df_ads_ending_date
        sql_str = f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` where date between '{date_1}' and '{date_2}'"
        job= client.query(sql_str)
        logging.info(job.result())
        logging.info("step_2-Writing Ads data again")
        write_data_to_BQ(df_ads)
    except Exception as e:
        logging.error(f"Check Logs: {e}")
    

if __name__ == "__main__":
    social_media_fb(event=None, context=None)
