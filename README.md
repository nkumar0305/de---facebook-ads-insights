# Facebook Ads Insights Data Pipeline

## Description 
Code to extract Facebook ads data is housed on Google Cloud Function 1st Gen. The cloud function is triggered by Cloud Scheduler every 4 hours through Google Pub/Sub

## Facebook Reqirements 
- Navigate to Facebook Apps [here](https://developers.facebook.com/apps/615864062906887/settings/basic/?business_id=722546771184702)
- Choose MyDealBulkAPI app 

  > Name: MyDealBulkAPI
  >
  > Mode:Live
  >
  > Type:Business
  >
  > Business:MyDeal.com.au
  > 
  > APP ID : 615864062906887
  > 
  > APP_SECRET: ce85de03ff1863d6c21636dedec91574
  >
  > Account ID : 648849926128939

- Generate Access Token : Products --> Marketing API --> Tools --> Get Access Token(Permissions: ads_management,ads_read,read_insights)

Note : The access token are valid for 60 days if unused



