from google.ads.googleads.client import GoogleAdsClient
import pandas as pd
from google.protobuf.json_format import MessageToDict

# https://github.com/googleads/google-ads-python/tree/0739716d5bea93f6242a4a8191419caa6a0b5b0c
# load google-ads.yaml for authentication
# get data from Google Ads
def get_ads(client, customer_id=None, query=None):
    ga_service = client.get_service("GoogleAdsService")

    # GAQL query
    # https://developers.google.com/google-ads/api/fields/v14/campaign
    
    if query is None:
        query =  """
            SELECT landing_page_view.unexpanded_final_url, metrics.impressions, metrics.clicks, metrics.cost_micros, segments.date
            FROM landing_page_view
            WHERE segments.date DURING LAST_7_DAYS 
            AND metrics.cost_micros > 0
            """
        
    # customer_id is optional. If not specified, the authenticated user's customer_id is used.
    # google ads account id, can be found in the UI
    if customer_id is None:
        customer_id = client.login_customer_id
        
    # issue a search request using streaming
    response = ga_service.search(customer_id=customer_id, query=query)

    # iterate over all rows in all messages and prints the requested field values for the ad in each row
    dictobj = MessageToDict(response._pb)
    df = pd.json_normalize(dictobj,record_path=['results'])
    return df




if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    client = GoogleAdsClient.load_from_storage("google-ads.yaml")
    df= get_ads(client)
    
