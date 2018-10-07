# LTVCalculation
Ingest stream data and calculate LTV with python (No Pandas / Spark )

ingest(events , dataset ):

The ingest events reads all the types of events sent to it and flattens the events bases on all available fields for all 
events , the unavailable fields are assigned NoneType.


Eg: The below Dictionary 
{"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a43", "event_time": "2017-01-06T12:55:55.555Z", "customer_id": "96f55c7d8f42", "total_amount": "12.34 USD"}
is flattened to the list
['ORDER', 'NEW', '68d84e5d1a43', "2017-01-06T12:55:55.555Z", None, None,None, "96f55c7d8f42", None,None , None ,12.34 ]

The list contains all the keys available from the sample events.


TopXSimpleLTVCustomers(top_n, dataset):

-Gets the n and the ingested data set as inputs

-processes the stream data and stores the data in the following data structures

-customer_weekly_metrics - A dictionary of aggregated metrics (store_visits, orders, order_amount,spend_per_visit)
                            partitioned at customer , year and week 

-unique_customers - stores the unique set of customers

-ltv_per_customer -  A dictionary of aggregated metrics (total_store_visits, total_orders, total_order_amount,
                                                        total_weeks, ltv) per each customer
Assumptions :                                                        
1) Min and max week are calculated within the available date range to get the LTV for that range
    eg : If we have two years of incomplete data the ranges for those years are calculated within 
    {2017: {'min_week': 1, 'max_week': 13}, 2018: {'min_week': 13, 'max_week': 13}}
    Notice the total weeks are calculated as 14 ( 13 in 2017 and 1 in 2018)
    [('96f55c7d8f42', {'total_store_visits': 6, 'total_orders': 6, 'total_order_amount': 78.04, 'total_weeks': 14, 'ltv': 2898.6285714285714}),
     ('96f55c7d8f44', {'total_store_visits': 5, 'total_orders': 5, 'total_order_amount': 71.7, 'total_weeks': 14, 'ltv': 2663.1428571428573})]
     
     In the real world this will not be an issue because the data will be continuous 

                                                        
2) Weeks with no events for the customer are assigned 0 for the respective metrics 
    eg: User with one order on week 13 will have ltv calculated for that whole range
    
     In the real world this will not be an issue because the data will be usually continuous 


Performance boosts :

1) lTV is being updated per each customer on the fly using weighted average while calculating 
aggregates at the customer level which saves another whole iteration of the customer data just for calculating LTV.

2) Top N is calculated on aggregated data sorted using heap which is better in performance the sorting the 
entire customer events in case of multi-millions events.

Future Improvements:

1) Can be enhanced for Exception Handling.

2) Can be rewritten in an object oriented way , if required.


#STEPS TO RUN

environment : Pyhton 3.6

Simply Navigate to src folder , run the following command

python LTV_sfly.py

output file : output/Top_Ltvs.csv




