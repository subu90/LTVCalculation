import json
from datetime import datetime
import heapq


"""
ingest(events , dataset ):

The ingest events reads all the types of events sent to it and flattens the events bases on all available fields for all 
events , the unavailable fields are assigned NoneType.


Eg: The below Dictionary 
{"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a43", "event_time": "2017-01-06T12:55:55.555Z", "customer_id": "96f55c7d8f42", "total_amount": "12.34 USD"}
is flattened to the list
['ORDER', 'NEW', '68d84e5d1a43', "2017-01-06T12:55:55.555Z", None, None,None, "96f55c7d8f42", None,None , None ,12.34 ]

The list contains all the keys available from the sample events.


"""

def ingest(events , dataset ):
    fields = ["type",
            "verb",
            "key",
            "event_time",
            "last_name",
            "adr_city",
            "adr_state",
            "customer_id",
            "tags",
            "camera_make",
            "camera_model",
            "total_amount"]
    for event in events :
        flattened_event = list()
        for field in fields:
            if field in event:
                flattened_event.append(event[field])
            else:
                flattened_event.append(None)
        dataset.append(flattened_event)

    return dataset


"""

TopXSimpleLTVCustomers(top_n, dataset):

Gets the n and the ingested data set as inputs
processes the stream data and stores the data in the following data structures

customer_weekly_metrics - A dictionary of aggregated metrics (store_visits, orders, order_amount,spend_per_visit)
                            partitioned at customer , year and week 
unique_customers - stores the unique set of customers
ltv_per_customer -  A dictionary of aggregated metrics (total_store_visits, total_orders, total_order_amount,
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



Performance boosts :

1) lTV is being updated per each customer on the fly using weighted average while calculating 
aggregates at the customer level which saves another whole iteration of the customer data just for calculating LTV.
2) Top N is calculated on aggregated data sorted using heap which is better in performance the sorting the 
entire customer events in case of multi-millions events


Future Improvements:

1) Can be enhanced for Exception Handling

2) Can be rewritten in an object oriented way , if required 


"""


def TopXSimpleLTVCustomers(top_n , dataset):
    customer_weekly_metrics = dict()
    unique_customers = set()
    ltv_per_customer = dict()
    year_week_tracker = dict()
    weeks = 52
    years = 10
    for event in dataset:
        event_type = event[0]
        # Bucket customer to unknown if the value is not passed
        event_key = event[2] if event[3] is not None else 'UNKNOWN'
        event_datetime = event[3]
        order_amount = float(event[11].split()[0]) if event[11] is not None else 0
        # Skip the event if datetime is None
        if event_datetime is None:
            continue

        dateobject = datetime.strptime(event_datetime, '%Y-%m-%dT%H:%M:%S.%fZ')

        # get the year and week number from the timestamp to mark the partitions
        year , week_number = map(int , dateobject.strftime('%Y %W').split())

        if year not in year_week_tracker:
            year_week_tracker[year] = dict(min_week= None, max_week= None)

        else:
            if year_week_tracker[year]["min_week"] is None or week_number < year_week_tracker[year]["min_week"]:
                year_week_tracker[year]["min_week"] = week_number

            if year_week_tracker[year]["max_week"] is None or week_number > year_week_tracker[year]["max_week"]:
                year_week_tracker[year]["max_week"] = week_number

        # Bucket customer to unknown if the value is not passed
        if event_type == 'CUSTOMER' :
            customer_id = event_key
        elif event_type is None:
            customer_id = 'UNKNOWN'
        else:
            customer_id = event[7]

        # Maintains the unique customers
        unique_customers.add(customer_id)

        # Add the weekly metrics for the customer
        if (customer_id , year , week_number) not in customer_weekly_metrics:
            customer_weekly_metrics[(customer_id, year, week_number)] = dict(site_visits=0, orders=0, order_amount=0.0,
                                                                             spend_per_visit=0.0)
        if event_type == 'SITE_VISIT' :
            customer_weekly_metrics[(customer_id, year, week_number)]["site_visits"]+=1
        elif event_type == 'ORDER':
            customer_weekly_metrics[(customer_id, year, week_number)]["orders"] += 1
            customer_weekly_metrics[(customer_id, year, week_number)]["order_amount"] += order_amount
        if customer_weekly_metrics[(customer_id, year, week_number)]["site_visits"] == 0:
            customer_weekly_metrics[(customer_id, year, week_number)]["spend_per_visit"] = 0
        else:
            customer_weekly_metrics[(customer_id, year, week_number)]["spend_per_visit"] = \
                (customer_weekly_metrics[(customer_id, year, week_number)]["order_amount"] /
                 customer_weekly_metrics[(customer_id, year, week_number)]["site_visits"])


# Fill the unavailable weeks for the customers om the partition based on the min and max range for each year
    for customer in unique_customers:
        for year in year_week_tracker:
            for week in range(year_week_tracker[year]["min_week"], year_week_tracker[year]["max_week"] + 1):
                if (customer , year , week ) not in customer_weekly_metrics:
                    customer_weekly_metrics[(customer ,year , week)] = dict(site_visits=0, orders=0, order_amount=0.0,
                                                                             spend_per_visit=0.0)


# Store overall metrics at the customer level and Calculate LTV for each  user
    for customer , metrics in customer_weekly_metrics.items():
        customer_id = customer[0]
        if customer_id not in ltv_per_customer :
            ltv_per_customer[customer_id] = dict(total_site_visits=0, total_orders=0, total_order_amount=0.0,
                                                 avg_spend_per_visit=0.0, total_weeks=0, ltv=0)

        # calculating LTV Dynamically as the data is iterated
        # noinspection PyTypeChecker
        ltv_per_customer[customer_id]["ltv"] = ((ltv_per_customer[customer_id]["total_order_amount"] +
                                                 customer_weekly_metrics[customer]["order_amount"]) /
                                                (ltv_per_customer[customer_id]["total_weeks"] + 1) ) * weeks * years

        ltv_per_customer[customer_id]["total_site_visits"] += customer_weekly_metrics[customer]["site_visits"]
        ltv_per_customer[customer_id]["total_orders"] += customer_weekly_metrics[customer]["orders"]
        ltv_per_customer[customer_id]["total_order_amount"] += customer_weekly_metrics[customer]["order_amount"]

        if ltv_per_customer[customer_id]["total_site_visits"] == 0:
            ltv_per_customer[customer_id]["avg_spend_per_visit"] = 0

        else:
            ltv_per_customer[customer_id]["avg_spend_per_visit"] = (ltv_per_customer[customer_id]["total_order_amount"] /
                                                                    ltv_per_customer[customer_id]["total_site_visits"])
        ltv_per_customer[customer_id]["total_weeks"] += 1

    # Make the top n between 0 to 500
    if top_n > len(unique_customers):
        top_n = len(unique_customers)
    elif top_n > 500:
        top_n = 500

    # Sort the customers on LTV using the heap algorithm to improve performance
    top_ltvs = heapq.nlargest(top_n, ltv_per_customer.items(), key=lambda x: x[1]["ltv"])

    return top_ltvs

def main():

# List to store the ingested events
    dataset = []
    print("Ingesting Data...\n")
    input_reader= open('../input/input.json')
    input_str = input_reader.read()
    events = json.loads(input_str)
    dataset = ingest (events , dataset )

    print("Computing LTVs...\n")
    top_ltvs = TopXSimpleLTVCustomers(4 , dataset)

    print("Writing Output to Top_Ltvs.csv...\n")
    with open('../output/Top_Ltvs.csv', 'w') as fwriter:
        fwriter.write('customer' + ','+'ltv'+"\n")
        for customer, metrics in top_ltvs:
            fwriter.write(customer +","+str(format(metrics["ltv"], '.2f'))+"\n")


if __name__ == "__main__" :
    main()

