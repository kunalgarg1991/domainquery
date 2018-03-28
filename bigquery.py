# always run 
# export GOOGLE_APPLICATION_CREDENTIALS="/Users/Kunal/Downloads/Reporting-fb5b7e0aa2c7.json" 
# before executing the script


"""Command-line application to perform an asynchronous query in BigQuery.
"""

import argparse
import json
import time
import uuid

import googleapiclient.discovery
from pandas.io import gbq
import datetime
from datetime import timedelta

# [START async_query]
def async_query(
        bigquery, project_id, query,
        batch=False, num_retries=5, use_legacy_sql=True):
    # Generate a unique job ID so retries
    # don't accidentally duplicate query
    job_data = {
        'jobReference': {
            'projectId': project_id,
            'jobId': str(uuid.uuid4())
        },
        'configuration': {
            'query': {
                'query': query,
                'priority': 'BATCH' if batch else 'INTERACTIVE',
                # Set to False to use standard SQL syntax. See:
                # https://cloud.google.com/bigquery/sql-reference/enabling-standard-sql
                'useLegacySql': use_legacy_sql
            }
        }
    }
    return bigquery.jobs().insert(
        projectId=project_id,
        body=job_data).execute(num_retries=num_retries)
# [END async_query]


# [START poll_job]
def poll_job(bigquery, job):
    """Waits for a job to complete."""

    print('Waiting for job to finish...')

    request = bigquery.jobs().get(
        projectId=job['jobReference']['projectId'],
        jobId=job['jobReference']['jobId'])

    while True:
        result = request.execute(num_retries=2)

        if result['status']['state'] == 'DONE':
            if 'errorResult' in result['status']:
                raise RuntimeError(result['status']['errorResult'])
            print('Job complete.')
            return

        time.sleep(1)
# [END poll_job]


# [START run]
def main(
        project_id, batch, num_retries, interval,
        use_legacy_sql):
    # [START build_service]
    # Construct the service object for interacting with the BigQuery API.
    bigquery = googleapiclient.discovery.build('bigquery', 'v2')
    # [END build_service]
    inputDate = input("Please enter the date in YYYY-MM-DD format ")
    year, month, day = map(int, inputDate.split('-'))
    date1 = datetime.date(year, month, day)
    week1=date1.isocalendar()[1]
    d=date1-timedelta(days=8)
    date2=d.strftime('%Y-%m-%d')

    query_string = "SELECT domain,bundle, sum(demand_side_revenue) as demand_side_revenue, sum(auctionss) as auctions, Min(week) as week, count(week) as weekCount, FROM(SELECT domain, bundle, sum(available_inventory) as auctionss , week(date) as week, sum(demand_side_revenue) as demand_side_revenue FROM TABLE_DATE_RANGE(reporting_hudson_views.stats_daily_, TIMESTAMP(\'"+ str(date2)+"\'), TIMESTAMP(\'"+str(inputDate)+ "\')) group by week, domain,bundle ) group by domain,bundle having bundle is null and weekCount==1 and week== "+ str(week1) + "order by demand_side_revenue desc"
    print(query_string)

    # Submit the job and wait for it to complete.
    use_legacy_sql
    query_job = async_query(
        bigquery,
        project_id,
        query_string,
        batch,
        num_retries,
        use_legacy_sql)

    df = gbq.read_gbq(query_string, project_id=project_id)
    print(df)
    df.to_csv("bq.csv")



    poll_job(bigquery, query_job)

    # Page through the result set and print all results.
    page_token = None
    while True:
        page = bigquery.jobs().getQueryResults(
            pageToken=page_token,
            **query_job['jobReference']).execute(num_retries=2)

        page_token = page.get('pageToken')
        if not page_token:
            break
# [END run]


# [START main]
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ##parser.add_argument('project_id', help='Your Google Cloud project ID.', default = 'divine-builder-586')
    parser.add_argument(
        '-b', '--batch', help='Run query in batch mode.', action='store_true')
    parser.add_argument(
        '-r', '--num_retries',
        help='Number of times to retry in case of 500 error.',
        type=int,
        default=5)
    parser.add_argument(
        '-p', '--poll_interval',
        help='How often to poll the query for completion (seconds).',
        type=int,
        default=1)
    parser.add_argument(
        '-l', '--use_legacy_sql',
        help='Use legacy BigQuery SQL syntax instead of standard SQL syntax.',
        type=bool,
        default=True)

    args = parser.parse_args()
    pro = "divine-builder-586"

    main(
        ##args.project_id,
        pro,
        args.batch,
        args.num_retries,
        args.poll_interval,
        args.use_legacy_sql)
# [END main]
