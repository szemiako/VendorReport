from data import Feeds
from setup import datetime
from setup import THIS_DIR

'''Async exporting of data so that the dash app doesn't create a new
file upon each refresh.'''

def export_data(feeds=Feeds(), ts=datetime.now().strftime('%Y-%m-%d %H.%M.%S')):
    '''Export the data to the /data directory, for use in a third
    party BI tool or archiving.'''
    feeds.expected.to_csv(f'{THIS_DIR}/data/expected_feeds_{ts}.csv', index=False)
    feeds.all_actuals.to_csv(f'{THIS_DIR}/data/processed_feeds_{ts}.csv', index=False)
    feeds.condensed_actuals.to_csv(f'{THIS_DIR}/data/condensed_processed_feeds_{ts}.csv', index=False)
    feeds.exported.to_csv(f'{THIS_DIR}/data/export_files_data_{ts}.csv', index=False)
    feeds.feeds.to_csv(f'{THIS_DIR}/data/feeds_data_{ts}.csv', index=False)

export_data()