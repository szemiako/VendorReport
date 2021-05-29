from df import apply_feed_type
from df import build_export_df
from df import configs_to_dict
from df import DataFrame
from df import df_from_sql
from df import filter_data
from df import get_feed_type
from df import group_and_consolidate
from df import to_datetime
from setup import CONN
from setup import CONFIGS
from setup import QUERIES
from setup import TEMPLATES
from setup import Tuple

'''Module to get expected files to be processed, actual files processed,
and all export files sent. Prepare and filter the data accordingly.'''

class Feeds:
    '''Class to hold the expected, all actually processed data, condensed
    actually processed data, exported data, and all data (exported and
    condensed processed data merged together).'''
    def __init__(
        self
    ):
        self.configs = self.Configs().configs
        self.expected = self.Expected(self.configs).expected
        self.actual = self.Actual(self.expected).actual
        self.all_actuals = self.actual[0]
        self.condensed_actuals = self.actual[1]
        self.exported = self.Exported().exported
        self.feeds = self._get_feeds()

    def _cleanup(self, df: DataFrame) -> DataFrame:
        df = df[[
            'Status',
            'ProcessorName',
            'VendorNumber',
            'FeedType',
            'Name_x',
            'Name_y',
            'Download',
            'Start',
            'End',
            'Modified',
            'SentAs'
        ]]
        df.columns = [
            'Status',
            'Vendor Name',
            'Vendor Number',
            'Feed Type',
            'Processor',
            'Jurisdiction',
            'Download',
            'Start',
            'End',
            'Export Created',
            'Sent As'
        ]
        return df

    def _get_feeds(self) -> DataFrame:
        '''Get the latest feed file status by merging the actually
        exported data and the condensed actually processed data.'''     
        feeds = self.condensed_actuals.merge(
            self.exported,
            left_on=['VendorName'],
            right_on=['VendorName'],
            how='outer'
        )
        feeds = apply_feed_type(feeds, self.configs)
        feeds = self._cleanup(feeds)
        return feeds

    class Configs:
        def __init__(
            self
        ):
            self.configs = self._get_configs()

        def _get_configs(self):
            '''Load the configurations data into a DataFrame, for
            reference later.'''
            configs = QUERIES.execute_sql(CONN, CONFIGS, TEMPLATES[0])
            df = df_from_sql('configs', configs)
            return df

    class Expected:
        '''Expected feeds for today. Based on database configurations
        table (namely, masks from feeds) and external configuration
        file.'''
        def __init__(
            self,
            configs
        ):
            self.expected = self._get_expected(configs)

        def _get_expected(self, configs) -> DataFrame:
            '''Get expected Vendor feed filenames and fix the filenames.'''
            expected = QUERIES.execute_sql(CONN, configs_to_dict('configs', configs), TEMPLATES[1])
            df = df_from_sql('expected', expected)
            return df

    class Actual:
        '''Build the "actually processed" DataFrame.'''
        def __init__(
            self,
            expected
        ):
            self._date_cols = {'Download', 'Start', 'End'}     # Date columns that we will aggregate on later.
            self._keys = ['VendorName', 'Name']                # Keys for aggregation.
            self.actual = self._get_actual(expected)

        def _cleanup(self, df: DataFrame)-> Tuple[DataFrame, DataFrame]:
            df = filter_data(df)
            df = to_datetime(df, self._date_cols)
            consolidated = group_and_consolidate(df, self._keys, self._date_cols)
            return df, consolidated

        def _get_actual(self, expected: DataFrame) -> Tuple[DataFrame, DataFrame]:
            '''Get the actually processed Vendor feed data, and split it
            into a "raw" DataFrame of all results and a "consolidated"
            DataFrame of "aggregated" values.'''
            actual = QUERIES.execute_sql(CONN, configs_to_dict('expected', expected), TEMPLATES[2])
            df = df_from_sql('actual', actual)
            dfs = self._cleanup(df)
            return dfs

    class Exported:
        '''Actually sent files. Comes from an external file generated on
        the server.'''
        def __init__(
            self
        ):
            self.exported = self._get_exported()

        def _get_exported(self) -> DataFrame:
            '''Read the file from an external csv.'''
            df = build_export_df()
            df = df[[
                'VendorName',
                'Name',
                'Modified',
                'Status',
                'SentAs'
            ]]
            return df