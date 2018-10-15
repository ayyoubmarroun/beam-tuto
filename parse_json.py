import apache_beam
import json
import datetime


class ParseJSON(apache_beam.DoFn):

    def process(self, element, *args, **kwargs):
        row = json.loads(element)
        # date format example: 2018-10-14T11:08:17.40939@
        timestamp_format = '%Y-%m-%dT%H:%M:%S.%f'
        default_date = '1900-01-01T00:00:00.00000'
        result = [{
            'timestamp': datetime.datetime.strptime(row.get('timestamp', default_date), timestamp_format),
            'id': row.get('id'),
            'totals_buys_price': float(row.get('totals', {}).get('buys', {}).get('unit_price', .0)),
            'totals_buys_quantity': int(row.get('totals', {}).get('buys', {}).get('quantity', 0)),
            'totals_sells_price': float(row.get('totals', {}).get('sells', {}).get('unit_price', .0)),
            'totals_sells_quantity': int(row.get('totals', {}).get('sells', {}).get('quantity', 0))
        }]
        return result