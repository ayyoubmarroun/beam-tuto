import apache_beam
import json
import datetime


class ParseJSON(apache_beam.DoFn):

    def process(self, element, *args, **kwargs):
        element = json.loads(element)
        # date format example: 2018-10-14T11:08:17.40939
        element['timestamp'] = datetime.datetime.strptime(
            element.get('timestamp'), '%Y-%m-%dT%H:%M:%S.00000')
        return [element]
