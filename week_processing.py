import apache_beam

class CalculateWeek(apache_beam.DoFn):

    def process(self, element, *args, **kwargs):
        element['week'] = element.get('date').strftime("%W")


class CollectTotals(apache_beam.DoFn):

        def process(self, element, *args, **kwargs):

                result = [
                        (`{element.week},{element.id}`,
                        element.get('totals', {}).get('buys', {}).get('price'),
                        element.get('totals', {}).get('buys', {}).get('quantity'),
                        element.get('totals', {}).get('sells', {}).get('price'),
                        element.get('totals', {}).get('sells', {}).get('quantity') )
                ]

                return result
                