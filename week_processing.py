import apache_beam


class CalculateWeek(apache_beam.DoFn):

    def process(self, element, *args, **kwargs):
        result = element.copy()
        result['week'] = result.get('timestamp').strftime('%W')
        return [result]


class CollectBuysPrice(apache_beam.DoFn):

    def process(self, element, *args, **kwargs):
        week = element.get('week')
        id_ = element.get('id')

        result = [
            (
                (week, id_),
                element.get('totals_buys_price'),
            )
        ]

        return result

class CollectBuysQuantity(apache_beam.DoFn):

    def process(self, element, *args, **kwargs):
        week = element.get('week')
        id_ = element.get('id')

        result = [
            (
                (week, id_),
                element.get('totals_buys_quantity'),
            )
        ]

        return result

class CollectSellsPrice(apache_beam.DoFn):

    def process(self, element, *args, **kwargs):
        week = element.get('week')
        id_ = element.get('id')

        result = [
            (
                (week, id_),
                element.get('totals_sells_price'),
            )
        ]

        return result

class CollectSellsQuantity(apache_beam.DoFn):

    def process(self, element, *args, **kwargs):
        week = element.get('week')
        id_ = element.get('id')

        result = [
            (
                (week, id_),
                element.get('totals_sells_quantity'),
            )
        ]

        return result
