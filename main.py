import apache_beam as beam
from apache_beam.io.textio import ReadFromText, ReadAllFromText, ReadAllFiles, Read
from parse_json import ParseJSON
from week_processing import CalculateWeek, CollectBuysPrice, CollectBuysQuantity, CollectSellsPrice, CollectSellsQuantity
from write import FormatToCSV


if __name__ == '__main__':
    input_ = 'data/input/*'
    output_filename = 'data/output/output_procesed'
    pipeline_options = beam.options.pipeline_options.PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as p:
        rows = (
            p |
            ReadFromText(input_) |
            # beam.Flatten() |
            beam.ParDo(ParseJSON()) |
            beam.ParDo(CalculateWeek())
        )

        weekly_buys_price = (
            rows |
            beam.ParDo(CollectBuysPrice()) |
            "Grouping buys price by week" >> beam.GroupByKey() |
            "Calculating buys price mean" >> beam.CombineValues(
                beam.combiners.MeanCombineFn()
            )
        )

        weekly_buys_quantity = (
            rows |
            beam.ParDo(CollectBuysQuantity()) |
            "Grouping buys quantity by week" >> beam.GroupByKey() |
            "Calculating buys quantity mean" >> beam.CombineValues(
                beam.combiners.MeanCombineFn()
            )
        )

        weekly_sells_price = (
            rows |
            beam.ParDo(CollectSellsPrice()) |
            "Grouping sells price by week" >> beam.GroupByKey() |
            "Calculating sells mean" >> beam.CombineValues(
                beam.combiners.MeanCombineFn()
            )
        )

        weekly_sells_quantity = (
            rows |
            beam.ParDo(CollectSellsQuantity()) |
            "Grouping sells quantity by week" >> beam.GroupByKey() |
            "Calculating sells quantity mean" >> beam.CombineValues(
                beam.combiners.MeanCombineFn()
            )
        )

        result = (
            (
                weekly_buys_price, 
                weekly_buys_quantity, 
                weekly_sells_price, 
                weekly_sells_quantity,
            ) |
            beam.CoGroupByKey() |
            "Format Output" >> beam.ParDo(FormatToCSV())
        )
        header = 'week,id,buys_price,buys_quantity,sells_price,sells_quantity'
        result | beam.io.textio.WriteToText(output_filename, file_name_suffix='.csv', header=header)
