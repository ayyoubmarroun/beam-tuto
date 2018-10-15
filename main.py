import apache_beam
from apache_beam.io.textio import ReadFromText
from parse_json import ParseJSON
from week_processing import CalculateWeek, CollectBuysPrice, CollectBuysQuantity, CollectSellsPrice, CollectSellsQuantity
from write import FormatToCSV


if __name__ == '__main__':
    filename = 'data/input/export-bq_export_20181013_000000000007.json'
    output_filenae = 'data/output/output_procesed'
    pipeline_options = apache_beam.options.pipeline_options.PipelineOptions()
    with apache_beam.Pipeline(options=pipeline_options) as p:
        rows = (
            p |
            ReadFromText(filename) |
            apache_beam.ParDo(ParseJSON()) |
            apache_beam.ParDo(CalculateWeek())
        )

        weekly_buys_price = (
            rows |
            apache_beam.ParDo(CollectBuysPrice()) |
            "Grouping buys price by week" >> apache_beam.GroupByKey() |
            "Calculating buys price mean" >> apache_beam.CombineValues(
                apache_beam.combiners.MeanCombineFn()
            )
        )

        weekly_buys_quantity = (
            rows |
            apache_beam.ParDo(CollectBuysQuantity()) |
            "Grouping buys quantity by week" >> apache_beam.GroupByKey() |
            "Calculating buys quantity mean" >> apache_beam.CombineValues(
                apache_beam.combiners.MeanCombineFn()
            )
        )

        weekly_sells_price = (
            rows |
            apache_beam.ParDo(CollectSellsPrice()) |
            "Grouping sells price by week" >> apache_beam.GroupByKey() |
            "Calculating sells mean" >> apache_beam.CombineValues(
                apache_beam.combiners.MeanCombineFn()
            )
        )

        weekly_sells_quantity = (
            rows |
            apache_beam.ParDo(CollectSellsQuantity()) |
            "Grouping sells quantity by week" >> apache_beam.GroupByKey() |
            "Calculating sells quantity mean" >> apache_beam.CombineValues(
                apache_beam.combiners.MeanCombineFn()
            )
        )

        result = (
            (
                weekly_buys_price, 
                weekly_buys_quantity, 
                weekly_sells_price, 
                weekly_sells_quantity,
            ) |
            apache_beam.CoGroupByKey() |
            "Format Output" >> apache_beam.ParDo(FormatToCSV())
        )
        header = 'week,id,buys_price,buys_quantity,sells_price,sells_quantity'
        result | apache_beam.io.textio.WriteToText(output_filenae, file_name_suffix='.csv', header=header)
