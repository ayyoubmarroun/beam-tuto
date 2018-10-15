import apache_beam
from apache_beam.io.textio import ReadFromText
from parse_json import ParseJSON
from week_processing import CalculateWeek, CollectTotals
from write import WriteToCSV, WriteToFile


if __name__ == '__main__':
    filename = 'data/input/export-bq_export_20181013_000000000007.json'
    output_filenae = 'data/output/output_procesed.csv'
    with apache_beam.Pipeline as p:
        rows = (
            p |
            ReadFromText(filename) |
            apache_beam.ParDo(ParseJSON())
        )

        weekly_rows = (
            rows |
            apache_beam.ParDo(CalculateWeek()) |
            apache_beam.ParDo(CollectTotals()) |
            "Grouping by week" >> apache_beam.GroupByKey()
        )

        weekly_means = (
            weekly_rows |
            "Calculating means" >> apache_beam.CombineValues(
                apache_beam.combiners.MeanCombineFn()
            )
        )

        weekly_totals = (
            weekly_rows |
            "Calculating totals" >> apache_beam.CombineValues(
                apache_beam.reduce(lambda x,y: x+y)
            )
        )

        result = (
            {
                'totals': weekly_totals,
                'means': weekly_means
            } |
            apache_beam.CoGroupByKey() |
            apache_beam.ParDo(WriteToCSV())
        )
