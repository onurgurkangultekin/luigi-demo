import luigi
from luigi.contrib import sqla
from sqlalchemy import String, Float


class DownloadFranceSales(luigi.Task):
    def output(self):
        return luigi.LocalTarget('france.csv')

    def run(self):
        with self.output().open('w') as f:
            print('France,May,100', file=f)
            print('France,June,200', file=f)


class DownloadGermanySales(luigi.Task):
    def output(self):
        return luigi.LocalTarget('germany.csv')

    def run(self):
        with self.output().open('w') as f:
            print('Germany,May,180', file=f)
            print('Germany,June,150', file=f)

    class CreateDatabase(sqla.CopyToTable):
        # columns defines the table schema, with each element corresponding
        # to a column in the format (args, kwargs) which will be sent to
        # the sqlalchemy.Column(*args, **kwargs)
        columns = [
            (["country", String(64)], {}),
            (["month", String(64)], {}),
            (["amount", Float], {})
        ]
        connection_string = "sqlite:///test.db"  # in memory SQLite database
        table = "sales"  # name of the table to store data
        column_separator = ','

        def rows(self):
            with self.input()[0].open() as f:
                for line in f:
                    yield line.split(self.column_separator)

            with self.input()[1].open() as f:
                for line in f:
                    yield line.split(self.column_separator)

        def requires(self):
            return [DownloadFranceSales(), DownloadGermanySales()]


if __name__ == '__main__':
    luigi.run(['CreateDatabase'])
