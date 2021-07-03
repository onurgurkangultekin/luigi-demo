import luigi


class DownloadSalesData(luigi.Task):
    def output(self):
        return luigi.LocalTarget('all_sales.csv')

    def run(self):
        with self.output().open('w') as f:
            print('France,May,100', file=f)
            print('France,June,120', file=f)
            print('Germany,May,180', file=f)
            print('Germany,June,150', file=f)


class GetFranceSales(luigi.Task):
    def requires(self):
        return DownloadSalesData()

    def output(self):
        return luigi.LocalTarget('france_sales.csv')

    def run(self):
        with self.output().open('w') as out:
            with self.input().open() as f:
                for line in f:
                    if line.startswith('France'):
                        out.write(line)


class GetGermanySales(luigi.Task):
    def requires(self):
        return DownloadSalesData()

    def output(self):
        return luigi.LocalTarget('germany_sales.csv')

    def run(self):
        with self.output().open('w') as out:
            with self.input().open() as f:
                for line in f:
                    if line.startswith('Germany'):
                        out.write(line)


class SummarizeFranceSales(luigi.Task):
    def requires(self):
        return GetFranceSales()

    def output(self):
        return luigi.LocalTarget('summary_france_sales.csv')

    def run(self):
        with self.output().open('w') as out:
            with self.input().open() as f:
                total = 0.0
                for line in f:
                    _, _, amount = line.split(',')
                    total += float(amount)
                out.write(str(total))


class SummarizeGermanySales(luigi.Task):
    def requires(self):
        return GetGermanySales()

    def output(self):
        return luigi.LocalTarget('germany_france_sales.csv')

    def run(self):
        with self.output().open('w') as out:
            with self.input().open() as f:
                total = 0.0
                for line in f:
                    _, _, amount = line.split(',')
                    total += float(amount)
                out.write(str(total))


class Final(luigi.WrapperTask):
    def requires(self):
        return [SummarizeFranceSales(), SummarizeGermanySales()]


if __name__ == '__main__':
    luigi.run(['Final'])
