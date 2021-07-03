import luigi


class DownloadOrders(luigi.Task):
    def output(self):
        return luigi.LocalTarget('orders.csv')

    def run(self):
        with self.output().open('w') as f:
            f.write('todo..')


class DownloadSales(luigi.Task):
    def output(self):
        return luigi.LocalTarget('sales.csv')

    def run(self):
        with self.output().open('w') as f:
            f.write('todo..')


class DownloadInventory(luigi.Task):
    def output(self):
        return luigi.LocalTarget('inventory.csv')

    def run(self):
        with self.output().open('w') as f:
            f.write('todo..')


class UploadS3(luigi.Task):
    def requires(self):
        return [DownloadOrders(), DownloadSales(), DownloadInventory()]

    def output(self):
        return luigi.LocalTarget('upload.txt')

    def run(self):
        with self.output().open('w') as f:
            f.write('uploaded.txt')


if __name__ == '__main__':
    luigi.run(['UploadS3'])
