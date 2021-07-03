import os
from time import sleep

import luigi

OUTPUT_FOLDER = 'output'


class DownloadFile(luigi.Task):
    input_folder = luigi.Parameter()
    file_name = luigi.Parameter()
    counter = luigi.IntParameter()

    def output(self):
        path = os.path.join(OUTPUT_FOLDER, str(self.counter), self.file_name)
        return luigi.LocalTarget(path)

    def run(self):
        sleep(5)
        input_path = os.path.join(self.input_folder, self.file_name)
        with open(input_path) as f:
            with self.output().open('w') as out:
                for line in f:
                    if ',' in line:
                        out.write(line)


class DownloadSalesData(luigi.Task):
    params = luigi.DictParameter()

    def output(self):
        return luigi.LocalTarget(self.params['output'])

    def run(self):
        processed_files = []
        self.set_progress_percentage(0)
        counter = 1
        files = sorted(os.listdir(self.params['input']))
        tasks = []
        for file in files:
            self.set_progress_percentage(round(100 * counter / len(files), 2))
            tasks.append(DownloadFile(self.params['input'], file, counter))
            counter += 1
        processed_files = yield tasks
        with self.output().open('w') as out:
            for file in processed_files:
                with file.open() as f:
                    for line in f:
                        out.write(line)


if __name__ == '__main__':
    luigi.run(['DownloadSalesData', '--workers', '3'])
