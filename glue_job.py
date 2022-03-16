import time
import json
import boto3
from etl_framework import ETLFramework, logging_method


class GlueJob(ETLFramework):

    def __init__(self):
        super(GlueJob, self).__init__()
        self.var = None

    @logging_method('Initialize step')
    def initialize(self):
        super().initialize()
        print(self.var)
        self.var = 3

    @logging_method('Extract step')
    def extract(self):
        print(self.var)
        print('Extract Succeeded')

    @logging_method('Load step')
    def load(self):
        print('Load Succeeded')


def main():
    glue_job = GlueJob()
    glue_job.run()


if __name__ == '__main__':
    main()