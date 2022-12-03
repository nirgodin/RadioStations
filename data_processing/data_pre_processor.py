from data_processing.data_merger import DataMerger

DATA_OUTPUT_PATH = r'data/merged_data.csv'


class DataPreProcessor:
    def pre_process(self):
        DataMerger.merge(output_path=DATA_OUTPUT_PATH)


if __name__ == '__main__':
    DataPreProcessor().pre_process()