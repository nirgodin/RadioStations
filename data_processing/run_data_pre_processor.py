from data_processing.data_pre_processor import DataPreProcessor

OUTPUT_PATH = r'data/merged_data.csv'


if __name__ == '__main__':
    pre_processor = DataPreProcessor()
    pre_processor.pre_process(output_path=OUTPUT_PATH)
