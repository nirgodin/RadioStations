from consts.path_consts import MERGED_DATA_PATH
from data_processing.data_pre_processor import DataPreProcessor


if __name__ == '__main__':
    pre_processor = DataPreProcessor(
        max_year=2023
    )
    pre_processor.pre_process(output_path=MERGED_DATA_PATH)
