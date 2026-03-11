from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from networksecurity.constants.training_pipeline import SCHEMA_FILE_PATH
from networksecurity.entity.config_entity import DataValidationConfig
from scipy.stats import ks_2samp
import os, sys
import pandas as pd
from networksecurity.utils.main_utils.utils import read_yaml_file, write_yaml_file


class DataValidation:

    def __init__(self, data_ingestion_artifact: DataIngestionArtifact,
                 data_validation_config: DataValidationConfig):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def read_data(self, file_path) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def validate_no_of_columns(self, dataframe: pd.DataFrame) -> bool:
        try:
            required_columns = len(self._schema_config["columns"])
            logging.info(f"Required no of columns: {required_columns}")
            logging.info(f"DataFrame columns: {len(dataframe.columns)}")

            return len(dataframe.columns) == required_columns

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def detect_dataset_drift(self, base_df, current_df, threshold=0.05):
        try:
            status = True
            report = {}

            for column in base_df.columns:
                d1 = base_df[column]
                d2 = current_df[column]

                ks_result = ks_2samp(d1, d2)

                drift_found = ks_result.pvalue < threshold
                if drift_found:
                    status = False

                report[column] = {
                    "p_value": float(ks_result.pvalue),
                    "drift_status": drift_found
                }

            drift_report_file_path = self.data_validation_config.drift_report_file_path
            os.makedirs(os.path.dirname(drift_report_file_path), exist_ok=True)

            write_yaml_file(file_path=drift_report_file_path, content=report)

            return status

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            # Read data
            train_dataframe = self.read_data(train_file_path)
            test_dataframe = self.read_data(test_file_path)

            # Validate train
            if not self.validate_no_of_columns(train_dataframe):
                raise Exception("Train dataframe does not contain required columns.")

            # Validate test
            if not self.validate_no_of_columns(test_dataframe):
                raise Exception("Test dataframe does not contain required columns.")

            # Detect data drift
            validation_status = self.detect_dataset_drift(
                base_df=train_dataframe,
                current_df=test_dataframe
            )

            # Save validated files
            os.makedirs(os.path.dirname(self.data_validation_config.valid_train_file_path), exist_ok=True)

            train_dataframe.to_csv(self.data_validation_config.valid_train_file_path, index=False)
            test_dataframe.to_csv(self.data_validation_config.valid_test_file_path, index=False)

            # Build artifact
            data_validation_artifact = DataValidationArtifact(
                validation_status=validation_status,
                valid_train_file_path=self.data_validation_config.valid_train_file_path,
                valid_test_file_path=self.data_validation_config.valid_test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )

            return data_validation_artifact

        except Exception as e:
            raise NetworkSecurityException(e, sys)