import os
import sys
from src.components.data_preprocessing import Datapreprocess
# from src.components.model_trainer import ModelTrainer
from src.exception import CustomException
from src.logger import logging
import pandas as pd
from sklearn.model_selection import train_test_split
from dataclasses import dataclass
import pyodbc
conn = pyodbc.connect(
    'DRIVER={SQL Server};'
    'SERVER=10.0.1.40;'
    'DATABASE=Ntier_EPIC;'
    'UID=Sisenseuser;'
    'PWD=!w@ntd@t@#123'
)

# from src.components.data_transformation import DataTransformation
# from src.components.data_transformation import DataTransformationConfig
# from src.components.model_trainer import ModelTrainerConfig
# from src.components.model_trainer import ModelTrainer


@dataclass
class DataIngestionConfig:
    train_data_path: str = os.path.join('artifacts', "train.csv")
    test_data_path: str = os.path.join('artifacts', "test.csv")
    raw_data_path: str = os.path.join('notebook/data', "data.csv")


class DataIngestion:
    def __init__(self):
        self.ingestion_config = DataIngestionConfig()

    def initiate_data_ingestion(self):
        logging.info("Entered the data ingestion method or component")
        try:
            cursor = conn.cursor()
            cursor.execute("""
                        Select distinct t1.Service_ID,t1.Patient_Number,t5.patient_age,t1.Actual_Dr_Name,
                        t1.Place_of_Service_Abbr,t1.Proc_Category_Abbr,t1.Type_of_Service_Abbr,t5.patient_zip_code,t5.patient_sex,
                        t1.Original_Carrier_Name, t5.Patient_City, t5.Patient_State,t2.CoPayment,t2.CoInsurance,t1.Primary_Diagnosis_Code,t1.Procedure_Code,
                        t1.Service_Units,convert(Date, t1.Service_Date_From) as Service_Date_From, t1.Claim_Number,
                        convert(Date, t1.Original_Billing_Date) as Original_Billing_Date,Convert(Date, t2.Date_Paid) as Date_Paid,
                        t1.Service_Fee,t2.Amount, t2.Allowed, t2.Deductible, t2.Transaction_Type, t4.Abbreviation,t4.Description, 
                        t4.Self_Pay_TranCode
                        from PM.vwGenSvcInfo as T1
                        left join PM.[vwGenSvcPmtInfo] T2 ON T1.Service_Id=T2.Service_Id
                        left join PM.Reimbursement_Detail T3 on T1.Service_Id=T3.Service_Id
                        left join [dbo].[vUAI_Transaction_Codes] T4 ON T2.Transaction_Code_Abbr=T4.Abbreviation
                        left join PM.vwGenPatInfo as T5 ON T1.Patient_Number=T5.Patient_Number
                        where (T4.Self_Pay_TranCode=0)
                        and (T4.Description not like '%Self%' And T4.Description not like '%Adj%') And (T2.Transaction_Type !='A') and 
                        (T2.Transaction_Type !='T') and (T2.Transaction_Type !='B') and (T1.Service_Fee >0)  and (t2.Amount >= 0) and
                        ((t1.Primary_Diagnosis_Code between 'E08' and 'E13') OR (t1.Primary_Diagnosis_Code='R73.03'))
                        and t2.Date_Paid >= DATEADD(day, -455, GETDATE())
                        AND t1.Service_Date_From between DATEADD(month,-15,GETDATE()) and DATEADD(month,-3,GETDATE())""")

            logging.info('Read the dataset as dataframe')
            df = pd.DataFrame([list(elem) for elem in cursor.fetchall()])
            cursor.close()
            conn.close()
            if not df.empty:
                df.columns = ["Service_ID", "Patient_Number", 'Actual_Dr_Name','Place_of_Service_Abbr',"patient_age",
                              "Proc_Category_Abbr","Type_of_Service_Abbr", "patient_zip_code",
                              "patient_sex", "Original_Carrier_Name","Patient_City", "Patient_State",
                              "CoPayment", "CoInsurance", "Primary_Diagnosis_Code",
                              "Procedure_Code", "Service_Units", "Service_Date_From", "Claim_Number",
                              "Original_Billing_Date", "Date_Paid", "Service_Fee", "Amount", "Allowed", "Deductible",
                              "Transaction_Type", "Abbreviation", "Description", "Self_Pay_TranCode"]

                os.makedirs(os.path.dirname(self.ingestion_config.train_data_path), exist_ok=True)
                df.to_csv(self.ingestion_config.raw_data_path, index=False, header=True)
                logging.info('Train test split')
                train_set, test_set = train_test_split(df, test_size=0.30, random_state=42)
                train_set.to_csv(self.ingestion_config.train_data_path, index=False, header=True)
                test_set.to_csv(self.ingestion_config.test_data_path, index=False, header=True)
                logging.info('Ingestion of Data is completed')
                return (
                    self.ingestion_config.train_data_path,
                    self.ingestion_config.test_data_path,
                    self.ingestion_config.raw_data_path

                )
        except Exception as e:
            raise CustomException(e, sys)


if __name__ == "__main__":
    obj = DataIngestion()
    train_data, test_data, data_set = obj.initiate_data_ingestion()
    data_preprocess= Datapreprocess()
    X_new=data_preprocess.initial_data_processing(data_set)
    print(X_new.head(), X_new.columns, X_new.shape)
#     data_transformation = DataTransformation()
#     train_arr, test_arr = data_transformation.initate_data_transformation(data_set)
#
#     modeltrainer = ModelTrainer()
#     score = modeltrainer.initate_model_training(train_arr, test_arr)
#     print(score)
