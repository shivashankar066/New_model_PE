import pandas as pd
import sys
from src.exception import CustomException
from src.logger import logging
import numpy as np
# from dataclasses import dataclass


class Datapreprocess:
    def q3(self, x):
        """function to return 3rd quartile"""
        return x.quantile(0.75)

    def get_payout_ratio(self, row, cpt_payment_dict):
        cpt = row['Procedure_Code']
        payor = row['Original_Carrier_Name']
        amt = row['Amount_per_serv_unit']
        med_payout = cpt_payment_dict[payor, str(cpt)]

        if med_payout == 0:
            if amt > med_payout:
                payout_ratio = 1
            else:
                payout_ratio = 0
        else:
            if amt > med_payout:
                amt = med_payout

            payout_ratio = amt / med_payout

        return payout_ratio

    def get_delay(self, row):

        date_paid = row['Date_Paid']
        billing_date = row['Original_Billing_Date']

        diff = (date_paid - billing_date).days

        amt = row["Amount"]

        if amt == 0:
            return 0
        else:
            return diff

    def get_normed_delay(self, row, cpt_delay_dict):
        cpt = row['Procedure_Code']
        payor = row['Original_Carrier_Name']
        amt = row['Amount']
        delay = row['delay_in_days']
        max_delay = cpt_delay_dict[payor, str(cpt)]

        if amt == 0:
            delay_normed = 0
        else:
            try:
                delay_normed = 1 - (delay / max_delay)
            except ZeroDivisionError:
                delay_normed = 0
        return delay_normed

    def groupbyservice_id(self, data):
        df_by_service_id = data.groupby(["Service_ID"], as_index=False).agg({
            "Patient_Number": "first",
            'patient_age': "max",
            'Actual_Dr_Name': 'first',
            'Place_of_Service_Abbr': 'first',
            'Proc_Category_Abbr': 'first',
            'Type_of_Service_Abbr': 'first',
            'patient_zip_code': "first",
            'patient_sex': "first",
            'Original_Carrier_Name': "first",
            'Patient_City': "first",
            'Patient_State': "first",
            'CoInsurance': "first",
            'CoPayment': "first",
            "Primary_Diagnosis_Code": "first",
            "Procedure_Code": "first",
            'Service_Units': "max",  # sum
            "Service_Fee": "max",
            'Allowed': 'max',
            'Deductible': 'max',
            "Amount": 'sum',
            "Score": 'mean'  # sum
        }
        )
        df_by_service_id = df_by_service_id[df_by_service_id.Amount <= df_by_service_id.Service_Fee]
        return df_by_service_id

    def get_payer_allowed_value(self,row,cpt_allowed_dict, cpt_avg_allowed):

        allowed = row['Allowed']

        if not np.isnan(allowed):

            payor_allowed = allowed

        else:

            cpt = row['Procedure_Code']
            payor = row['Original_Carrier_Name']

            try:
                payor_allowed = cpt_allowed_dict[payor, str(cpt)]
            except KeyError:
                try:
                    payor_allowed = cpt_avg_allowed[cpt]
                except KeyError:
                    payor_allowed = None
        return payor_allowed

    def initial_data_processing(self, df):
        try:
            data = pd.read_csv(df)
            data = data.drop_duplicates()
            data = data.dropna(subset=['Original_Billing_Date', 'Date_Paid', 'Original_Carrier_Name'])
            data["Original_Billing_Date"] = pd.to_datetime(data["Original_Billing_Date"])
            data["Date_Paid"] = pd.to_datetime(data["Date_Paid"])
            data["Delay_in_days"] = (
                        pd.to_datetime(data["Date_Paid"]) - pd.to_datetime(data["Original_Billing_Date"])).dt.days
            data = data[(data['Delay_in_days'] >= 0) & (data['Delay_in_days'] <= 365)]
            data = data.drop(['Delay_in_days'], axis='columns')
            data['Amount_per_serv_unit'] = data['Amount'] / data['Service_Units']
            # Payout calculation
            cpt_payment_q3 = data.groupby(['Original_Carrier_Name', 'Procedure_Code']).agg(
                {'Amount_per_serv_unit': self.q3})
            cpt_payment_dict = cpt_payment_q3.to_dict('dict')['Amount_per_serv_unit']
            data['payout_ratio'] = data[['Procedure_Code', 'Original_Carrier_Name', 'Amount_per_serv_unit']].apply(
                self.get_payout_ratio, axis=1)
            data['payout_ratio'] = data['payout_ratio'].round(2)
            # Delay Calculation
            data['delay_in_days'] = data[['Original_Billing_Date', 'Date_Paid', 'Amount']].apply(self.get_delay,
                                                                                                 axis=1)
            cpt_delay_max = data.groupby(['Original_Carrier_Name', 'Procedure_Code'])[['delay_in_days']].max()
            cpt_delay_dict = cpt_delay_max.to_dict('dict')['delay_in_days']
            data['normalized_delay'] = data[['Procedure_Code', 'Original_Carrier_Name', 'Amount',
                                             'delay_in_days']].apply(self.get_normed_delay,
                                                                     axis=1)
            # Final score
            payment_wt = 0.75
            delay_wt = 0.25

            data["Score"] = (payment_wt * data["payout_ratio"]) + (delay_wt * data["normalized_delay"])
            print(data.Score)

            final_data = self.groupbyservice_id(data)
            final_data['CoInsurance'] = final_data.CoInsurance.fillna(0)
            final_data['CoPayment'] = final_data.CoPayment.fillna(0)
            final_data['Deductible'] = final_data.Deductible.fillna(0)
            final_data['Allowed_per_serv_unit'] = final_data['Allowed'] / final_data['Service_Units']
            # finding the mean 'Allowed' for a given cpt by a given payer
            cpt_allowed_mean = final_data[~final_data['Allowed_per_serv_unit'].isna()].groupby(
                ['Original_Carrier_Name', 'Procedure_Code'])[['Allowed_per_serv_unit']].mean()
            cpt_allowed_dict = cpt_allowed_mean.to_dict('dict')['Allowed_per_serv_unit']
            cpt_payer_allowed_dict = {}
            for payer_cpt, alwd_value in cpt_allowed_dict.items():

                payer_name, cpt_name = payer_cpt

                try:
                    cpt_payer_allowed_dict[cpt_name][payer_name] = alwd_value
                except KeyError:
                    cpt_payer_allowed_dict[cpt_name] = {payer_name: alwd_value}

            cpt_avg_allowed = {}
            for cpt, payer_allwd in cpt_payer_allowed_dict.items():
                avg_allowed = sum(payer_allwd.values()) / len(payer_allwd.values())
                cpt_avg_allowed[cpt] = avg_allowed

            final_data['Allowed'] = final_data.apply(self.get_payer_allowed_value(cpt_allowed_dict, cpt_avg_allowed), axis=1)
            final_data['Service_fee_per_serv_unit'] = final_data['Service_Fee'] / final_data[
                'Service_Units']
            Qut = self.q3(final_data['Allowed_per_serv_unit'] / final_data['Service_fee_per_serv_unit'])
            cpt_avg_allowed['allowed_ratio_q3'] = Qut
            final_data.loc[final_data['Allowed'].isna(), 'Allowed'] = final_data['Service_Fee'] * Qut
            cat_cols = ['Patient_Number', 'Actual_Dr_Name', 'Place_of_Service_Abbr', 'Proc_Category_Abbr',
                        'Type_of_Service_Abbr', 'patient_zip_code', 'patient_sex', 'Original_Carrier_Name',
                        'Patient_City', 'Patient_State', 'Primary_Diagnosis_Code', 'Procedure_Code']
            dtype_dict = dict(zip(cat_cols, ['category'] * len(cat_cols)))
            X_new = final_data.copy()
            X_new = X_new.astype(dtype_dict)
            # Drop unneeded columns
            cols_to_drop = ['Patient_Number', 'Service_ID', 'Actual_Dr_Name', 'Place_of_Service_Abbr', 'Service_Units',
                            'Type_of_Service_Abbr', 'Amount', 'Primary_Diagnosis_Code', 'Service_Fee',
                            'Allowed_per_serv_unit', 'Service_fee_per_serv_unit']
            X_new = X_new.drop(cols_to_drop, axis='columns')
            return X_new

        except Exception as e:
            logging.info('Exception occured in initiate_data_transformation function')
            raise CustomException(e, sys)
