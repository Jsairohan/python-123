import json
import io as io
import boto3
import pandas as pd
import openpyxl
import simplejson as sj
import urllib.parse
import os as os
from jsonschema.exceptions import ValidationError
import jsonschema
from datetime import date
from jsonschema import Draft201909Validator, validate

print('Loading function')

s3 = boto3.client('s3')
dynamoDb = boto3.resource('dynamodb')
today = date.today()


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    print("BUCKET : " + bucket)
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print("KEY : " + key)
    try:
        # bucket = s3.Bucket(bucket)
        # print("Bucket called", bucket)
        response = s3.get_object(Bucket=bucket, Key=key)
        print(response, "test")
        excel_data_df = pd.read_excel(io.BytesIO(response['Body'].read()), dtype=str)
        excel_data_df = excel_data_df.rename(columns=lambda x: x.strip())
        # print(excel_data_df)
        json_array = convertExcelRowToJson(excel_data_df)
        pushDataToTable(json_array)
        return "Data has been inserted!!"
    except Exception as e:
        print(e)
        print("lamdba dsdsd")
        raise e


def convertExcelRowToJson(excel_data_df):
    # json_list = []
    json_dict = {}
    try:
        for ind in excel_data_df.index:
            if excel_data_df['RoleName'][ind] in json_dict:
                existing_json = json_dict[excel_data_df['RoleName'][ind]]
                auth = excel_data_df['CoarsegrainedGroup'][ind]
                auth = auth.strip()

                if auth == 'bcbsa_bcp_edar_cg':
                    if auth in existing_json['RoleInfo']['authorization']:
                        fg_group = existing_json['RoleInfo']['authorization']['bcbsa_bcp_edar_cg']['fg_groups']
                        fg_group.append((excel_data_df['FineGrainedGroup'][ind]).strip())
                        existing_json['RoleInfo']['authorization']['bcbsa_bcp_edar_cg']['fg_groups'] = fg_group
                        ndw_plan_code = existing_json['RoleInfo']['authorization']['bcbsa_bcp_edar_cg']['ndw_plan_code']
                        temp_ndw_plan_code = str((excel_data_df['NDWPlanCode'][ind])).split(",")
                        stripped_list = [s.strip() for s in temp_ndw_plan_code]
                        ndw_plan_code.extend(stripped_list)
                        existing_json['RoleInfo']['authorization']['bcbsa_bcp_edar_cg']['ndw_plan_code'] = ndw_plan_code
                    else:
                        temp_auth = existing_json['RoleInfo']['authorization']
                        ndw_plan_code = str((excel_data_df['NDWPlanCode'][ind])).split(",")
                        stripped_list = [s.strip() for s in ndw_plan_code]
                        temp_auth[auth] = {"fg_groups": [excel_data_df['FineGrainedGroup'][ind]],
                                           "ndw_plan_code": stripped_list}
                        existing_json['RoleInfo']['authorization'] = temp_auth
                elif auth == 'bcbsa_bcp_rtm_cg':
                    if auth in existing_json['RoleInfo']['authorization']:
                        fg_group = existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']['fg_groups']
                        fg_group.append((excel_data_df['FineGrainedGroup'][ind]).strip())
                        existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']['fg_groups'] = fg_group
                    else:
                        temp_auth = existing_json['RoleInfo']['authorization']
                        temp_auth[auth] = {"fg_groups": [excel_data_df['FineGrainedGroup'][ind]]}
                        existing_json['RoleInfo']['authorization'] = temp_auth
                    if pd.notnull(excel_data_df['ControlPlanCode'][ind]) | pd.notnull(
                            excel_data_df['ControlStationCode'][ind]):
                        plan_code = {'plan_code': excel_data_df['ControlPlanCode'][ind],
                                     'station_code': excel_data_df['ControlStationCode'][ind]}
                        control = {'codes': {'control': [plan_code]}}
                        if 'claim' in existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']:
                            claim_list = existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']['claim']
                            claim_list.append(control)
                            existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']['claim'] = claim_list
                        else:
                            temp_auth = existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']
                            temp_auth['claim'] = [control]
                            existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg'] = temp_auth
                    if pd.notnull(excel_data_df['ProcessingPlanCode'][ind]) | pd.notnull(
                            excel_data_df['ProcessingStationCode'][ind]):
                        plan_code = {'plan_code': excel_data_df['ProcessingPlanCode'][ind],
                                     'station_code': excel_data_df['ProcessingStationCode'][ind]}
                        processing = {'codes': {'processing': [plan_code]}}
                        if 'claim' in existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']:
                            claim_list = existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']['claim']
                            last_claim = claim_list[len(claim_list) - 1]
                            last_claim['codes']['processing'] = [plan_code]
                            existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']['claim'][
                                len(claim_list) - 1] = last_claim
                            # print(existing_json)
                        else:
                            temp_auth = existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']
                            temp_auth['claim'] = [processing]
                            existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg'] = temp_auth
                    if 'claim' in existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']:
                        claim_list = existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']['claim']
                        last_claim = claim_list[len(claim_list) - 1]
                        last_claim['privacy'] = {}
                        last_claim['boid'] = excel_data_df['BOID'][ind]
                        existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']['claim'][
                            len(claim_list) - 1] = last_claim
                elif auth == 'bcbsa_bcp_role_admin_cg':
                    if auth in existing_json['RoleInfo']['authorization']:
                        fg_group = existing_json['RoleInfo']['authorization']['bcbsa_bcp_role_admin_cg']['fg_groups']
                        fg_group.append((excel_data_df['FineGrainedGroup'][ind]).strip())
                        existing_json['RoleInfo']['authorization']['bcbsa_bcp_role_admin_cg']['fg_groups'] = fg_group
                        ndw_plan_code = existing_json['RoleInfo']['authorization']['bcbsa_bcp_role_admin_cg'][
                            'ndw_plan_code']
                        temp_ndw_plan_code = str((excel_data_df['NDWPlanCode'][ind])).split(",")
                        stripped_list = [s.strip() for s in temp_ndw_plan_code]
                        ndw_plan_code.extend(stripped_list)
                        existing_json['RoleInfo']['authorization']['bcbsa_bcp_role_admin_cg'][
                            'ndw_plan_code'] = ndw_plan_code
                    else:
                        temp_auth = existing_json['RoleInfo']['authorization']
                        ndw_plan_code = str((excel_data_df['NDWPlanCode'][ind])).split(",")
                        stripped_list = [s.strip() for s in ndw_plan_code]
                        temp_auth[auth] = {"fg_groups": [(excel_data_df['FineGrainedGroup'][ind]).strip()],
                                           "ndw_plan_code": stripped_list}
                        existing_json['RoleInfo']['authorization'] = temp_auth
                elif auth == 'bcbsa_bcp_portal_cg':
                    if auth in existing_json['RoleInfo']['authorization']:
                        fg_group = existing_json['RoleInfo']['authorization']['bcbsa_bcp_portal_cg']['fg_groups']
                        fg_group.append((excel_data_df['FineGrainedGroup'][ind]).strip())
                        existing_json['RoleInfo']['authorization']['bcbsa_bcp_portal_cg']['fg_groups'] = fg_group
                        ndw_plan_code = existing_json['RoleInfo']['authorization']['bcbsa_bcp_portal_cg'][
                            'ndw_plan_code']
                        temp_ndw_plan_code = str((excel_data_df['NDWPlanCode'][ind])).split(",")
                        stripped_list = [s.strip() for s in temp_ndw_plan_code]
                        ndw_plan_code.extend(stripped_list)
                        existing_json['RoleInfo']['authorization']['bcbsa_bcp_portal_cg'][
                            'ndw_plan_code'] = ndw_plan_code
                    else:
                        temp_auth = existing_json['RoleInfo']['authorization']
                        ndw_plan_code = str((excel_data_df['NDWPlanCode'][ind])).split(",")
                        stripped_list = [s.strip() for s in ndw_plan_code]
                        temp_auth[auth] = {"fg_groups": [excel_data_df['FineGrainedGroup'][ind]],
                                           "ndw_plan_code": stripped_list}
                        existing_json['RoleInfo']['authorization'] = temp_auth
            else:
                json_struct = {'RoleName': excel_data_df['RoleName'][ind],
                               'Active': '1',
                               'InactiveDate': dateToStr(excel_data_df['InactiveDate'][ind]),
                               'LHCode': excel_data_df['LHEntityCode'][ind].split("-")[0],
                               'AuditRecInsertDate': dateToStr(excel_data_df['RecordInsertDate'][ind]),
                               'Env': excel_data_df['Env'][ind],
                               'EffectiveDate': dateToStr(excel_data_df['RecordInsertDate'][ind]),
                               'Roleid': excel_data_df['RoleID'][ind],
                               'RoleInfo': {'authorization': {}, 'version': '0.0.1',
                                            'lh_code': excel_data_df['LHEntityCode'][ind].split("-")[0]},
                               'AuditRecInsertBy': excel_data_df['RecordInsertedBy'][ind],
                               'AuditRecUpdby': excel_data_df['RecordUpdatedBy'][ind],
                               'LHEntityCode': excel_data_df['LHEntityCode'][ind],
                               'AuditRecUpDate': today.strftime('%m/%d/%Y')}
                auth = excel_data_df['CoarsegrainedGroup'][ind]
                auth = auth.strip()

                if auth == 'bcbsa_bcp_rtm_cg':
                    auth = {auth: {"fg_groups": [(excel_data_df['FineGrainedGroup'][ind]).strip()]}}

                    if pd.notnull(excel_data_df['ControlPlanCode'][ind]) | pd.notnull(
                            excel_data_df['ControlStationCode'][ind]):
                        plan_code = {'plan_code': excel_data_df['ControlPlanCode'][ind],
                                     'station_code': excel_data_df['ControlStationCode'][ind]}
                        codes = {'codes': {'control': [plan_code]}}
                        temp_auth = auth['bcbsa_bcp_rtm_cg']
                        temp_auth['claim'] = [codes]
                        auth['bcbsa_bcp_rtm_cg'] = temp_auth
                    if pd.notnull(excel_data_df['ProcessingPlanCode'][ind]) | pd.notnull(
                            excel_data_df['ProcessingStationCode'][ind]):
                        plan_code = {'plan_code': excel_data_df['ProcessingPlanCode'][ind],
                                     'station_code': excel_data_df['ProcessingStationCode'][ind]}
                        codes = {'codes': {'processing': [plan_code]}}
                        # print(codes)
                        if 'claim' in auth['bcbsa_bcp_rtm_cg']:
                            temp_code = auth['bcbsa_bcp_rtm_cg']['claim'][0]['codes']
                            temp_code['processing'] = [plan_code]
                            auth['bcbsa_bcp_rtm_cg']['claim'][0]['codes'] = temp_code
                        else:
                            temp_auth = auth['bcbsa_bcp_rtm_cg']
                            temp_auth['claim'] = [codes]
                            auth['bcbsa_bcp_rtm_cg'] = temp_auth
                        # print(auth)
                    if 'claim' in auth['bcbsa_bcp_rtm_cg']:
                        temp_auth = auth['bcbsa_bcp_rtm_cg']['claim'][0]
                        temp_auth['privacy'] = {}
                        temp_auth['boid'] = excel_data_df['BOID'][ind]
                        auth['bcbsa_bcp_rtm_cg']['claim'][0] = temp_auth
                    json_struct['RoleInfo']['authorization'] = auth
                elif auth == 'bcbsa_bcp_edar_cg':
                    ndw_plan_code = str((excel_data_df['NDWPlanCode'][ind])).split(",")
                    stripped_list = [s.strip() for s in ndw_plan_code]
                    auth = {auth: {"fg_groups": [(excel_data_df['FineGrainedGroup'][ind]).strip()],
                                   "ndw_plan_code": stripped_list}}
                    json_struct['RoleInfo']['authorization'] = auth
                elif auth == 'bcbsa_bcp_role_admin_cg':
                    ndw_plan_code = str((excel_data_df['NDWPlanCode'][ind])).split(",")
                    stripped_list = [s.strip() for s in ndw_plan_code]
                    auth = {auth: {"fg_groups": [(excel_data_df['FineGrainedGroup'][ind]).strip()],
                                   "ndw_plan_code": stripped_list}}
                    json_struct['RoleInfo']['authorization'] = auth
                elif auth == 'bcbsa_bcp_portal_cg':
                    ndw_plan_code = str((excel_data_df['NDWPlanCode'][ind])).split(",")
                    stripped_list = [s.strip() for s in ndw_plan_code]
                    auth = {auth: {"fg_groups": [(excel_data_df['FineGrainedGroup'][ind]).strip()],
                                   "ndw_plan_code": stripped_list}}
                    json_struct['RoleInfo']['authorization'] = auth
                json_dict[excel_data_df['RoleName'][ind]] = json_struct
        json_list = list(json_dict.values())
        json_array = sj.dumps(json_list, ignore_nan=True, encoding="utf-8")
        # print(json_array)
        return json_array
    except Exception as e:
        raise e


def dateToStr(param):
    if type(param) == str:
        yyyy, mm, dd = param.split(" ")[0].split("-")[0], param.split(" ")[0].split("-")[1], \
                       param.split(" ")[0].split("-")[2]
        return mm + '/' + dd + '/' + yyyy


def pushDataToTable(json_array):
    try:
        role_data = json.loads(json_array)
        bucket = os.environ.get('JSON_SCHEMA_BUCKET')
        key = os.environ.get('JSON_SCHEMA_KEY')
        # print(key)
        response = s3.get_object(Bucket=bucket, Key=key)
        # print(response)
        auth_schema = {}
        auth_schema = json.loads(response['Body'].read().decode())
        json_validator = Draft201909Validator(auth_schema)
        # print(auth_schema)
        table_name = os.environ.get('TABLE_NAME')
        print("Table Name :- " + table_name)
        table = dynamoDb.Table(table_name)
        with table.batch_writer(overwrite_by_pkeys=['RoleName']) as batch:
            for item in role_data:
                try:
                    json_validator.validate(item['RoleInfo'])
                    batch.put_item(Item=item)
                    # print('Data Inserted '+item['RoleName'])
                except ValidationError as e:
                    print("exception validaing error", e)
                    print("ererererere")
                    print(
                        'Authorization Object is invalid for Rolename : ' + item['RoleName'] + ' Skipping from Insert')
                    print(item)
    except Exception as e:
        raise e