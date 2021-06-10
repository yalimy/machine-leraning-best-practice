#!/usr/bin/python3
import json
import boto3
import argparse
import pprint
import time
import os
import botocore.session
import sys

from time import sleep
from decimal import Decimal

from pathlib import Path


PP = pprint.PrettyPrinter(indent=4)

DynamoDB = None
Table    = None
Output   = None

cargo_start = time.time()


def table_put_item(item):
    global Table
    condition_expression=''

    for key_schema in Table.key_schema:
        if condition_expression == '':
            condition_expression = "attribute_not_exists({})".format(key_schema['AttributeName'])
        else:
            condition_expression = condition_expression + " AND attribute_not_exists({})".format(key_schema['AttributeName'])

    try:
        Table.put_item(Item = item, ConditionExpression = condition_expression)
    except botocore.exceptions.ClientError as ex:
        if ex.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise
        elif ex.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return -1
        else:
            return

    return 0


def table_get_item(item):
    global Table

    try:
        response = Table.get_item(Key = item)
    except Exception as ex:
        print (ex)
        return {}

    if 'Item' in response:
        return response['Item']
    else:
        return None


def compare_item(item_a, item_b):
    if len(item_a) != len(item_b):
        return len(item_a) - len(item_b)

    for key in item_a:
        if item_a[key] != item_b[key]:
            return 1

    return 0


def init_files(output_path, file_name):
    ok_file_name = "{}/{}.ok".format(output_path, os.path.basename(file_name))
    fail_file_name = "{}/{}.fail".format(output_path, os.path.basename(file_name))
    duplicated_file_name = "{}/{}.dupl".format(output_path, os.path.basename(file_name))

    try:
        ok_file_name = open(ok_file_name, 'w', encoding='UTF8')
    except Exception as ex:
        print(ex)
        raise

    try:
        fail_file_name = open(fail_file_name, 'w', encoding='UTF8')
    except Exception as ex:
        print(ex)
        ok_file_name.close()
        raise

    try:
        duplicated_file_name = open(duplicated_file_name, 'w', encoding='UTF8')
    except Exception as ex:
        print(ex)
        ok_file_name.close()
        fail_file_name.close()
        raise

    return [ok_file_name, fail_file_name, duplicated_file_name]


def remove_duplication_in_list(data_list):
    return list(dict.fromkeys(data_list))


def dynamodb_type_converter(item):
    for k, v in list(item.items()):
        if v is None:
            del item[k]

        if type(v) == dict:
            item[k] = json.dumps(v)
        elif type(v) == list:
            item[k] = set(remove_duplication_in_list(v))

    return item


def table_put_items(file_name):
    global Table
    global Output

    [ok_f, fail_f, duplicated_f] = init_files(Output, os.path.basename(file_name))

    ok_cnt = 0
    fail_cnt = 0
    dupl_cnt = 0
    total_cnt = 0
    delay_cnt = 0

    delay_cnt = Table.provisioned_throughput['WriteCapacityUnits']

    try:
        with open(file_name, 'r') as json_f:
            for line in json_f.readlines():
                try:
                    item = json.loads(line)
                    item = dynamodb_type_converter(item)

                    ret = table_put_item(item)
                    if ret == 0:
                        ok_cnt += 1
                        ok_f.writelines(line)
                    elif ret == -1:
                        dupl_cnt += 1
                        duplicated_f.writelines(line)
                    else:
                        fail_cnt += 1
                        fail_f.writelines(line)
                except Exception as ex:
                    fail_cnt += 1
                    fail_f.writelines(line)
                total_cnt += 1
    finally:
        if not fail_f.closed:
            fail_f.close()
        if not ok_f.closed:
            ok_f.close()
        if not duplicated_f.closed:
            duplicated_f.close()

    with open("{}.result".format(file_name), 'a') as result:
        print("= Put Items ====================================================================", file=result)
        print("= table: {}".format(Table.name), file=result)
        print("= total: {}".format(total_cnt), file=result)
        print("= ok   : {}".format(ok_cnt), file=result)
        print("= fail : {}".format(fail_cnt), file=result)

    return 0


def table_verify_items(file_name):
    global Output
    global Table
    json_f = None
    fail_f = None

    try:
        json_f = open(file_name, 'r')
    except Exception as ex:
        print ("filename: {}[{}]".format(file_name, ex))
        return -1

    try:
        fail_f = open("{}/{}.verify.fail".format(Output, os.path.basename(file_name)), 'w')
    except Exception as ex:
        print ("filename: {}.verify.fail[{}]".format(file_name, ex))
        json_f.close()
        return -2

    cnt       = 0
    ok_cnt    = 0
    fail_cnt  = 0
    total_cnt = 0
    delay_cnt = 0

    delay_cnt = Table.provisioned_throughput['ReadCapacityUnits']

    line = json_f.readline()
    while line:
        try:
            req_item = {}
            item     = json.loads(line)

            for key_schema in Table.key_schema:
                req_item[key_schema['AttributeName']] = item[key_schema['AttributeName']]

            res_item = table_get_item(req_item)
            if compare_item(item, res_item) == 0:
                ok_cnt += 1
            else:
                fail_f.writelines(line)
                fail_cnt += 1
        except Exception as ex:
            print ("line: {}[{}]".format(total_cnt + 1, ex))
            fail_f.writelines(line)
            fail_cnt += 1

        line = json_f.readline()
        total_cnt += 1

    json_f.close()
    fail_f.close()

    with open("{}.result".format(file_name), 'a') as result:
        print ("= Verify Items =================================================================", file=result)
        print ("= table: {}".format(Table.name), file=result)
        print ("= total: {}".format(total_cnt), file=result)
        print ("= ok   : {}".format(ok_cnt), file=result)
        print ("= fail : {}".format(fail_cnt), file=result)

    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--file_name', required = True, help = 'json file')
    parser.add_argument('--table', required = True, help = 'table name')
    parser.add_argument('--region', required = False, help = 'aws region')
    parser.add_argument('--profile', required = False, help = 'aws profile')
    parser.add_argument('--verify', required = False, type = bool, default = False, help = 'table verify')
    parser.add_argument('--output', required = False, type = Path, default = './', help = 'declare the output path')

    args = parser.parse_args()

    Output = args.output

    try:
        if args.profile is None:
            if args.region is None:
                DynamoDB = boto3.resource('dynamodb')
            else:
                DynamoDB = boto3.resource('dynamodb', region_name=args.region)
        else:
            session = boto3.Session(profile_name = args.profile)
            if args.region is None:
                DynamoDB = session.resource('dynamodb')
            else:
                DynamoDB = session.resource('dynamodb', region_name=args.region)

        Table = DynamoDB.Table(args.table)

        if args.verify is True:
            table_verify_items(args.file_name)
        else:
            table_put_items(args.file_name)

    except Exception as ex:
        print(ex)

    cargo_stop = time.time()
    with open("{}/{}.result".format(Output, os.path.basename(args.file_name)), 'w') as result:
        print("Elapsed time during the whole program in seconds:", cargo_stop-cargo_start, file=result)