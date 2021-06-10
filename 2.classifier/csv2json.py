#!/usr/bin/python3
import argparse
import csv  # import json module
import json  # import json module
import pprint
import time

from exceptions.invalidException import InvalidException
from validator.field_validator import FieldValidator

PP = pprint.PrettyPrinter(indent=4)


def remove_duplication_in_list(data_list):
    if data_list is None:
        return None
    return list(dict.fromkeys(data_list))


def init_files(csv_file, stack, output_path):
    ocfrd_file_name = "{}/ocfrd_{}.out".format(output_path, stack)
    link_file_name  = "{}/ocfrd_link_{}.out".format(output_path, stack)
    no_migration_file_name = "{}/no_migration_{}.out".format(output_path, stack)
    invalid_file_name = "{}/invalid_{}.out".format(output_path, stack)

    try:
        csv_file = open(csv_file, 'r', encoding='UTF8')
    except Exception as ex:
        print(ex)
        raise

    try:
        ocfrd_file_name = open(ocfrd_file_name, 'w', encoding='UTF8')
    except Exception as ex:
        print(ex)
        csv_file.close()
        raise

    try:
        link_file_name = open(link_file_name, 'w', encoding='UTF8')
    except Exception as ex:
        print(ex)
        csv_file.close()
        ocfrd_file_name.close()
        raise

    try:
        no_migration_file_name = open(no_migration_file_name, 'w', encoding='UTF8')
    except Exception as ex:
        print(ex)
        csv_file.close()
        ocfrd_file_name.close()
        link_file_name.close()
        raise

    try:
        invalid_file_name = open(invalid_file_name, 'w', encoding='UTF8')
    except Exception as ex:
        print(ex)
        csv_file.close()
        ocfrd_file_name.close()
        link_file_name.close()
        no_migration_file_name.close()
        raise

    return [csv_file, ocfrd_file_name, link_file_name, no_migration_file_name, invalid_file_name]


def csv2json_ocfrd(csv_file_name, stack, output_path):
    [csv_f, ocfrd_f, link_f, no_migration_f, invalid_f] = init_files(csv_file_name, stack, output_path)

    cassandra_field_names = ('di', 'href', 'isMigrated', 'links', 'lt', 'n', 'xmodel')

    try:
        csv_reader = csv.DictReader(csv_f, cassandra_field_names, delimiter='\t', skipinitialspace=True)
        line = 1
        ocfrd_list = []
        for row in csv_reader:
            ocfrd_json = {}
            link_json  = {}
            dropped = False

            if not FieldValidator.validation_json_format(row):
                invalid_f.write("Invalid json format : {}".format(row))
                invalid_f.write('\n')
                continue

            row = pre_processed_items(row)
            update_time = int(time.time() * 1000)

            for key, value in row.items():
                if not isinstance(value, str):
                    invalid_f.write("Invalid character's contains : {}".format(row))
                    invalid_f.write('\n')
                    dropped = True
                    break

                if key == 'isMigrated' and value == "True":
                    no_migration_f.write("Drop(migration done) : {}".format(row))
                    no_migration_f.write('\n')
                    dropped = True
                    break

                if key == 'links':
                    if not FieldValidator.validation_json_format(value):
                        invalid_f.write("Invalid json format : {} - {}".format(row, value))
                        invalid_f.write('\n')
                        dropped = True
                        break

                    try:
                        FieldValidator.validation_links(value, row['di'], row['href'])
                    except InvalidException as e:
                        invalid_f.write("Invalid link json format : {} - {}".format(row, value))
                        invalid_f.write('\n')
                        dropped = True
                        break

                    link_json = json.loads(value)
                    if 'href' in link_json and not FieldValidator.validation_href(link_json['href']):
                        invalid_f.write("Invalid href : {} - {}".format(row, value))
                        invalid_f.write('\n')
                        dropped = True
                        break

                    link_json['ri'] = link_json['if']
                    del link_json['if']
                    if 'eps' in link_json:
                        del link_json['eps']
                    link_json['rt'] = remove_duplication_in_list(link_json['rt'])
                    link_json['ri'] = remove_duplication_in_list(link_json['ri'])
                    link_json['type'] = remove_duplication_in_list(link_json['type'])
                    link_json['updateTime'] = update_time

                else:
                    if key == 'href' or key == 'isMigrated':
                        continue

                    if key == 'xmodel':
                        ocfrd_json['model'] = value
                    elif key == 'lt':
                        ocfrd_json[key] = int(value)
                    else:
                        ocfrd_json[key] = value

            if dropped:
                continue

            if ocfrd_json['di'] not in ocfrd_list:
                ocfrd_list.append(ocfrd_json['di'])
                ocfrd_json['updateTime'] = update_time
                json.dump(ocfrd_json, ocfrd_f)
                ocfrd_f.write('\n')

            link_json['di'] = ocfrd_json['di']
            json.dump(link_json, link_f)
            link_f.write('\n')
            line += 1
    finally:
        if not ocfrd_f.closed:
            ocfrd_f.close()
        if not link_f.closed:
            link_f.close()
        if not csv_f.closed:
            csv_f.close()
        if not no_migration_f.closed:
            no_migration_f.close()
        if not invalid_f.closed:
            invalid_f.close()


def pre_processed_items(row):
    new_row = {}

    for key, value in row.items():
        if key == "href":
            if "\"" in value:
                value = value.replace("\"", "")
            if "\\" in value:
                value = value.replace("\\", "")
            if "\n" in value:
                value = value.replace("\n", "")

        if key == "links":
            if "\"{" in value:
                value = value.replace("\"{", "{").replace("}\"", "}")
            if "\\" in value:
                value = value.replace("\\", "")
            if "\n" in value:
                value = value.replace("\n", "")

        new_row[key] = value

    return new_row

def csv2json_ocfgatway_addhashkey(csv_file_name, field_names, output_path, check_empty, a_key_name, b_key_name, hash_key_name):
    json_file_name = csv_file_name.replace('.csv', '.out')
    err_file_name  = csv_file_name.replace('.csv', '.err')
    tmp_file_name  = csv_file_name.replace('.csv', '.tmp')

    try:
        csv_f = open(csv_file_name, 'r', encoding = 'UTF8')
    except Exception as ex:
        print (ex)
        return

    try:
        out_f = open("{}/{}".format(output_path, json_file_name), 'w')
    except Exception as ex:
        print (ex)
        csv_f.close()
        return

    try:
        err_f = open("{}/{}".format(output_path, err_file_name), 'w')
    except Exception as ex:
        print (ex)
        csv_f.close()
        out_f.close()
        return

    try:
        tmp_f = open("{}/{}".format(output_path, tmp_file_name), 'w')
    except Exception as ex:
        print (ex)
        csv_f.close()
        out_f.close()
        err_f.close()
        return

    data_json  = {}
    csv_reader = csv.DictReader(csv_f, field_names,  delimiter = '|')

    line = 1
    for row in csv_reader:
        is_empty = False
        for key, value in row.items():
            if key is not None:
                data_json[key] = value
                if (check_empty == True) and (value == ''):
                    is_empty = True
                    break

        #ocfgateway make hashkey by 2 other key's value
        data_json[hash_key_name] = data_json[a_key_name]+":"+data_json[b_key_name]
        if is_empty == False:
            try:
                if line == 1:
                   json.dump(data_json, out_f)
                else:
                   out_f.write('\n')
                   json.dump(data_json, out_f)
            except Exception as ex:
                print (ex)
                print (row, file = err_f)

            line += 1
        else:
            tmp_f.write("{}\n".format(row))

    tmp_f.close()
    err_f.close()
    out_f.close()
    csv_f.close()



def csv2json_common(csv_file_name, field_names, output_path, check_empty):
    json_file_name = csv_file_name.replace('.csv', '.out')
    err_file_name  = csv_file_name.replace('.csv', '.err')
    tmp_file_name  = csv_file_name.replace('.csv', '.tmp')

    try:
        csv_f = open(csv_file_name, 'r', encoding = 'UTF8')
    except Exception as ex:
        print (ex)
        return

    try:
        out_f = open("{}/{}".format(output_path, json_file_name), 'w')
    except Exception as ex:
        print (ex)
        csv_f.close()
        return

    try:
        err_f = open("{}/{}".format(output_path, err_file_name), 'w')
    except Exception as ex:
        print (ex)
        csv_f.close()
        out_f.close()
        return

    try:
        tmp_f = open("{}/{}".format(output_path, tmp_file_name), 'w')
    except Exception as ex:
        print (ex)
        csv_f.close()
        out_f.close()
        err_f.close()
        return

    data_json  = {}
    csv_reader = csv.DictReader(csv_f, field_names,  delimiter = '|')

    line = 1
    for row in csv_reader:
        is_empty = False
        for key, value in row.items():
            if key is not None:
                data_json[key] = value
                if (check_empty == True) and (value == ''):
                    is_empty = True
                    break

        if is_empty == False:
            try:
                if line == 1:
                   json.dump(data_json, out_f)
                else:
                   out_f.write('\n')
                   json.dump(data_json, out_f)
            except Exception as ex:
                print (ex)
                print (row, file = err_f)

            line += 1
        else:
            tmp_f.write("{}\n".format(row))

    tmp_f.close()
    err_f.close()
    out_f.close()
    csv_f.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_file', required = True, help = 'csv file')
    parser.add_argument('--field_names', required = False, help = 'csv field names')
    parser.add_argument('--output_file', required = False, help = 'json file')
    parser.add_argument('--output_path', required = False, default = '.', help = 'path for output')
    parser.add_argument('--stack', required = True, help = 'stack name')
    parser.add_argument('--ocfrd', required = False, type = bool, default = False, help = 'ocfrd')
    parser.add_argument('--ocfgateway_action_profile', required = False, type = bool, default = False, help = 'ocfgateway action_profile table')
    parser.add_argument('--ocfgateway_service_subscription', required = False, type = bool, default = False, help = 'ocfgateway service_subscription table')
    parser.add_argument('--check_empty', required = False, type = bool, default = False, help = 'check empty')

    args = parser.parse_args()

    if args.ocfrd is True:
        csv2json_ocfrd(args.input_file, args.stack, args.output_path)
    elif args.ocfgateway_action_profile is True:
    # ocfgateway action_profile table case
        field_names = []
        for field_name in args.field_names.split(','):
            field_names.append(field_name)

        csv2json_ocfgatway_addhashkey(args.input_file, field_names, args.output_path, args.check_empty,"mnmn","vid","mnmn_vid")
    elif args.ocfgateway_service_subscription is True:
    # ocfgateway service_subscription table
        field_names = []
        for field_name in args.field_names.split(','):
            field_names.append(field_name)

        csv2json_ocfgatway_addhashkey(args.input_file, field_names, args.output_path, args.check_empty,"app_id","subscription_id","app_id_subscription_id")
    else:
        field_names = []
        for field_name in args.field_names.split(','):
            field_names.append(field_name)

        csv2json_common(args.input_file, field_names, args.output_path, args.check_empty)
