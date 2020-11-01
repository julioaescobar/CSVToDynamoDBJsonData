import pandas as pd
from boto3.dynamodb.types import TypeSerializer
import json
import csv


def replaceObjectName(data):
    data = data.replace('"M"', '"m"')
    data = data.replace('"L"', '"l"')
    data = data.replace('"S"', '"s"')
    data = data.replace('"N"', '"n"')
    return data


def converterToDynamodbFormat(data):
    typer = TypeSerializer()
    dynamodbJsonData = json.dumps(typer.serialize(data)['M'])
    return replaceObjectName(dynamodbJsonData)


df = pd.read_csv('data/data4.csv', sep=';', encoding='latin-1',dtype={'ek_identification': object,'first_name': object,'last_name': object,
'email': object,'type_doc': object,'doc': object,'cel_phone': object,'phone': object,'node': object,'test': object,'registration_date': object,
'start_time': object,'end_time': object,'url_sumadi': object,'url_admin_password': object,'password': object,'register_status': object})
user_number, columns_number = df.shape
print(columns_number)


students = []
for student_index in range(user_number):
    student = {}
    for columns_index in range(columns_number):
        column_name = df.columns[columns_index]
        if(column_name == "node"):
            student[column_name] = str(df[column_name][student_index])[-1]
        else:
            student[column_name] = str(df[column_name][student_index])
    students.append(student)


with open("data-pipeline/datacsvProcessed4.txt", 'w+') as f:
    for student in students:
        f.write(converterToDynamodbFormat(student))
        #f.write(json.dumps(student))
        f.write('\n')
