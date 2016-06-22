# Create tags for all ELBs in a region based on tag data
# from associated instances & ami's

import boto3
import botocore.exceptions
import itertools
import csv
import argparse
import collections
import json
from datetime import datetime
import logging


# Create Session with IAM User
# session = boto3.session.Session(aws_access_key_id='ACCESS_KEY',
# aws_secret_access_key='ACCESS_KEY_SECRET')
session = boto3.session.Session(region_name="us-east-1")

# Create AWS clients
ec = session.client('ec2')
lb = session.client('elb')

# Create AWS resources
ec2_resource = session.resource('ec2')

DryRunFlag = False

# AWS Account and Region Definition for Reboot Actions
akid = "NOT_YET_DEFINED"
region = "NOT_YET_DEFINED"

# rd = session.client('rds')
# cw = session.client('cloudwatch')

global_tag_keys = ['Application', 'Environment', 'Version']
ignore_tag_key_prefixes = ["aws", "opsworks", "elasticbeanstalk"]
ignore_tag_keys = ['LaunchedBy', 'service', 'component']

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    global akid, region, interface_tags_to_be_tagged

    function_arn = context.invoked_function_arn
    arn, provider, service, region, akid, resource_type, resource_id = \
        function_arn.split(":")

    # Enumerate ELBs
    load_balancers = lb.describe_load_balancers()

    elb_count = 0
    for elb in load_balancers['LoadBalancerDescriptions']:
        elb_tags = lb.describe_tags(
            LoadBalancerNames=[
                elb['LoadBalancerName']
            ]
        )['TagDescriptions'][0]['Tags']

        if is_fully_tagged(elb_tags):
            message = elb['LoadBalancerName'] + " has all global keys"
            logger.info({"Message": message})
            continue

        # print elb['LoadBalancerName'], elb_tags, elb['Instances']
        instance_ids = [i['InstanceId'] for i in elb['Instances']]
        reservations = ec.describe_instances(InstanceIds=instance_ids).get('Reservations', [])
        instances = sum(
            [
                [i for i in r['Instances']]
                for r in reservations
            ], [])
        instance = instances[0]
        instance_tags = get_instance_tags(instance)
        image = ec2_resource.Image(instance['ImageId'])
        image_tags = get_image_tags(image)
        elb_tags_to_be_tagged = generate_tags_data(elb_tags, instance_tags, image_tags)
        if len(elb_tags_to_be_tagged) > len(elb_tags):
            logger.info({"Message": "Tagging data",
                     "LoadBalancerName": elb['LoadBalancerName'],
                     "Tags": str(elb_tags_to_be_tagged)})
            add_elb_tags(elb['LoadBalancerName'], elb_tags_to_be_tagged)
    output = event
    output['Status'] = "OK"
    return output


def is_fully_tagged(elb_tags):
    for key in global_tag_keys:
        if key not in [tag['Key'] for tag in elb_tags]:
            return False
    return True


def get_instance_tags(instance):
    instance_tags = [t for t in instance['Tags']
                     if not t['Key'].startswith('aws:') and
                     not t['Key'].startswith('opsworks:')]
    tags_dict = dict([(t['Key'], t['Value']) for t in instance['Tags']])

    if 'Application' not in tags_dict.keys():
        for tag in instance['Tags']:
            if tag['Key'] == 'opsworks:stack':
                instance_tags.append({'Key': 'Application', 'Value': tag['Value']})
                break
            elif tag['Key'] == 'elasticbeanstalk:environment-name':
                instance_tags.append({'Key': 'Application', 'Value': tag['Value']})
                break
        logger.info({"Tags": instance_tags})
    return instance_tags


def get_image_tags(image):
    try:
        image_tags = image.tags
        tags_dict = convert_tags_list_to_dict(image_tags)

        if 'Application' not in tags_dict.keys() and 'Project' in tags_dict.keys():
            image_tags.append({'Key': 'Application', 'Value': tags_dict['Project']})
            for tag in image_tags:
                if tag['Key'] == 'Project':
                    image_tags.remove(tag)
    except (AttributeError, TypeError):
        image_tags = []
    return image_tags


def convert_tags_list_to_dict(tags_list):
    return dict([(t['Key'], t['Value']) for t in tags_list])


def write_dict_to_csv(csv_file, csv_columns, dict_data):
    try:
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in dict_data:
                writer.writerow(data)
    except IOError as (errno, strerror):
            print("I/O error({0}): {1}".format(errno, strerror))
    return


def is_reserved_tag(tag_key):
    assert isinstance(tag_key, str)
    if tag_key.find(":") == -1:
        return False
    prefix = tag_key.split(":")[0]
    # logger.info({"Prefix": prefix, "Message": "Found reserved tag prefix"})
    if prefix in ignore_tag_key_prefixes:
        return True
    return False


def generate_tags_data(primary_tags, instance_tags, image_tags):
    assert isinstance(instance_tags, list)
    assert isinstance(image_tags, list)
    assert isinstance(primary_tags, list)
    result = [tag for tag in primary_tags if not is_reserved_tag(tag['Key'])]

    logger.info({"PrimaryTags": primary_tags, "Message": "Tags taken from the ELB"})
    for t in itertools.chain(instance_tags, image_tags):
        if t['Key'] in global_tag_keys and t['Key'] not in [r['Key'] for r in result]:
            logger.info({"Tag": t, "Message": "Found new tag"})
            result.append(t)
    return result


def generate_volume_tags(instance_tags, image_tags, volume_tags):
    assert isinstance(instance_tags, list)
    assert isinstance(image_tags, list)
    assert isinstance(volume_tags, list)
    result = []
    for t in itertools.chain(volume_tags, instance_tags, image_tags):
        if t['Key'] not in [r['Key'] for r in result] and t['Key'] in global_tag_keys:
                result.append(t)
    return result


def ignore_tag(tag):
    assert isinstance(tag, dict)
    if tag['Key'] in ignore_tag_keys:
        return True
    for prefix in ignore_tag_key_prefixes:
        if tag['Key'].startswith(prefix):
            return True
    return False


def create_ec2_resource_tags(resource_id, tags):
    try:
        for tag in [t for t in tags if t['Key'].startswith('Build') and t['Key'] not in global_tag_keys]:
            tags.remove(tag)
        print "Number of tags for %s: %i" % (resource_id, len(tags))
        ec.create_tags(Resources=[resource_id], DryRun=DryRunFlag, Tags=tags)
    except Exception, e:
        print ("Tagging Error Encountered.")
        print resource_id, tags
        print (e.message)


def add_elb_tags(elb_name, tags):
    parameters = {'LoadBalancerNames': [elb_name],
                  'Tags': tags}
    response = lb.add_tags(**parameters)
    logger.info({"Message": "Load balancers tagged", "Response": response})

def get_image_owner_id(image):
    try:
        return image.owner_id
    except:
        return 0


class Context:
    def __init__(self, **entries):
        self.__dict__.update(entries)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dryrun", help="Dry run - don't change anything in AWS")
    parser.add_argument("--accesskey", help="AWS Access Key")
    parser.add_argument("--secretkey", help="AWS Secret Key")
    args = parser.parse_args()

    cundo_aws_secret_access_key = args.secretkey
    context = dict([("invoked_function_arn","arn:aws:lambda:us-east-1:176853725791:function:cundo-lambda")])

    lambda_handler({}, Context(**context))
