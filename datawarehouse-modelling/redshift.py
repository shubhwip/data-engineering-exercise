import pandas as pd
import boto3
import json
import configparser
import sys
from colorama import Fore

def pretty_redshift_props(props):
    """
    It displays redshift cluster's properties 
    """
    pd.set_option('display.max_colwidth', 2)
    keys_to_show = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keys_to_show]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def create_iam_role(iam, iam_role_name):
    """
    It creates new iam role using iam client
    """
    role_arn = ""
    try:
        print('1.1 Creating a new IAM Role')
        role = iam.create_role(
            Path='/',
            RoleName=iam_role_name,
            Description="Aloows Redshift to call AWS services on your behalf",
            AssumeRolePolicyDocument=json.dumps(
                {
                    'Statement':[{'Action':'sts:AssumeRole',
                                 'Effect': 'Allow',
                                 'Principal': {'Service' : 'redshift.amazonaws.com'}}],
                    'Version':'2012-10-17'
                }
            )
        )
        print(role)
        
        print('1.2 Attaching Policy If the next result is 200 then it is success')
        iam_attach_response = iam.attach_role_policy(RoleName=iam_role_name,
                       PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
                       )['ResponseMetadata']['HTTPStatusCode']
        if iam_attach_response != 200 :
            return role_arn
        
        print('1.3 Get the IAM role ARN')
        role = iam.get_role(RoleName=iam_role_name)
        print(role)

        role_arn=role['Role']['Arn']
        print(role_arn)
        
        return role_arn
    
    except Exception as e:
        print("Exception occured in creating IAM role" + e)

def create_redshift_cluster(redshift, cluster_type, node_type, num_nodes, db_name, cluster_identifier, db_user, db_password, role_arn):
        try:
            redshift_cluster = redshift.create_cluster(        
                # add parameters for hardware
                ClusterType = cluster_type,
                NodeType = node_type,
                NumberOfNodes = int(num_nodes),

                # add parameters for identifiers & credentials
                DBName = db_name,
                ClusterIdentifier=cluster_identifier,
                MasterUsername=db_user,
                MasterUserPassword=db_password,
        
                # add parameter for role (to allow s3 access)
                IamRoles=[role_arn]
            )

            redshift_cluster_props = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
            pretty_redshift_props(redshift_cluster_props)
            print(redshift_cluster_props)

            redshift_clusters = redshift.describe_clusters()['Clusters']
            while len(redshift_clusters) > 0:
                df = pd.DataFrame(redshift_clusters)
                redshift_cluster=df[df.ClusterIdentifier==cluster_identifier.lower()]

                print("Cluster status is: {}".format(redshift_cluster['ClusterAvailabilityStatus'].item()))
                if redshift_cluster['ClusterAvailabilityStatus'].values[0].lower() == 'available':
                    break
                redshift_clusters = redshift.describe_clusters()['Clusters']

            redshift_cluster = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
            print(Fore.GREEN + "ENDPOINT :: ", redshift_cluster['Endpoint']['Address'])
            print(Fore.GREEN + "ROLE_ARN :: ", redshift_cluster['IamRoles'][0]['IamRoleArn'])
            print(Fore.GREEN + "Cluster Created Successfully")
            return redshift_cluster
        except Exception as e:
            print(e)

def cleanup(redshift, iam, cluster_identifier, iam_role_name):
    try:
        redshift_cluster = redshift.describe_clusters()['Clusters']
    
        if len(redshift_cluster) > 0:
            df = pd.DataFrame(redshift_cluster)
            redshift_cluster=df[df.ClusterIdentifier==cluster_identifier.lower()]
        
            print("My cluster status is: {}".format(redshift_cluster['ClusterAvailabilityStatus'].item()))
            redshift.delete_cluster( ClusterIdentifier=cluster_identifier,  SkipFinalClusterSnapshot=True)
    
            redshift_cluster = redshift.describe_clusters()['Clusters']
    
            while len(redshift_cluster) > 0:
                df = pd.DataFrame(redshift_cluster)
                redshift_cluster=df[df.ClusterIdentifier==cluster_identifier.lower()]

                print("My cluster status is: {}".format(redshift_cluster['ClusterAvailabilityStatus'].item()))
        
                redshift_cluster_props = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
                pretty_redshift_props(redshift_cluster_props)
                redshift_cluster = redshift.describe_clusters()['Clusters']
        else:
            print('No clusters available')
    except Exception as e:
        print(e)

    # Todo : Add checks if iam roles exists and then delete
    try:
        role = iam.get_role(RoleName=iam_role_name)
        role_arn=role['Role']['Arn']
        print("Deleting role" + role_arn)
        iam.detach_role_policy(RoleName=iam_role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=iam_role_name)
    except Exception as e:
        print(e)

def main():
    """
    Loads properties from datawarehouse configuration
    Creates boto3 clients for ec2, s3, iam and redshift
    """

    print('Number of arguments:', len(sys.argv), 'arguments.') 
    print('Argument List:', str(sys.argv))

    if len(sys.argv) > 2 :
        print("Incorrect number of argument passed")
        print("Please pass one argument create or delete")
        print("create will create the redshift cluster and clean existing redshift clusters")
        print("delete will delete the redshift clusters in your AWS account")
        exit(1)
    option = sys.argv[1]

    datawarehouse_config = configparser.ConfigParser()
    datawarehouse_config.read('dwh.cfg')
    KEY                = datawarehouse_config.get('AWS','KEY')
    SECRET             = datawarehouse_config.get('AWS','SECRET')
    CLUSTER_TYPE       = datawarehouse_config.get("CLUSTER","CLUSTER_TYPE")
    NUM_NODES          = datawarehouse_config.get("CLUSTER","NUM_NODES")
    NODE_TYPE          = datawarehouse_config.get("CLUSTER","NODE_TYPE")
    CLUSTER_IDENTIFIER = datawarehouse_config.get("CLUSTER","CLUSTER_IDENTIFIER")
    DB                 = datawarehouse_config.get("CLUSTER","DB")
    DB_USER            = datawarehouse_config.get("CLUSTER","DB_USER")
    DB_PASSWORD        = datawarehouse_config.get("CLUSTER","DB_PASSWORD")
    # Todo: Use below port for security group inbound rules
    #PORT               = datawarehouse_config.get("CLUSTER","PORT")
    IAM_ROLE_NAME      = datawarehouse_config.get("CLUSTER", "IAM_ROLE_NAME")

    # Todo : Add support for writing strict security group
    # Creating ec2 client
    # ec2 = boto3.client("ec2", region_name='us-west-2'
    #               , aws_access_key_id=KEY
    #               , aws_secret_access_key=SECRET)

    # Creating iam client
    iam = boto3.client("iam", region_name='us-west-2', aws_access_key_id=KEY
                  , aws_secret_access_key=SECRET)

    redshift = boto3.client("redshift", region_name='us-west-2', aws_access_key_id=KEY
                  , aws_secret_access_key=SECRET)
    if option == "create":
        # cleanup resources
        cleanup(redshift, iam, CLUSTER_IDENTIFIER, IAM_ROLE_NAME)

        # create iam role
        role_arn = create_iam_role(iam, IAM_ROLE_NAME)
        if role_arn == "":
            cleanup(redshift, iam, CLUSTER_IDENTIFIER, IAM_ROLE_NAME)
        datawarehouse_config['IAM_ROLE']['ARN'] = role_arn

        # create redshift cluster
        redshift_cluster_props = create_redshift_cluster(redshift, CLUSTER_TYPE, NODE_TYPE, NUM_NODES, DB, CLUSTER_IDENTIFIER, DB_USER, DB_PASSWORD, role_arn)

        datawarehouse_config['CLUSTER']['HOST'] = redshift_cluster_props['Endpoint']['Address']
        
        with open('dwh.cfg', 'w') as configfile:
            datawarehouse_config.write(configfile)

    elif option == "destroy":
        # cleanup resources
        cleanup(redshift, iam, CLUSTER_IDENTIFIER, IAM_ROLE_NAME)
        datawarehouse_config['IAM_ROLE']['ARN'] = ''
        datawarehouse_config['CLUSTER']['HOST'] = ''
        with open('dwh.cfg', 'w') as configfile:
            datawarehouse_config.write(configfile)
    else:
        print("Incorrect argument passed")
        print("Please pass one argument create or delete")
        print("create will create the redshift cluster and clean existing redshift clusters")
        print("delete will delete the redshift clusters in your AWS account")
        exit(1)


if __name__ == "__main__":
    main()


