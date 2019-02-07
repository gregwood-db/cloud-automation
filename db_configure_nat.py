# db_configure_nat.py - utility to configure a NAT gateway in a Databricks deployment
#
# Created by Databricks Enterprise Platform SME Group: ent-platform-sme@databricks.com
#
# This script will create a subnet, nat gateway, route table, and appropriate routes within the specified Databricks
# VPC. Note that you *must* obtain the VPC ID before running this script; we do not check whether the provided VPC ID
# is the correct one in this script. You must also have already configured Boto3 or AWS CLI prior to running this
# script, since it relies upon the AWS Shared Credentials file at ~/.aws/credentials.
#
# To run this script, simply invoke db_configure_nat, and follow the text prompts. If the provided VPC and subnet
# CIDR range are valid, the script should complete successfully. Common problems:
#   - Invalid or missing AWS credentials
#   - Improper permissions (must have EC2 permissions for the specified VPC)
#   - Incorrect VPC ID (must be the Databricks VPC)
#   - Invalid CIDR range (must be a.b.c.d/XX, and not currently occupied)
#
# For support, contact the Databricks Enterprise Platform SME group at ent-platform-sme@databricks.com.

import re
import time
import boto3
from botocore.exceptions import ClientError
from botocore.exceptions import NoRegionError
from botocore.exceptions import NoCredentialsError


# Create the EC2 client connection
def create_client(*args):
    """
    Create an EC2 client for downstream use. Must have a valid AWS configuration for boto3 to use.
    :param args: optional profile name for the AWS shared config file.
    :return: returns a boto3 EC2 client.
    """

    try:
        if len(args) == 0:
            client = boto3.Session().client('ec2')
        else:
            client = boto3.Session(profile_name=args[0]).client('ec2')

        # try to describe instances- this will fail if credentials or permissions are inappropriate
        client.describe_instances()
        return client

    except NoRegionError:
        raise Exception('Error creating client; please run `aws configure` to set up your environment first.')
    except NoCredentialsError:
        raise Exception('Error creating client; your AWS credentials were not found. Run `aws configure` to check.')
    except ClientError as e:
        raise Exception('Error creating client; please check your credentials and permissions. Error was: {}'.format(e))


# Create the subnet to host the NAT gateway
def create_subnet(client, cidr_blk, vpc_id):
    """
    Create a subnet in the given CIDR block and VPC using client.
    :param client: a valid boto3 EC2 client.
    :param cidr_blk: a valid IP range in the format 'a.b.c.d/XX'
    :type cidr_blk: str
    :param vpc_id: the VpcID of the Databricks VPC.
    :type vpc_id: str
    :return: returns the subnet ID of the created subnet
    """

    try:
        # create subnet on the Databricks VPC
        response = client.create_subnet(
            VpcId=vpc_id,
            CidrBlock=cidr_blk,
        )

    except ClientError as e:
        raise Exception('Error when creating subnet: {}', format(e))

    subnet_id = response.get('Subnet').get('SubnetId')
    print("Create subnet with subnetID {}".format(subnet_id))
    return subnet_id


def setup_nat_gateway(client, subnet):
    """
    Sets up a NAT gateway
    :param client: ec2 client
    :param subnet: the ID of an external subnet to connect the NAT gateway to
    :type subnet: str
    :return: the ID of the NAT gateway
    :rtype: str
    """

    try:
        allocation = client.allocate_address(Domain='vpc')
    except ClientError as e:
        raise Exception("Error allocating ip: {}".format(e))

    try:
        nat_gateway = client.create_nat_gateway(
            SubnetId=subnet,
            AllocationId=allocation['AllocationId']
        )

        nat_gateway_ids = nat_gateway.get('NatGateway').get("NatGatewayId")
        print("Creating NAT gateway {}...\n".format(nat_gateway_ids))
        client.get_waiter('nat_gateway_available').wait(NatGatewayIds=[nat_gateway_ids])
        print("Created NAT gateway\n")
        return nat_gateway_ids

    except Exception as e:
        print("Error returned when creating NAT Gateway Using subnet ID {}, Allocation ID: {}: {}\n"
              .format(subnet, allocation['AllocationId'], e))


def create_route_table(client, vpc, subnet):
    """
    Create a Route Table in the specified VPC, and associate it with the given subnet.
    :param client: EC2 client object.
    :param vpc: Databricks VPC ID.
    :type vpc: str
    :param subnet: The subnet ID of the subnet to be associated.
    :type subnet: str
    :return: Returns the RouteTableID from the created route table.
    """

    try:
        route_table = client.create_route_table(VpcId=vpc)
        route_table_id = route_table.get('RouteTable').get('RouteTableId')

        client.associate_route_table(RouteTableId=route_table_id, SubnetId=subnet)
    except ClientError as e:
        raise Exception("Error returned when creating Route Table: {}\n".format(e))

    return route_table_id


# Create a route to the NAT gateway
def create_route(client, route_table, destination, gateway, mode):
    """
    Creates a route, choose between gateway, peering or nat
    :param client: ec2 client connection
    :param route_table: route table ID
    :type route_table: str
    :param destination: destination CIDR
    :type destination: str
    :param gateway: destination gateway id
    :type gateway: str
    :param mode: either 'NAT' or 'IGW'
    :type mode: str
    :return: { 'Return': True|False }
    """

    try:
        if mode.lower() == 'nat':
            route = client.create_route(
                RouteTableId=route_table,
                DestinationCidrBlock=destination,
                NatGatewayId=gateway
            )

        elif mode.lower() == 'igw':
            route = client.create_route(
                RouteTableId=route_table,
                DestinationCidrBlock=destination,
                GatewayId=gateway
            )
        else:
            raise Exception('Inappropriate inputs for create_route. Mode must be either "nat" or "igw".')

        print("Route created\n")
        return route

    except ClientError as e:
        if "NotFound" in str(e):
            print("Retrying...\n")
            time.sleep(5)
            return create_route(client, route_table, destination, gateway, 'nat')
        else:
            raise Exception("Error returned when creating a route to {}, from {} using table {}: {}\n"
                            .format(destination, gateway, route_table, e))
    except Exception as e:
        if "NotFound" in str(e):
            print("Retrying...\n")
            time.sleep(5)
            return create_route(client, route_table, destination, gateway, 'nat')
        else:
            raise Exception("Error returned when creating a route to {}, from {} using table {}: {}\n"
                            .format(destination, gateway, route_table, e))


# Create an internet gateway and attach to the specified VPC
def create_igw(client, vpc):
    """
    Create a new internet gateway, and attach it to the specified VPC.
    :param client: EC2 client.
    :param vpc: The VPC to retrieve the IGW for.
    :type vpc: str
    :return: Returns the gateway ID
    """

    try:
        response = client.create_internet_gateway()
        igw = response.get('InternetGateway').get('InternetGatewayId')
        client.attach_internet_gateway(InternetGatewayId=igw, VpcId=vpc)
        return igw

    except ClientError as e:
        raise Exception("Could not create IGW. Error received: {}".format(e))


# Wrapper for main()
def configure_nat():
    main()


def main():

    # Create the EC2 client to use
    client = create_client()

    # User input for CIDR and VPC
    vpc = input("Enter the VPC ID of the Databricks VPC")
    while not vpc:
        print("Please provide a valid VPC ID.")
        input("Enter the VPC ID of the Databricks VPC")

    cidr = input("Enter the CIDR range for the subnet, in 'A.B'C'D/XX' format")

    # Verify CIDR
    cidr_re = '^([0-9]{1,3}\.){3}[0-9]{1,3}(\/([0-9]|[1-2][0-9]|3[0-2]))?$'
    cidr_re = re.compile(cidr_re)

    while not cidr_re.match(cidr):
        print("CIDR range format is invalid. Please enter a valid CIDR Range.")
        cidr = input("Enter the CIDR range for the subnet, in 'A.B'C'D/XX' format")

    # Create IGW for this VPC
    igw = create_igw(client, vpc)

    # Create the subnet in the Databricks VPC
    subnet_id = create_subnet(client, cidr, vpc)

    # Create the NAT gateway in the created subnet
    nat_gateway = setup_nat_gateway(client, subnet_id)

    # Create the route table in the VPC
    route_table = create_route_table(client, vpc, subnet_id)

    # Create route to IGW
    success = create_route(client, route_table, '0.0.0.0/0', igw, 'igw')
    if not success:
        raise Exception("Could not create IGW route. Please check that IGW exists and is attached.")

    # Get number of routes to create
    num_routes = input("How many routes need to be created?")
    success = 0

    # Validate # routes is numerical
    while success == 0:
        try:
            num_routes = int(num_routes)
            success = 1
        except ValueError:
            num_routes = input("Number of routes must be convertible to int. Please enter a number.")

    # Create routes in route table
    for i in range(num_routes):
        route_dest = input("Enter destination for route #{}".format(i))

        # Validate input CIDR range
        while not cidr_re.match(route_dest):
            print("CIDR range format is invalid. Please enter a valid CIDR Range.\n")
            route_dest = input("Enter destination for route #{}".format(i))

        # Create the route
        success = create_route(client, route_table, route_dest, nat_gateway, 'nat')

        if not success:
            print("Warning: Could not create route {}".format(route_dest))


if __name__ == "__main__":
    main()
