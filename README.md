# Pinterest Data Pipeline
Pinterest is a world-class visual discovery platform where users can find ideas or inspirations and share useful contents.

## Table Content
- [Brief Description](#brief-description)
    - [Tools used for the project](#tools-used-for-the-project)
- [Installation Instructions](#installation-instructions)
    - [Setting up AWS Cloud account - Step by Step](#set-up-aws-cloud-account)
    - [Creating a Key Pair](#creating-a-key-pair)
    - [Setting up Amazon EC2 Client Machine:](#setting-up-amazon-ec2-client-machine)
- [Usage Instructions](#usage-instructions)
- [File Structure of the Project](#file-structure-of-the-project)
- [License Information](#license-information)

## Brief Description
For this project, the scenario is to create a system that uses the AWS Cloud that will enables the Pinterest to crunch billions of data points each day by running through two separate pipelines - Batch Processing and Stream Processing. 

### Definition of Batch & Stream Processing:
- Batch Processing is where data is aggregated, before being processed all at once in bulk
- Stream Processing is where data is processed as soon as it is ingested into the system

### Tools used for this project :
- Amazon EC2 - is used as an Apache Kafka client machine
    - Amazon Elastic Compute Cloud (EC2) is a key component of Amazon Web Services (AWS) and plays a vital role in cloud computing. EC2 provides a scalable and flexible infrastructure for hosting virtual servers, also known as instances, in the cloud.
- Apache Kafka - Apache Kafka is a relatively new open-source technology for distributed data storage optimised for ingesting and processing streaming data in real-time. Kafka provides 3 main functions:
    1.  Publish and subscribe to streams of records
    1.  Effectively store streams of records in the order in which records were generated
    1.  Process streams of records in real-time

    Kafka is primarily used to build real-time streaming data pipelines and applications that adapt to the data streams. It combines messaging, storage, and stream processing to allow storage and analysis of both historical and real-time data.

- Amazon MSK - Amazon Managed Streaming for Apache Kafka (Amazon MSK) is a fully managed service used to build and run applications that use Apache Kafka to process data. Apache Kafka is an open-source technology for distributed data storage, optimized for ingesting and processing streaming data in real-time.

    Amazon MSK makes it easy for you to build and run production applications on Apache Kafka without needing Apache Kafka infrastructure management expertise. That means you spend less time managing infrastructure and more time building applications.

- AWS MSK Connect - AWS MSK Connect is used to connect the MSK Cluster to a dedicated S3 Bucket, which will automatically save any data that is going through the cluster.

    MSK Connect is a feature of AWS MSK, that allows users to stream data to and from their MSK-hosted Apache Kafka clusters. With MSK Connect, you can deploy fully managed connectors that move data into or pull data from popular data stores like Amazon S3 and Amazon OpenSearch Service, or that connect Kafka clusters with external systems, such as databases and file systems. 

- Amazon S3 - Amazon S3, also known as Simple Storage Service, is a scalable and highly available object storage service provided by AWS. Its primary purpose is to store and retrieve large amounts of data reliably, securely, and cost-effectively.

## Installation Instructions
Setting up and steps followed:

### Set up AWS Cloud account:
For this project an AWS Cloud account must be created to be able to use different services that runs in the AWS Cloud. To create an AWS Cloud account visit the [AWS Website](https://colab.research.google.com/corgiredirector?site=https%3A%2F%2Fconsole.aws.amazon.com%2Fconsole%2Fhome) and click on 'Create a new AWS account'.

![](images/aws_cloud_account.png)

#### Step by Step set-up:
1.  Provide your email address - This will be the email address for the root user, who will have full administrative access to the account.
1.  Choose an account name - The account name will be used as a subdomain in the default URL of your AWS resources.
1.  Verification email - Check your email inbox and look for the email from AWS with the subject AWS Email Verification.
1.  Enter the verification code
1.  Set up root user password - Set the password for the root user
1.  Select a support plan - Choose the plan that best fits your needs (e.g Personal)
1.  Enter your payment information - AWS requires this information for billing purposes. You will only be charged for the services you use beyond the AWS Free Tier limits
1.  Confirm your identity - Select the option you prefer and click on Call me now or Send SMS to receive a verification code
1.  Enter the verification code
1. Select a support plan - Select the Basic plan and click on Complete sign up
1. Wait for AWS account activation
1. Access your AWS account

### Document
In order to mimic the kind of work that Data Engineers work at Pinterest, this project contained a Python script, [user_posting_emulation.py](user_posting_emulation.py). When running in terminal it executes random data points (as seen below images) for those 3 tables that are received by the Pinterest API when a POST request was made by the user. Below are the 3 tables that was contained in the AWS RDS Database:

-   'pinterest_data' contains data about posts being updated to Pinterest

![](images/pin_data.png)

-   'geolocation_data' contains data about the geolocation of each Pinterest post found in 'pinterest_data'

![](images/geo_data.png)

-   'user_data' contains data about the user that has uploaded each post found in 'pinterest_data'

![](images/user_data.png)

### Creating a Key Pair
Definition of Key Pair

-   In Amazon EC2, a key pair is a secure method of accessing your EC2 instances. It consists of a public key and a corresponding private key. The public key is used to encrypt data that can only be decrypted using the private key. Key pairs are essential for establishing secure remote access to your EC2 instances.

To create a new key pair navigate to EC2 console > Network & Security > Key Pair and top right hand corner click 'Create Key Pair', which will take the user to the below page. Here is where the user will create the new Key Pair. Give the key pair a descriptive name and choose 'RSA' and '.pem' for the file extension name. 

![](images/create_key_pair.png)


### Setting up Amazon EC2 Client Machine:

In order for the EC2 Client Machine to connect via SSH (Secure Shell), a private key is required that is associated with the Key Pair that is used during the instance launch.

To connect using an SSH client:

![](images/connect_to_instance.png)



1. Ensure you have the private key file (.pem) associated with the key pair used for the instance.

1. Open the terminal in the VSC application. In the terminal an appropriate permission needs to be set for the private key to be only accessible to the owner. Here is the command to set the permission:


        chmod 400 /path/to/private_key.pem

1. Use the SSH command to connect to the EC2 instance, which can be found under the Example in the SSH client tab, if already in the folder where the .pem then no need to specify the filepath name just enter the private_key.pem file

        ssh -i /path/to/private_key.pem ec2-user@public_dns_name

1. When accessing the EC2 Client Machine for the first time it may prompt a message about the authenticity of the host. The reason for this is because the SSH Client does not recognise the remote host and wanted to verify for secure communication. Enter yes to confirm and continue to connect.

1. Once the connection is successful the below image should show to confirm that the user has connected to the EC2 Client Machine

    ![](images/ec2_cm.png)


## Usage Instructions

## File Structure of the Project

## License Information