# Launch Amazon EMR Cluster
aws emr create-cluster \
    --name etl_yelp_cluster \
    --use-default-roles \
    --release-label emr-5.28.0  \
    --instance-count 3 \
    --applications Name=Spark \
    --ec2-attributes KeyName=spark_etl_yelp_cluster,SubnetId=subnet-04a815f2d9bdc8b44 \
    --instance-type m5.xlarge \
    --profile yelp