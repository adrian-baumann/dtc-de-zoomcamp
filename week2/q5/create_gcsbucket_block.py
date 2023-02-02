# To circumvent the gcsbucket block in prefect cloud not allowing to add credentials
# create a gcs bucket block using the credentials block when logged into prefect cloud
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

gcp_credentials_block = GcpCredentials.load("gcs-dtc-cred")

GcsBucket(bucket="dtc-week3", gcp_credentials=gcp_credentials_block).save("gcs-dtc-bucket")
