prefect deployment build -n "flows_to_gcs" --param color=green --param year=2020  flows/cloud/etl_web_to_gcs.py:flows_to_gcs -a --output ./flows/deployments/deploy_flows_to_gcs.yaml

prefect deployment build -n "flows_to_bq" --param color=green --param year=2020  flows/cloud/etl_gcs_to_bq.py:flows_gcs_to_bq -a --output ./flows/deployments/deploy_flows_gcs_to_bq.yaml