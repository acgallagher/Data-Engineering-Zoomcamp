prefect deployment build ./week2/parameterized_flow.py:etl_parent_flow \
-n "Github Storage Flow" \
-sb github/zoomcamp-prefect \
-o ./week2/etl_parent_flow-deployment.yaml \
--apply 
