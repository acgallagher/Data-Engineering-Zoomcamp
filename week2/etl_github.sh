prefect deployment build /home/andrewgallagher/Documents/Repositories/Data-Engineering-Zoomcamp/week2/parameterized_flow.py:etl_parent_flow \
-n "Github Storage Flow" \
-sb github/zoomcamp-prefect/week2/parameterized_flow.py \
-o /home/andrewgallagher/Documents/Repositories/Data-Engineering-Zoomcamp/week2/etl_parent_flow-deployment.yaml \
--apply 
