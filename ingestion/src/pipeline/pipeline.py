
from io import StringIO
from graphlib import TopologicalSorter
from etl.extract import Extract
from etl.transform import Transform
from etl.load import Load
import datetime as dt
import logging
import yaml

def run_pipeline():

    # set up logging 
    run_log = StringIO()
    logging.basicConfig(stream=run_log,level=logging.INFO, format="[%(levelname)s][%(asctime)s]: %(message)s")

    logging.info("Reading yaml config file")
    # get config variables
    with open("../config.yaml") as stream:
        config = yaml.safe_load(stream)
    
    logging.info("Getting yaml config variables")
    path_to_seed_folder = config['extract']['path_to_seed_folder']
    path_to_output_folder = config['load']['path_to_output_folder']

    try:

        # Extract transactions
        logging.info("Extracting")
        extract_object = Extract(path=path_to_seed_folder)
        list_of_df = extract_object.run()

        # Transform transactions
        logging.info("Transforming")
        transform_object = Transform(list_of_df = list_of_df)
        list_of_transformed_dfs = transform_object.run()

        # Load transactions
        logging.info("Preparing Load nodes")
        list_of_load_nodes = []
        for df in list_of_transformed_dfs:
            output_filename = df.attrs['name']
            load_node = Load(df=df, path_to_output_folder=path_to_output_folder, output_filename=output_filename)
            list_of_load_nodes.append(load_node)

        # Build dag
        dag = TopologicalSorter()
        
        # Adding load node
        for node in list_of_load_nodes:
            dag.add(node)

        # Run dag
        logging.info("Running DAG")
        dag_rendered = tuple(dag.static_order())
        for node in dag_rendered: 
            node.run()

        logging.info("Pipeline run successful")
    
    except Exception as e: 
        logging.exception(e)

    print(run_log.getvalue())

if __name__ == "__main__":
    run_pipeline()