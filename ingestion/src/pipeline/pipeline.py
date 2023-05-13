
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
    date_filter = config['transform']['date_filter']  
    output_filename = config['load']['output_filename'] 

    try:

        # Extract transactions
        logging.info("Extracting")
        extract_object = Extract(path=path_to_seed_folder)
        list_of_df = extract_object.run()

        # Transform transactions
        logging.info("Transforming")
        transform_object = Transform(list_of_df = list_of_df, date_filter=date_filter)
        transformed_df = transform_object.run()

        # Load transactions
        logging.info("Preparing Load nodes")
        load_node = Load(df=transformed_df, path_to_output_folder=path_to_output_folder, output_filename=output_filename)
        
        # Build dag
        dag = TopologicalSorter()
        
        # Adding load node
        dag.add(load_node)

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