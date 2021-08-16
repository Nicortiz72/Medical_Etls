import mysql.connector
import logging.config
import configparser
import database
import utils
import concept_file_parser
import os

def _get_logger():
    logger = logging.getLogger(__name__)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    dir_name, filename = os.path.split(os.path.abspath(__file__))
    output_file = dir_name + "/concept_etl.log"
    handler = logging.FileHandler(output_file)
    handler.setFormatter(formatter)
    logger.setLevel(logging.DEBUG) # DEBUG - INFO - WARN - ERROR
    logger.addHandler(handler)
    return logger

logger = _get_logger()

def load_concepts(file_path, concepts, cnx):
    concept_read = 0
    concept_inserted = 0
    concept_errors = 0
    row = 2
    for line in utils.read_csv_file(file_path, delimiter='\t'):
        concept = concept_file_parser.get_concept(line)
        concept_read += 1
        try:
            # Add new concept to dictionary
            concept_ref = str(concept['CONCEPT_ID']).strip() + "-" + str(concept['VOCABULARY_ID']).strip() + "-" + str(concept['CODE']).strip()
            if concept_ref not in concepts:
                id = database.add_concept(concept,
                                             cnx)
                                             
                concepts[concept_ref] = id
                logger.info("Inserting concept ref {0} in database.".format(concept_ref))
                concept_inserted += 1
            else:
                logger.info("concept ref {0} already exists in database.".format(concept_ref))
        except Exception as e:
            message = str(e) + " file: {0} - row: {1}".format(file_path, row)
            logger.error(message)
            print(message)
            concept_errors += 1
            return False
        row += 1
    return True

def execute(path_file):
    config = configparser.ConfigParser()
    config.read('config.ini')
    database_configuration = config['database']

    config = {
      'user': database_configuration['db_user'],
      'password': database_configuration['db_password'],
      'host': database_configuration['db_host'],
      'database': database_configuration['db_schema'],
      'raise_on_warnings': True
    }

    logger.info("Connecting to database...")
    cnx = mysql.connector.connect(**config)
    logger.info("The connection to the database was succesfull")

    logger.info('Getting all current concepts from database')
    concepts = database.get_current_concepts(cnx)
    print(concepts)

    print("*********** processing file %s *****************" % path_file)
    logger.info('processing file %s' % path_file)
    resultado = load_concepts(path_file, concepts, cnx)

    print("completed processing of the concepts")
    logger.info('Completed processing of file')
    return resultado
