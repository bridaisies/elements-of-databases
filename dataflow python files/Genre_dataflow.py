import datetime, logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class SplitGenre(beam.DoFn):
  def process(self, element):
    # get the values from each individual column
    tconst = element['tconst']
    genres = element['genres']

    # make one overall list of records
    records = []
    # convert the comma separated string into a comma separated list
    if genres != None:
        genres = genres.split(',')
        for i in range(len(genres)):
            split_genre = (genres[i]).lower()
            # make a new record for each of the characters and append that to the overall 
            # records list
            record = {'tconst': tconst, 'genres': split_genre}
            records.append(record)
    # for values = None
    else:
        # make new record and append to list of records
        record = {'tconst': tconst, 'genres': genres}
        records.append(record)
        
    return records
           
def run():
     # set up location
     PROJECT_ID = 'trim-cistern-288221'
     BUCKET = 'gs://bhnk-milestone1-data'
     DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

     # use DataflowRunner instead of DirectRunner
     options = PipelineOptions(
     flags=None,
     runner='DataflowRunner',
     project=PROJECT_ID,
     job_name='imdbgenre',
     temp_location=BUCKET + '/temp',
     region='us-central1')

     p = beam.pipeline.Pipeline(options=options)

     # retrieve the data from imdb_refined dataset and save this information (location)
     sql = 'SELECT * FROM imdb_refined.Genre'
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     # use the previously saved information (location) and read from BigQuery
     # query results is now input P collection
     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)

     # Use ParDo to call function on query results
     out_pcoll = query_results | 'Split genres' >> beam.ParDo(SplitGenre())

     out_pcoll | 'Log output' >> WriteToText(DIR_PATH + 'output.txt')

     dataset_id = 'imdb_refined'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'Genre_Dataflow'
     schema_id = 'tconst:STRING,genres:STRING'

     # write to BigQuery using the location set above
     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     # run and display results after everything is finished
     result = p.run()
     result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()