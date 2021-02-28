import logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class SplitCast(beam.DoFn):
  def process(self, element):
    # get the values from each individual column
    Title = element['Title']
    Casts = element['Cast']

    # make one overall list of records
    records = []
    # convert the comma separated string into a comma separated list
    if Casts != None:
        Casts = Casts.split(',')
        for i in range(len(Casts)):
            split_cast = Casts[i]
            # make a new record for each of the characters and append that to the overall 
            # records list
            record = {'Title': Title, 'Cast': split_cast}
            records.append(record)
    # for values = None
    else:
        # make new record and append to list of records
        record = {'Title': Title, 'Director': Director}
        records.append(record)
        
    return records
           
def run():
     # set up location
     PROJECT_ID = 'trim-cistern-288221'
     BUCKET = 'gs://bhnk-milestone1-data'

     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # executed with DirectRunner
     p = beam.Pipeline('DirectRunner', options=opts)

     # retrieve the data from imdb_refined dataset and save this information (location)
     sql = 'SELECT * FROM kaggle_refined.Title_cast limit 50'
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     # use the previously saved information (location) and read from BigQuery
     # query results is now input P collection
     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)

     # Use ParDo to call function on query results
     out_pcoll = query_results | 'Split Cast' >> beam.ParDo(SplitCast())

     out_pcoll | 'Log output' >> WriteToText('output.txt')

     dataset_id = 'kaggle_refined'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'Title_cast_Beam'
     schema_id = 'Title:STRING,Cast:STRING'

     # write to BigQuery using the location set above
     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     # run and display results after everything is finished
     result = p.run()
     result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()
