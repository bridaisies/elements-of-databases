import logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class SplitWriters(beam.DoFn):
  def process(self, element):
    # get the values from each individual column
    tconst = element['tconst']
    writers = element['writers']

    # make one overall list of records
    records = []
    # convert the comma separated string into a comma separated list
    if writers != None:
        writers = writers.split(',')
        for i in range(len(writers)):
            split_writers = writers[i]
            # make a new record for each of the characters and append that to the overall 
            # records list
            record = {'tconst': tconst, 'writers': split_writers}
            records.append(record)
    # for values = None
    else:
        # make new record and append to list of records
        record = {'tconst': tconst, 'writers': writers}
        records.append(record)
        
    return records
           
def run():
     PROJECT_ID = 'trim-cistern-288221'
     BUCKET = 'gs://bhnk-milestone1-data'

     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DirectRunner', options=opts)

     sql = 'SELECT * FROM imdb_refined.Writers limit 250'
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)

     out_pcoll = query_results | 'Split Writers' >> beam.ParDo(SplitWriters())

     out_pcoll | 'Log output' >> WriteToText('output.txt')

     dataset_id = 'imdb_refined'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'Writers_Beam'
     schema_id = 'tconst:STRING,writers:STRING'

     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     result = p.run()
     result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()
