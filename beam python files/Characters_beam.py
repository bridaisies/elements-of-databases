import logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class SplitCharacters(beam.DoFn):
  def process(self, element):
    tconst = element['tconst']
    nconst = element['nconst']
    characters = element['characters']

    # make one overall list of records
    records = []

    # if the character in the characters string is not a ", [, or ], add it to the string character
    # keep a comma separated string
    character = ""
    if characters != None:
        new = ""
        for j in range(len(characters)):
            if characters[j] != "\"" and characters[j] != "[" and characters[j] != "]":
                new += characters[j]
        # assign this updated string to the variable characters
        characters = new
        
    # convert the comma separated string into a comma separated list                                    
    if characters != None:
        characters = characters.split(',')
        for i in range(len(characters)):
            split_characters = characters[i]
            # make a new record for each of the characters and append that to the overall 
            # records list
            record = {'tconst': tconst, 'nconst': nconst, 'characters': split_characters}
            records.append(record)
    # for characters = None
    else:
        # make new record and append to list of records
        record = {'tconst': tconst, 'nconst': nconst, 'characters': characters}
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
     sql = 'SELECT * FROM imdb_refined.Characters limit 250'
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     # use the previously saved information (location) and read from BigQuery
     # query results is now input P collection
     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)

     # Use ParDo to call function on query results
     out_pcoll = query_results | 'Split characters' >> beam.ParDo(SplitCharacters())

     # write the results into text file
     out_pcoll | 'Log output' >> WriteToText('output.txt')

     dataset_id = 'imdb_refined'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'Characters_Beam'
     schema_id = 'tconst:STRING,nconst:STRING,characters:STRING'

     # write to BigQuery using the location set above
     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     # run and display results after everything is finished
     result = p.run()
     result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()