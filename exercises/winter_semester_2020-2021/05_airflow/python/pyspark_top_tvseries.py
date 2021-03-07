import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import argparse
from pyspark.sql.functions import desc

def get_args():
    """
    Parses Command Line Args
    """
    parser = argparse.ArgumentParser(description='Some Basic Spark Job doing some stuff on IMDb data stored within HDFS.')
    parser.add_argument('--year', help='Partion Year To Process, e.g. 2019', required=True, type=str)
    parser.add_argument('--month', help='Partion Month To Process, e.g. 10', required=True, type=str)
    parser.add_argument('--day', help='Partion Day To Process, e.g. 31', required=True, type=str)
    parser.add_argument('--hdfs_source_dir', help='HDFS source directory, e.g. /user/hadoop/imdb', required=True, type=str)
    parser.add_argument('--hdfs_target_dir', help='HDFS target directory, e.g. /user/hadoop/imdb/ratings', required=True, type=str)
    parser.add_argument('--hdfs_target_format', help='HDFS target format, e.g. csv or parquet or...', required=True, type=str)
    
    return parser.parse_args()

if __name__ == '__main__':
    """
    Main Function
    """

    # Parse Command Line Args
    args = get_args()

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    # Read Title Basics from HDFS
    imdb_title_basics_dataframe = spark.read.format('csv')\
	.options(header='true', delimiter='\t', nullValue='null', inferschema='true')\
	.load(args.hdfs_source_dir + '/title_basics/' + args.year + '/' + args.month + '/' + args.day + '/*.tsv')

    # Read Title Ratings from HDFS
    imdb_title_ratings_dataframe = spark.read.format('csv')\
        .options(header='true', delimiter='\t', nullValue='null', inferschema='true')\
        .load(args.hdfs_source_dir + '/title_ratings/' + args.year + '/' + args.month + '/' + args.day + '/*.tsv')

    # Join IMDb Title Basics and ratings
    title_basics_and_ratings_df = imdb_title_basics_dataframe.join(imdb_title_ratings_dataframe, imdb_title_basics_dataframe.tconst == imdb_title_ratings_dataframe.tconst)

    # Get Top TV Series
    top_tvseries = title_basics_and_ratings_df.\
            filter(title_basics_and_ratings_df['titleType']=='tvSeries').\
            filter(title_basics_and_ratings_df['numVotes'] > 200000)\
            .orderBy(desc('averageRating'))\
            .select('originalTitle', 'startYear', 'endYear', 'averageRating', 'numVotes')
    
    # Write data to HDFS
    top_tvseries.write.format('csv').\
            mode('overwrite').\
            save(args.hdfs_target_dir)
