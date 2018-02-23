# using the psycopg2 module to connect to the database.
import psycopg2


# What are the most popular three articles?
title_of_query_1 = ("What are the most popular three articles?")
query_1 = (
    "select articles.title, count(*) as views "
    "from articles join log on log.path "
    "like concat('%', articles.slug, '%') "
    "where log.status like '%200%' group by "
    "articles.title, log.path order by views desc limit 3")

# Who are the most popular article authors?
title_of_query_2 = ("Who are the most popular article authors?")
query_2 = (
    "select authors.name, count(*) as views "
    "from articles join authors on articles.author = authors.id "
    "join log on log.path like concat('%', articles.slug, '%') "
    "where log.status like '%200%' "
    "group by authors.name order by views desc")

# On which days did more than 1% of requests lead to errors
title_of_query_3 = ("On which days did more than 1% of requests lead to errors?")
query_3 = (
    "select day, perc from ("
    "select day, round((sum(requests)/(select count(*) from log where "
    "substring(cast(log.time as text), 0, 11) = day) * 100), 2) as "
    "perc from (select substring(cast(log.time as text), 0, 11) as day, "
    "count(*) as requests from log where status like '%404%' group by day)"
    "as log_percentage group by day order by perc desc) as final_query "
    "where perc >= 1")


def connect(database_name="news"):
    """Connect to the PostgreSQL database. Returns a database connection """
    try:
        db = psycopg2.connect("dbname={}".format(database_name))
        cursor = db.cursor()
        return db, cursor
    except:
        print ("Unable to connect to the database")


def get_query_results(query):
    """Return query results for given query """
    db, cursor = connect()
    cursor.execute(query)
    return cursor.fetchall()
    db.close()


def print_query_results(query_results):
    print (query_results[1])
    for index, results in enumerate(query_results[0]):
        print (
            "\t", index+1, "-", results[0],
            "\t - ", str(results[1]), "views")


def print_error_results(query_results):
    print (query_results[1])
    for results in query_results[0]:
        print ("\t", results[0], "-", str(results[1]) + "% errors")


if __name__ == '__main__':
     # store query results
    popular_articles_results = get_query_results(query_1), title_of_query_1
    popular_authors_results = get_query_results(query_2), title_of_query_2
    load_error_days = get_query_results(query_3), title_of_query_3

     # print query results
    print_query_results(popular_articles_results)
    print_query_results(popular_authors_results)
    print_error_results(load_error_days)
