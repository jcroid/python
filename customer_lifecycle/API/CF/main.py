
import psycopg2
import pandas.io.sql as psql


"""
what about secret-manager? in non sample env
api call to secret manager will be use to get connection details
example

secrets = secretmanager.SecretManagerServiceClient(project=PROJECT_ID)

params_dic = secrets.access_secret_version(
    request={"name": "projects/" + PROJECT_ID + "/secrets/params_dic/versions/1"}).payload.data.decode("utf-8")

"""


def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn


def postgresql_to_json(params_dic, select_query):
    """
    Tranform a SELECT query into a pandas dataframe
    """

    conn = connect(params_dic)
    try:
        df = psql.read_sql(select_query, conn)
        df = df.to_json(orient='records')
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        return 1

    return df


def customer_lifecycle(request):
    """ HTTP Cloud Function
    pass json data as input for the required vars
    JSON output for the requested data

    """
    request_json = request.get_json(silent=True)

    if request_json and 'query' in request_json and 'postgres_dic' in request_json:
        if request_json['query'] == 'QUERY' or request_json['postgres_dic']['host'] == 'IP_ADDRESS':
            output_json = {"error": "sample file"}
        else:
            params_dic = request_json['postgres_dic']
            query = request_json['query']
            if 'underwriter' in request_json or 'month' in request_json:
                if 'underwriter' in request_json and 'month' in request_json:
                    underwriter = request_json['underwriter']
                    month = request_json['month']
                    query = query.replace(
                        '{underwriter}', underwriter).replace('{month}', month)
                    output_json = postgresql_to_json(params_dic, query)
                elif 'underwriter' in request_json:
                    underwriter = request_json['underwriter']
                    query = query.replace('{underwriter}', underwriter)
                    output_json = postgresql_to_json(params_dic, query)
                else:
                    month = request_json['month']
                    query = query.replace('{month}', month)
                    output_json = postgresql_to_json(params_dic, query)
            else:
                output_json = postgresql_to_json(params_dic, query)

    else:
        output_json = {"error": "no query sent"}

    headers = {'Content-Type': 'application/json; charset=utf-8'}
    return output_json, headers
