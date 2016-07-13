import facebook
import requests
import pandas as pd
import sys
import psycopg2

''' GLOBAL PARAMETERS ============================================================'''

# Set your path HERE
PATH = 'XX'
# Set your Facebook access token HERE
access_token = 'XX'
# Set target users HERE
users = ['BillGates', 'MelindaGates']
# Set your database name HERE
DBNAME = 'posts'

''' CRAWL_AND_STORE FUNCTION ====================================================='''
def crawl_and_store():
    # Access Facebook API
    graph = facebook.GraphAPI(access_token)

    # Store info from posts in a list
    l_posts = []

    df = pd.DataFrame()
    # Retrieve information from 10 pages of posts (if exist)
    for user in users:
        profile = graph.get_object(user)
        posts = graph.get_connections(profile['id'], 'posts')
        n_pages = 0
        while True and n_pages < 10:
            try:
                l_posts += [post for post in posts['data']]
                posts = requests.get(posts['paging']['next']).json()
                n_pages += 1
            except KeyError:
                break
        # Convert to dataframe:
        df_u = pd.DataFrame(l_posts)
        # Add column with user
        df_u['user_id'] = user
        df = pd.concat([df_u, df], axis=0)

    # Format columns and order
    df.columns = ['created_time', 'post_id', 'message', 'story', 'user_id']
    df = df[['user_id', 'post_id', 'created_time', 'message']]
    for i in range(len(df)):
        try:
            df['message'][i] = df['message'][i].replace('\n',' ')
            df['message'][i] = df['message'][i].replace('  ',' ')
        except:
            next

    # Create csv file
    df.to_csv('posts.csv', encoding='utf-8-sig', index=False)

    # Set dbname
    DSN = 'dbname=' + DBNAME
    # Open connection
    conn = psycopg2.connect(DSN)
    curs = conn.cursor()

    # Erase schema if already exists
    try:
        curs.execute('DROP SCHEMA '+DBNAME+' CASCADE;')
    except:
        next
    # Create schema and table
    curs.execute('CREATE SCHEMA posts;')
    curs.execute('CREATE TABLE '+DBNAME+'.posts (user_id varchar(20), post_id ' +
                'char(40), created_time TIMESTAMP(24), message varchar(3500));')
    curs.execute('commit;')
    # Copy info from csv
    cop_command = "COPY "+DBNAME+".posts from " + PATH + "/posts.csv' CSV " + \
            "DELIMITER ',' HEADER;"
    curs.execute(cop_command)
    curs.execute('commit;')

    # Retrieve distinct users
    print "\n(1) Distinct users:"
    curs.execute("SELECT DISTINCT user_id from "+DBNAME+".posts;")
    query1 = curs.fetchall()
    print [x[0] for x in query1]

    # Retrieve messages in descending chronological order
    print "\n(2) Messages in chronological order:"
    curs.execute("SELECT user_id, message, created_time from "+DBNAME+
                 ".posts order by created_time desc;")
    query2 = curs.fetchall()
    print 'Retrieved posts in chronological order'
    print 'Showing first 5:'
    for i in range(5):
        print ' Post', i+1
        print '   User:    ',query2[i][0]
        print '   Message: ',query2[i][1]
        print '   Time:    ',query2[i][2]

    # Delete posts with blank message
    print "\n(3) Deleting posts with blank message:"
    curs.execute('SELECT COUNT(*) from '+DBNAME+'.posts WHERE message is NULL;')
    to_del = curs.fetchall()
    curs.execute('DELETE from '+DBNAME+'.posts WHERE message is NULL;')
    curs.execute('commit;')
    curs.execute('SELECT COUNT(*) from '+DBNAME+'.posts WHERE message is NULL;')
    deleted = curs.fetchall()
    print 'Total posts deleted: ', to_del[0][0] - deleted[0][0]
    # Close connection
    conn.rollback()


''' MAIN SCRIPT ============================================================'''
if __name__ == "__main__":
    crawl_and_store()

'''========================================================================='''