import psycopg2
import redis
import json
import time


# Redis setup on localhost for cache
r = redis.Redis()

# PostgreSQL setup on localhost for database 
postgres = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password='',
    host='localhost'
)
cursor = postgres.cursor()


def cache_plays_from_postgres():
    cursor.execute("SELECT * FROM play_by_play LIMIT 100")
    columns = [desc[0] for desc in cursor.description]

    for row in cursor.fetchall():
        row_dict = dict(zip(columns, row))

        # Use GameID + Drive + qtr + ttime as key
        game_id = row_dict.get("gameid", "unknown")
        drive = row_dict.get("drive", "unknown")
        qtr = row_dict.get("qtr", "unknown")
        ttime = row_dict.get("ttime", "unknown")

        redis_key = f"play:{game_id}:{drive}:{qtr}:{ttime}"
        #print(f"Caching play with key: {redis_key}")
        # Replace None with 'NA' or any default string
        safe_row_dict = {k: (str(v) if v is not None else "NA") for k, v in row_dict.items()}
        r.hset(redis_key, mapping=safe_row_dict)
    
    print("Cached plays from PostgreSQL to Redis.")


def get_play_from_postgres(game_id, drive, qtr, ttime):
    cursor.execute(
        "SELECT * FROM play_by_play WHERE GameID = %s AND Drive = %s AND qtr = %s AND ttime = %s",
        (game_id, drive, qtr, ttime)
    )
    return cursor.fetchone()


def get_play_with_cache(game_id, drive, qtr, ttime):
    redis_key = f"play:{game_id}:{drive}:{qtr}:{ttime}"
    #print(f"Checking cache for key: {redis_key}")
    play = r.hgetall(redis_key)

    if play:
        print("Cache hit for play:")
        return {k.decode(): v.decode() for k, v in play.items()}
    
    else:
        cursor.execute(
            "SELECT * FROM play_by_play WHERE GameID = %s AND Drive = %s AND qtr = %s AND ttime = %s",
            (game_id, drive, qtr, ttime)
        )
        result = cursor.fetchone()

        if result:
            columns = [desc[0] for desc in cursor.description]
            row_dict = dict(zip(columns, result))
            r.hset(redis_key, mapping=row_dict)
            print("Cache miss")
            return row_dict
        
        else:
            print("Play not found in database.")
            return None
    

if __name__ == "__main__":
    # Cache plays from PostgreSQL to Redis
    cache_plays_from_postgres()

    start = time.perf_counter()
    get_play_from_postgres("2009091000", "3", "1", "09:43")
    print("Postgres time:", time.perf_counter() - start)

    start = time.perf_counter()
    get_play_with_cache("2009091000", "3", "1", "09:43")
    print("Redis/Cache time:", time.perf_counter() - start)
