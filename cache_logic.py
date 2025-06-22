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
        game_id = row_dict.get("GameID", "unknown")
        drive = row_dict.get("Drive", "unknown")
        qtr = row_dict.get("qtr", "unknown")
        ttime = row_dict.get("ttime", "unknown")

        redis_key = f"play:{game_id}:{drive}:{qtr}:{ttime}"
        r.hset(redis_key, mapping=row_dict)
    
    print("Cached plays from PostgreSQL to Redis.")


def get_play_from_postgres(game_id, drive, qtr, ttime):
    cursor.execute(
        "SELECT * FROM play_by_play WHERE GameID = %s AND Drive = %s AND qtr = %s AND ttime = %s",
        (game_id, drive, qtr, ttime)
    )
    return cursor.fetchone()


def get_play_with_cache(game_id, drive, qtr, ttime):
    redis_key = f"play:{game_id}:{drive}:{qtr}:{ttime}"
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
    get_play_from_postgres(5000)
    print("Postgres time:", time.perf_counter() - start)

    start = time.perf_counter()
    get_play_with_cache(5000)
    print("Redis/Cache time:", time.perf_counter() - start)
