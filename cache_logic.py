import psycopg2
import redis
import json


# Redis setup
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# PostgreSQL setup
conn = psycopg2.connect(
    dbname='your_dbname',
    user='your_user',
    password='your_password',
    host='localhost'
)
cursor = conn.cursor()

def get_user_by_id(user_id):
    key = f"user:{user_id}"

    # Try to get from Redis
    cached = r.get(key)
    if cached:
        print("Cache hit")
        return json.loads(cached)

    # Else query PostgreSQL
    print("Cache miss, querying DB")
    cursor.execute("SELECT id, name, email FROM users WHERE id = %s", (user_id,))
    result = cursor.fetchone()
    if result:
        user_data = {"id": result[0], "name": result[1], "email": result[2]}
        r.set(key, json.dumps(user_data), ex=60*5)  # TTL = 5 minutes
        return user_data

    return None


print(get_user_by_id(1))
print(get_user_by_id(1))  # This should hit the cache the second time
