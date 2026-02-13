import asyncio
import os
import sys
import psycopg

# Add project root to path
sys.path.insert(0, os.getcwd())

from app.core.config import DATABASE_URL

async def inspect():
    print(f"Connecting to {DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else 'DB'}...")
    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        async with conn.cursor() as cur:
            # Check lifecycle_labels columns
            await cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'lifecycle_labels'
            """)
            columns = [row[0] for row in await cur.fetchall()]
            print(f"Lifecycle Labels Columns: {columns}")
            
            # Check feature_snapshots columns
            await cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'feature_snapshots'
            """)
            columns = [row[0] for row in await cur.fetchall()]
            print(f"Feature Snapshots Columns: {columns}")

if __name__ == "__main__":
    asyncio.run(inspect())
