"""
DSQL PingPong Demo Script

A skeleton demonstration script for Amazon Aurora DSQL using SQLAlchemy.
"""

import argparse
import boto3
from sqlalchemy import create_engine, event, BigInteger, Column
from sqlalchemy.engine import URL
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.sql import text


class Base(DeclarativeBase):
    pass


class PingPong(Base):
    __tablename__ = "pingpong"
    __table_args__ = {"schema": "demo"}
    
    id = Column(BigInteger, primary_key=True, autoincrement=False)
    value = Column(BigInteger)


def initialize(engine):
    """Initialize the demo schema and tables."""
    print("Initializing demo schema...")
    
    # Create schema
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS demo"))
        conn.commit()
    
    # Drop and recreate the pingpong table
    PingPong.__table__.drop(engine, checkfirst=True)
    PingPong.__table__.create(engine)
    
    # Insert the single row with id=1, value=0
    session = Session(engine)
    initial_row = PingPong(id=1, value=0)
    session.add(initial_row)
    session.commit()
    session.close()
    
    print("Demo schema initialized")


def start_game(engine):
    """Start the game by updating the value to 1."""
    session = Session(engine)
    rows_updated = session.query(PingPong).filter(PingPong.id == 1).update({PingPong.value: 1})
    session.commit()
    session.close()
    
    assert rows_updated == 1, f"Expected to update 1 row, but updated {rows_updated} rows"
    print("Started the game")


def run_until_interrupted(start_flag, engine):
    """Run the game loop until interrupted by Ctrl-C."""
    # Initialize lastValueWritten: what this instance last wrote
    # Starter wrote 1, non-starter wrote nothing (0)
    lastValueWritten = 1 if start_flag else 0
    
    try:
        while True:
            # Read the singleton row
            session = Session(engine)
            row = session.query(PingPong).filter(PingPong.id == 1).first()
            value = row.value
            session.close()
            
            # Check if value read is higher than last value written by this instance
            if value > lastValueWritten:
                # Write the next value (current value + 1)
                lastValueWritten = value + 1
                
                # Update the value in DSQL
                session = Session(engine)
                session.query(PingPong).filter(PingPong.id == 1).update({PingPong.value: lastValueWritten})
                session.commit()
                session.close()
                
                # Print Ping or Pong based on start flag
                message = "Ping" if start_flag else "Pong"
                print(f"{message} {lastValueWritten}", end="", flush=True)
            else:
                # Print a period without newline
                print(".", end="", flush=True)
    except KeyboardInterrupt:
        print("\nGame interrupted. Exiting...")


def create_sqlalchemy_engine(cluster_url, client, region):
    """Create SQLAlchemy engine for DSQL."""
    url = URL.create(
        "auroradsql+psycopg2",
        username="admin",
        host=cluster_url,
        database="postgres",
    )
    engine = create_engine(url)
    
    # Add token management
    @event.listens_for(engine, "do_connect")
    def add_token_to_params(dialect, conn_rec, cargs, cparams):
        token = client.generate_db_connect_admin_auth_token(cluster_url, region)
        cparams["password"] = token
    
    print("SQLAlchemy engine initialized")
    return engine


def main():
    """Main entry point for the demo."""
    parser = argparse.ArgumentParser(description="DSQL PingPong Demo")
    parser.add_argument("--identifier", required=True, help="Cluster ID")
    parser.add_argument("--start", action="store_true", default=False, help="Start the demo")
    parser.add_argument("--initialize", action="store_true", default=False, help="Initialize the schema then exit")
    
    args = parser.parse_args()
    
    print(f"Cluster: {args.identifier}")
    print(f"Start: {args.start}")
    
    # Initialize boto3 client
    client = boto3.client("dsql")
    region = client.meta.region_name
    print(f"Boto3 DSQL client initialized in region: {region}")
    
    # Generate cluster URL
    cluster_url = f"{args.identifier}.dsql.{region}.on.aws"
    print(f"Cluster URL: {cluster_url}")
    
    # Initialize SQLAlchemy engine
    engine = create_sqlalchemy_engine(cluster_url, client, region)
    
    # Handle initialize flag
    if args.initialize:
        initialize(engine)
        return
    
    # Handle start flag
    if args.start:
        start_game(engine)
    
    run_until_interrupted(args.start, engine)


if __name__ == "__main__":
    main()
