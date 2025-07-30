"""
DSQL PingPong Demo Script

A skeleton demonstration script for Amazon Aurora DSQL using SQLAlchemy.
"""

import argparse
import boto3
from sqlalchemy import create_engine, event
from sqlalchemy.engine import URL


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


if __name__ == "__main__":
    main()
