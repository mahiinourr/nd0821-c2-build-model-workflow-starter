#!/usr/bin/env python
"""
Performs basic cleaning on the data and save the results in Weights & Biases
"""
import argparse
import logging
import pandas as pd
import wandb

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def clean_data(df):
    # Drop rows with missing values in important columns
    df_cleaned = df.dropna(subset=['neighbourhood_group', 'room_type', 'price', 'minimum_nights'])

    # Convert price to numeric and handle outliers or incorrect values
    df_cleaned['price'] = pd.to_numeric(df_cleaned['price'], errors='coerce')
    df_cleaned = df_cleaned[df_cleaned['price'].notna()]

    # Convert latitude and longitude to numeric (if necessary)
    df_cleaned['latitude'] = pd.to_numeric(df_cleaned['latitude'], errors='coerce')
    df_cleaned['longitude'] = pd.to_numeric(df_cleaned['longitude'], errors='coerce')

    # Handle categorical data if needed (e.g., encoding, mapping)
    df_cleaned['room_type'] = df_cleaned['room_type'].map({'Private room': 0, 'Entire home/apt': 1, 'Shared room': 2})

    return df_cleaned


def go(args):
    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Load input artifact

    artifact = run.use_artifact('iti-/nyc_airbnb/sample.csv:v0', type='raw_data')
    artifact_dir = artifact.download()

    # Load data from artifact
    data_path = artifact_dir + "/sample1.csv"
    df = pd.read_csv(data_path)

    # Perform data cleaning
    cleaned_df = clean_data(df)

    # Save cleaned data as a new artifact
    cleaned_artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description
    )

    # Write cleaned data to artifact
    cleaned_file_path = artifact_dir + "/cleaned_sample.csv"
    cleaned_df.to_csv(cleaned_file_path, index=False)
    cleaned_artifact.add_file(cleaned_file_path)
    run.log_artifact(cleaned_artifact)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="This script performs basic cleaning on the data")



    parser.add_argument(
        "--input_artifact",
        type=str,
        help="Name of the input artifact containing the data to be cleaned",
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Name for the output artifact containing the cleaned data",
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="Type of the output artifact. This will be used to categorize the artifact in the W&B interface",
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="A brief description of the output artifact",
        required=True
    )

    args = parser.parse_args()
    go(args)

