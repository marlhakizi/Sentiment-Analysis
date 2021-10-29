import json
import boto3
import csv


def lambda_handler(event, context):
    """Read file from s3 on trigger."""
    s3 = boto3.client("s3")
    if event:
        file_obj = event["Records"][0]
        bucketname = str(file_obj["s3"]["bucket"]["name"])
        filename = str(file_obj["s3"]["object"]["key"])
        fileObj = s3.get_object(
            Bucket="amaznreviews1",
            Key="results/1000reviews/2021/09/24/thousandreviews.csv",
        )
        file_content = fileObj["Body"].read().decode("utf-8").splitlines()
        reader = csv.DictReader(file_content)
        local_file_name = "/tmp/updatedthousand.csv"
        comprehend = boto3.client("comprehend")
        with open("/tmp/updatedthousand.csv", "w", newline="") as outfile:
            fieldnames = ["review_body", "sentiment"]
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in reader:
                result = comprehend.detect_sentiment(
                    Text=row["review_body"], LanguageCode="en"
                )
                writer.writerow(
                    {
                        "review_body": row["review_body"],
                        "sentiment": result["Sentiment"],
                    }
                )

        #    for line in reversed(reader): # reverse order
        #        writer.writerow(line)
        outfile.close()
        # upload file from tmp to s3 key
