import json
import boto3
from urllib.parse import unquote_plus
from pdf2image import convert_from_path
from io import BytesIO
import base64
import os


SNS_TOPIC_ARN = "<SNS topic ARN for textract job completion>"
SNS_ROLE_ARN = "<SNS role ARN for textract>"

# We are using the same bucket for input PDF and the output results.
# Input folder name: input_pdfs/
# Textract response folder name: textract_responses/
BUCKET_NAME = "<Bucket name>"

# Use these lambda layers
# arn:aws:lambda:us-east-1:637423511258:layer:python-mds-layer:8
# arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p311-arm64-Pillow:4

# THIS IS A HACK! We want our own lambda layer's library to be loaded first. Other library paths are in
# the variable `LD_LIBRARY_PATH`.
os.environ['LD_PRELOAD'] = "/opt/lib/libz.so.1.2.11"


def lambda_handler(event, context):
    textract = boto3.client("textract")
    s3 = boto3.client("s3")

    # Sometimes, when lots of PDF get uploaded at a specific time range, they
    # are batched together and sent in the same event.
    failed_jobs_count = 0
    for record in event["Records"]:
        s3_object_key = unquote_plus(str(record["s3"]["object"]["key"]))
        mds_job_id = s3_object_key.split("/")[-1].replace(".pdf", "")

        response = textract.start_document_text_detection(
            DocumentLocation={"S3Object": {"Bucket": BUCKET_NAME, "Name": s3_object_key}},
            OutputConfig={"S3Bucket": BUCKET_NAME, "S3Prefix": f"textract_responses/mds_job_{mds_job_id}"},
            NotificationChannel={"SNSTopicArn": SNS_TOPIC_ARN, "RoleArn": SNS_ROLE_ARN},
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            print(f"⚠️ Failed for MDS job: {mds_job_id}")
            failed_jobs_count += 1
        else:
            print("Textract job ID:", response["JobId"])
            print(f"MDS job ID: {mds_job_id}")

        # Convert PDF to images!

        # Temporarily download the file
        temp_download_path = f"/tmp/{mds_job_id}.pdf"
        s3.download_file(BUCKET_NAME, s3_object_key, temp_download_path)

        # Convert PDF to images using Poppler
        images = convert_from_path(temp_download_path, poppler_path="/opt/bin")

        # Convert images to base64 and store in S3
        page_image_base64_urls = []
        for i, image in enumerate(images):
            buffered = BytesIO()
            image.save(buffered, format="JPEG")
            img_str = base64.b64encode(buffered.getvalue()).decode('utf-8')
            page_image_base64_urls.append(f"data:image/jpeg;base64,{img_str}")

        # Save the images in a JSON, as base64 URLs so that we can access them together
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"pdf_to_images/{mds_job_id}.json",
            Body=json.dumps({"base64_urls": page_image_base64_urls}, indent=4, sort_keys=True),
        )

    if failed_jobs_count:
        return {
            "statusCode": 200,
            "body": json.dumps(
                f"{failed_jobs_count} jobs were failed out of {len(event['Records'])}!"
            )
        }

    return {
        "statusCode": 200, "body": json.dumps("Job(s) created successfully!")
    }
