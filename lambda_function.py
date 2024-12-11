import json
import boto3
from urllib.parse import unquote_plus
from pdf2image import convert_from_path
from io import BytesIO
import base64
import os


# Assumptions:
# 1. The trigger of this function is an object creation event.
# 2. Exists in the 'input_files/' directory.
# 3. Is a PDF file.

BUCKET_NAME = "<S3-BUCKET-NAME>"

# Use these lambda layers
# arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p311-arm64-Pillow:4

# THIS IS A HACK! We want our own lambda layer's library to be loaded first. Other library paths are in
# the variable `LD_LIBRARY_PATH`.
os.environ["LD_PRELOAD"] = "/opt/lib/libz.so.1.2.11"


def lambda_handler(event, context):
    s3 = boto3.client("s3")

    # Sometimes, when lots of PDF get uploaded at a specific time range, they
    # are batched together and sent in the same event.
    for record in event["Records"]:
        # *** Conversion of PDF to images

        s3_object_key = unquote_plus(str(record["s3"]["object"]["key"]))
        pdf_filename_without_ext = s3_object_key.split("/")[-1].replace(".pdf", "")

        # Temporarily download the file
        temp_input_pdf_download_path = f"/tmp/{pdf_filename_without_ext}.pdf"
        s3.download_file(BUCKET_NAME, s3_object_key, temp_input_pdf_download_path)

        # Convert PDF to images using Poppler
        images = convert_from_path(temp_input_pdf_download_path, poppler_path="/opt/bin")

        # Save each page as a JPEG image in S3 bucket
        for i, image in enumerate(images):
            page_number = i + 1
            output_filename = f"pdf_to_images/{pdf_filename_without_ext}/page{page_number:03d}.jpeg"
            temp_image_path = f"/tmp/page{page_number:03d}-{pdf_filename_without_ext}.jpeg"

            # Save the image temporarily
            image.save(temp_image_path, format="JPEG")

            # Save the page image in S3
            with open(temp_image_path, "rb") as file:
                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=output_filename,
                    Body=file,
                )

            print(f"Uploaded {output_filename} to bucket {BUCKET_NAME}")

        # 🧹✨ Clean up the temporary files
        os.remove(temp_input_pdf_download_path)
        for i in range(len(images)):
            temp_image_path = f"/tmp/page{i + 1:03d}-{pdf_filename_without_ext}.jpeg"
            os.remove(temp_image_path)

        print(
            f"Processed PDF {pdf_filename_without_ext} and saved images to {BUCKET_NAME}/pdf_to_images/{pdf_filename_without_ext}/"
        )

    return {"statusCode": 200, "body": json.dumps(f"{event['Records']} job(s) ran successfully!")}
