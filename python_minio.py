from minio import Minio
from minio.error import S3Error


def main():
    # Create a client with the MinIO server playground, its access key
    # and secret key.
    client = Minio(
        "192.168.2.39:9000",
        access_key="kraikrai",
        secret_key="kraikrai",
        secure=False
    )

    # Make 'asiatrip' bucket if not exist.
    print("HELLO01")
    buckets = client.list_buckets()
    print("HELLO2")
    for bucket in buckets:
        print(bucket.name, bucket.creation_date)

    # Upload '/home/user/Photos/asiaphotos.zip' as object name
    # 'asiaphotos-2015.zip' to bucket 'asiatrip'.
    # client.fput_object(
    #     "asiatrip", "asiaphotos-2015.zip", "/home/user/Photos/asiaphotos.zip",
    # )
    # print(
    #     "'/home/user/Photos/asiaphotos.zip' is successfully uploaded as "
    #     "object 'asiaphotos-2015.zip' to bucket 'asiatrip'."
    # )


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)