from lib.s3_connect import S3Connect


class S3Connector:
    FILES = ("users.csv", "groups.csv", "dialogs.csv", "group_log.csv")

    def __init__(self, s3: S3Connect, bucket: str, key: str) -> None:
        if key not in self.FILES:
            raise ValueError("not valid table name")
        self.bucket = bucket
        self.key = key
        self.s3 = s3

    def download_dataset(self):
        self.s3.client().download_file(
            Bucket=self.bucket, Key=self.key, Filename=f"/lessons/dags/data/{self.key}"
        )
