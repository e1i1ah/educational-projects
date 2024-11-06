import boto3


class S3Connect:
    def __init__(
        self, aws_access_key_id: str, aws_secret_access_key: str, endpoint_url: str
    ) -> None:
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url

    def client(self):
        session = boto3.session.Session()
        s3_client = session.client(
            service_name="s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )
        return s3_client
