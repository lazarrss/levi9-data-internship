import unittest
from unittest.mock import MagicMock, patch
from my_lambda import lambda_handler

class TestLambda(unittest.TestCase):

    @patch("my_lambda.s3")
    def test_handler_reads_from_s3(self, mock_s3):

        fake_body = MagicMock()
        fake_body.read.return_value = b"hello_s3"

        mock_s3.get_object.return_value = { "Body": fake_body }

        event = { "bucket": "my_bucket", "key": "my_key" }

        result = lambda_handler(event, None)

        mock_s3.get_object.assert_called_once_with(
            Bucket="my_bucket", Key="my_key"
        )

        self.assertEqual(result["statusCode"], 200)
        self.assertEqual(result["content"], "hello_s3")

if __name__ == "__main__":
    unittest.main()