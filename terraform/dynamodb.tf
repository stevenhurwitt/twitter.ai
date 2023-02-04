resource "aws_dynamodb_table" "tweets" {
 name = "tweets"
 billing_mode = "PROVISIONED"
 read_capacity= "30"
 write_capacity= "30"
 attribute {
  name = "created_at"
  type = "S"
 }
 attribute {
    name = "user"
    type = "S"
 }
 range_key = "created_at"
 hash_key = "user"
}