resource "aws_dynamodb_table" "example_table" {
 name = "example-table"
 billing_mode = "PROVISIONED"
 read_capacity= "30"
 write_capacity= "30"
 attribute {
  name = "created_at"
  type = "S"
 }
 hash_key = "user"
}