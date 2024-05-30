from aws_cdk import Stack
from aws_cdk.assertions import Template

from infrastructure.constructs.data_scraper import DataScraper


def test_synthesizes_properly(service_config):
    # app = App()

    stack = Stack()
    DataScraper(
        stack, "DataScraperTest", stage_name="itest", service_config=service_config
    )
    template = Template.from_stack(stack)
    # We get one extra lambda for Log retention created by the cdk. This might change
    template.resource_count_is("AWS::Lambda::Function", 2)
    template.resource_count_is("AWS::DynamoDB::Table", 2)
    # template.resource_count_is("AWS::DynamoDB::Table", 2)
