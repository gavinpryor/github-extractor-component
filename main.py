import os
import logging
import requests
from keboola.component import ComponentBase
from keboola.component.interface import CsvWriter
from typing import List, Dict

# Define the main component class for the GitHub Extractor
class GitHubExtractor(ComponentBase):
    def __init__(self):
        super().__init__()

    def run(self):
        # Fetch the parameters from the configuration
        token = self.configuration.parameters['#token']  # GitHub personal access token
        owner = self.configuration.parameters['owner']  # GitHub repository owner
        repo = self.configuration.parameters['repo']  # GitHub repository name
        file_paths = self.configuration.parameters.get('file_paths', [])  # List of file paths to retrieve

        # Base URL for the GitHub API
        base_url = f"https://api.github.com/repos/{owner}/{repo}/contents/"
        
        # Headers for the API request
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json'
        }

        # Container to store file contents
        data_rows = []

        # Retrieve file contents from GitHub
        for file_path in file_paths:
            url = f"{base_url}{file_path}"
            logging.info(f"Fetching file from: {url}")
            response = requests.get(url, headers=headers)

            # Check if the request was successful
            if response.status_code == 200:
                file_content = response.json()
                file_data = {
                    'file_path': file_path,
                    'content': file_content['content']
                }
                data_rows.append(file_data)
            else:
                logging.error(f"Failed to fetch file: {file_path} with status code: {response.status_code}")
        
        # Define output table
        output_table = self.create_out_table_definition('github_files.csv', primary_key=['file_path'])
        output_table_path = output_table.full_path

        # Write the data to CSV
        with open(output_table_path, mode='w', newline='', encoding='utf-8') as file:
            writer = CsvWriter(file)
            writer.writeheader(['file_path', 'content'])  # CSV header
            for row in data_rows:
                writer.writerow(row)

        # Write the manifest for the output table
        self.write_manifest(output_table)


# Execute the component
if __name__ == "__main__":
    # Create an instance of the GitHubExtractor and run it
    extractor = GitHubExtractor()
    extractor.execute()
