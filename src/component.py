import logging
import requests
import base64
import csv
from keboola.component import ComponentBase


# Define the main component class for the GitHub Extractor
class GitHubExtractor(ComponentBase):

    def __init__(self):
        super().__init__()

    def run(self):
        # Fetch the parameters from the configuration
        token = self.configuration.parameters['#token']  # GitHub personal access token
        owner = self.configuration.parameters['owner']  # GitHub repository owner
        repo = self.configuration.parameters['repo']  # GitHub repository name

        # Headers for the API request
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json'
        }

        def get_file_content(owner, repo, path):
            url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                logging.error(f"Failed to fetch file: {path} with status code: {response.status_code}")
                return None

            content = response.json()

            if content.get('encoding') == 'base64':
                if is_binary_file(content['name']):
                    logging.info(f"Skipping binary file: {path}")
                    return None
                else:
                    try:
                        return base64.b64decode(content['content']).decode('utf-8')
                    except UnicodeDecodeError:
                        logging.info(f"Unable to decode binary file: {path}")
                        return None

            return ""

        def is_binary_file(filename):
            binary_extensions = ['png', 'jpg', 'jpeg', 'gif', 'pdf', 'zip', 'tar', 'exe']
            extension = filename.split('.')[-1].lower()
            return extension in binary_extensions

        def write_to_csv(data, output_table_path):
            column_names = ['repo_name', 'file_path', 'filename', 'language', 'code', 'url']
            with open(output_table_path, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=column_names)
                writer.writeheader()
                writer.writerows(data)

        def get_language(file_extension):
            extension_language_map = {
                'py': 'Python',
                'js': 'JavaScript',
                'html': 'HTML',
                'css': 'CSS',
                'java': 'Java',
                'cpp': 'C++',
                'c': 'C',
                'cs': 'C#',
                'rb': 'Ruby',
                'php': 'PHP',
                'ts': 'TypeScript',
                'go': 'Go',
                'rs': 'Rust',
                'swift': 'Swift',
                'kt': 'Kotlin',
                'sh': 'Shell',
                'r': 'R',
                'pl': 'Perl',
                'scala': 'Scala',
                'sql': 'SQL',
                'md': 'Markdown',
                'xml': 'XML',
                'yml': 'YAML',
                'json': 'JSON',
                'txt': 'Text',
                'ipynb': 'Jupyter Notebook',
                'gitignore': 'GitIgnore'
            }
            return extension_language_map.get(file_extension.lower(), 'Unknown')

        def extract(owner, repo, path=""):
            url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
            response = requests.get(url, headers=headers)
            contents = response.json()

            if isinstance(contents, list):
                for item in contents:
                    item_path = item['path']
                    if item['type'] == 'dir':
                        logging.info(f"Exploring directory: {item_path}")
                        extract(owner, repo, item_path)
                    else:
                        # Fetch file content
                        logging.info(f"Fetching file: {item_path}")
                        code = get_file_content(owner, repo, item_path)
                        if code:
                            repo_data.append({
                                'repo_name': repo,
                                'file_path': item_path,
                                'filename': item['name'],
                                'language': get_language(item['name'].split('.')[-1]),
                                'code': code,
                                'url': item['html_url']
                            })

        # Main logic
        repo_data = []

        # Extract file data
        extract(owner, repo)

        # Create output table
        output_table = self.create_out_table_definition(f"{owner}-{repo}-repodata.csv", primary_key=['file_path'])
        output_table_path = output_table.full_path

        # Write data to the output table
        write_to_csv(repo_data, output_table_path)
        self.write_manifest(output_table)


# Execute the component
if __name__ == "__main__":

    # Create an instance of the GitHubExtractor and run it
    extractor = GitHubExtractor()
    extractor.execute()
