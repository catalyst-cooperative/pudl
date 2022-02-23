"""Create metadata.yml for Datasette."""
from pathlib import Path

from ...src.pudl.metadata import DatasetteMetadata

DATASETTE_DIR = Path(__file__).parent.resolve()

label_column_dict = {
    'plants_entity_eia': 'plant_name_eia',
    'plants_ferc1': 'plant_name_ferc1',
    'plants_pudl': 'plant_name_pudl',
    'utilities_entity_eia': 'utlity_name_eia',
    'utilities_ferc1': 'utility_name_eia',
    'utilities_pudl': 'utility_name_pudl'
}

data_sources = ['pudl', 'ferc1', 'eia860', 'eia860m', 'eia923']

dm = DatasetteMetadata.from_data_sources(data_sources, label_column_dict)
DatasetteMetadata.to_yaml(DATASETTE_DIR / 'metadata.yml')
