import requests

import config
from data_extractor.services.utils import write_json_file


class ExtractorService:

    def get_products_api_data(self) -> dict:
        response = requests.get(
            config.PRODUCTS_API_URL,
            headers={
                "Accept": "application/json",
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def extract(self):
        payload = self.get_products_api_data()
        products = payload.get("products", [])
        output_path = write_json_file(payload, config.EXTRACTED_PRODUCTS_FILE)

        extraction_summary = {
            "api_url": config.PRODUCTS_API_URL,
            "output_file": str(output_path),
            "products_rows": len(products),
            "products_keys": list(products[0].keys()) if products else [],
        }

        return payload, extraction_summary
