from django.core.management.base import BaseCommand

from data_extractor.services.extractor_service import ExtractorService


class Command(BaseCommand):

    def handle(self, *args, **options):
        service = ExtractorService()

        try:
            payload, summary = service.extract()
            products = payload.get("products", [])
            self.stdout.write(
                self.style.SUCCESS(
                    f"Extraction completed. Saved JSON to: {summary['output_file']}"
                )
            )
            self.stdout.write(
                f"Rows: {summary['products_rows']}, Total: {payload.get('total')}, "
                f"Preview count: {len(products[:3])}"
            )
        except Exception as exc:
            raise SystemExit(f"Extraction failed: {exc}") from exc
