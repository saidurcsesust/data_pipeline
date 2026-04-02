#!/usr/bin/env python3
from data_process.services.processor_service import ProcessorService


def main():
    service = ProcessorService()

    try:
        _, _, summary = service.process()
        print(
            "Processing completed. "
            f"source_a_rows={summary['source_a_rows']}, "
            f"source_b_rows={summary['source_b_rows']}, "
            f"final_output_rows={summary['final_output_rows']}"
        )
    except Exception as exc:
        raise SystemExit(f"Processing failed: {exc}") from exc
    finally:
        service.stop()


if __name__ == "__main__":
    main()
