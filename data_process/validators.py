def build_validation_report(summary):
    report_lines = [
        f"Row count of source_a: {summary['source_a_rows']}",
        f"Row count of source_b: {summary['source_b_rows']}",
        f"Row count after join: {summary['post_join_rows']}",
        f"Final output row count: {summary['final_output_rows']}",
        f"Dropped rows (missing source_id): {summary['dropped_rows_missing_source_id']}",
        f"Final schema: {summary['final_schema']}",
        f"Confirmation: exactly 10 columns = {summary['exactly_ten_columns']}",
    ]
    return "\n".join(report_lines) + "\n"
