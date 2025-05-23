from sql_reader import get_data_batch
from sql_uploader import process_and_upload
from llm_menu_extractor import run_qc_extraction
from write_log import write_log

def main(start_row_index: int, end_row_index: int, source: str):
    try:
        df = get_data_batch(start_row_index, end_row_index, source)
        cleaned_results = run_qc_extraction(df)
        process_and_upload(cleaned_results, source=source)
        write_log(start_row_index, end_row_index, source, "success")
        return {
            "status": "success",
            "processed": len(cleaned_results)
        }
    except Exception as e:
        write_log(start_row_index, end_row_index, source, "error", message=str(e))
        return {
            "status": "error",
            "message": str(e)
        }

if __name__ == "__main__":
    # Example arguments
    start_row_index = 1
    end_row_index = 3
    source = "training"

    result = main(start_row_index, end_row_index, source)
    print(result)